# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""PostgreSQL-backed durable checkpointer for production, multi-process runs.

Each checkpoint is a row in a ``checkpoints`` table keyed by ``(thread, step)``
with a monotonic ``revision`` column and a ``jsonb`` payload. Writes run inside
a transaction: an upsert bumps ``revision`` atomically, and a conditional write
(``if_revision``) checks the stored revision under ``FOR UPDATE`` so two
resumes racing the same super-step can't silently clobber each other — the
loser gets :class:`~agentflow.errors.CheckpointConflict`.

Postgres is the recommended production-durability default: unlike Redis (which
needs AOF/RDB tuning to survive a crash) a committed row is durable by design.

Uses asyncpg. Requires the ``postgres`` extra:
``pip install 'agent-workflow-sdk[postgres]'``.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import AsyncIterator

from agentflow.checkpoint._serde import from_payload, to_payload
from agentflow.checkpoint.base import Checkpoint, ThreadInfo
from agentflow.errors import CheckpointConflict, CheckpointError
from agentflow.redaction import Redactor, redact_none

try:
    import asyncpg
except ModuleNotFoundError as exc:  # pragma: no cover - import-time guard
    raise ModuleNotFoundError(
        "PostgresCheckpointer requires asyncpg. Install the extra: "
        "pip install 'agent-workflow-sdk[postgres]'"
    ) from exc

__all__ = ["PostgresCheckpointer"]


class PostgresCheckpointer:
    """Durable checkpointer backed by a PostgreSQL table.

    Pass either a ``dsn`` (``postgresql://user:pw@host:port/db``) or an existing
    ``asyncpg`` pool via ``pool``. ``table`` names the checkpoints table (it is
    created on first use). ``redact`` optionally masks sensitive values in the
    persisted state (see :class:`~agentflow.checkpoint.file.FileCheckpointer`
    for the resume-fidelity caveat).
    """

    def __init__(
        self,
        dsn: str = "postgresql://postgres@localhost:5432/postgres",
        *,
        pool: asyncpg.Pool | None = None,
        table: str = "checkpoints",
        redact: Redactor | None = None,
    ) -> None:
        if not table.isidentifier():
            raise ValueError(f"unsafe table name {table!r}")
        self._dsn = dsn
        self._pool = pool
        self._owns_pool = pool is None
        self.table = table
        self._redact = redact or redact_none
        self._init_done = False

    # ------------------------------------------------------------------

    async def _get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self._dsn)
        await self._ensure_schema()
        return self._pool

    async def _ensure_schema(self) -> None:
        if self._init_done:
            return
        assert self._pool is not None
        await self._pool.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                thread   TEXT    NOT NULL,
                step     INTEGER NOT NULL,
                revision INTEGER NOT NULL DEFAULT 1,
                payload  JSONB   NOT NULL,
                ts       TEXT    NOT NULL DEFAULT '',
                PRIMARY KEY (thread, step)
            )
            """
        )
        self._init_done = True

    async def close(self) -> None:
        """Close the connection pool (only if this checkpointer created it)."""
        if self._pool is not None and self._owns_pool:
            await self._pool.close()
            self._pool = None

    # ------------------------------------------------------------------

    def _serialize(self, cp: Checkpoint) -> str:
        payload = to_payload(cp)
        payload["state"] = self._redact(payload["state"])
        payload["interrupt_payload"] = self._redact(payload["interrupt_payload"])
        try:
            return json.dumps(payload, ensure_ascii=False)
        except TypeError as exc:
            raise CheckpointError(
                f"checkpoint state for thread {cp.thread!r} is not JSON-serializable: {exc}"
            ) from exc

    async def put(self, cp: Checkpoint, *, if_revision: int | None = None) -> str:
        pool = await self._get_pool()
        blob = self._serialize(cp)
        async with pool.acquire() as conn, conn.transaction():
            if if_revision is not None:
                row = await conn.fetchrow(
                    f"SELECT revision FROM {self.table} WHERE thread=$1 AND step=$2 FOR UPDATE",
                    cp.thread,
                    cp.step,
                )
                current = row["revision"] if row else 0
                if current != if_revision:
                    raise CheckpointConflict(cp.thread, cp.step, if_revision, current)
            await conn.execute(
                f"""
                INSERT INTO {self.table} (thread, step, revision, payload, ts)
                VALUES ($1, $2, 1, $3::jsonb, $4)
                ON CONFLICT (thread, step) DO UPDATE SET
                    revision = {self.table}.revision + 1,
                    payload = EXCLUDED.payload,
                    ts = EXCLUDED.ts
                """,
                cp.thread,
                cp.step,
                blob,
                cp.ts,
            )
        return f"{cp.thread}:{cp.step}"

    def _row_to_cp(self, row: asyncpg.Record) -> Checkpoint:
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        return dataclasses.replace(from_payload(payload), revision=row["revision"])

    async def get(self, thread: str, step: int | None = None) -> Checkpoint | None:
        pool = await self._get_pool()
        if step is None:
            row = await pool.fetchrow(
                f"SELECT payload, revision FROM {self.table} "
                "WHERE thread=$1 ORDER BY step DESC LIMIT 1",
                thread,
            )
        else:
            row = await pool.fetchrow(
                f"SELECT payload, revision FROM {self.table} WHERE thread=$1 AND step=$2",
                thread,
                step,
            )
        return self._row_to_cp(row) if row is not None else None

    async def history(self, thread: str) -> AsyncIterator[Checkpoint]:
        pool = await self._get_pool()
        rows = await pool.fetch(
            f"SELECT payload, revision FROM {self.table} WHERE thread=$1 ORDER BY step ASC",
            thread,
        )
        for row in rows:
            yield self._row_to_cp(row)

    async def delete_thread(self, thread: str) -> None:
        pool = await self._get_pool()
        await pool.execute(f"DELETE FROM {self.table} WHERE thread=$1", thread)

    async def prune(
        self, thread: str, *, before_step: int | None = None, older_than: str | None = None
    ) -> int:
        pool = await self._get_pool()
        clauses = ["thread=$1"]
        params: list[object] = [thread]
        if before_step is not None:
            params.append(before_step)
            clauses.append(f"step < ${len(params)}")
        if older_than is not None:
            params.append(older_than)
            clauses.append(f"ts < ${len(params)}")
        sql = f"DELETE FROM {self.table} WHERE {' AND '.join(clauses)}"
        result = await pool.execute(sql, *params)
        # asyncpg returns a tag like "DELETE 3".
        return int(result.split()[-1]) if result else 0

    async def list_threads(self, *, limit: int = 100, offset: int = 0) -> list[ThreadInfo]:
        pool = await self._get_pool()
        # DISTINCT ON picks the latest row per thread (highest step).
        rows = await pool.fetch(
            f"SELECT DISTINCT ON (thread) thread, step, payload "
            f"FROM {self.table} "
            "ORDER BY thread, step DESC "
            "LIMIT $1 OFFSET $2",
            limit,
            offset,
        )
        infos: list[ThreadInfo] = []
        for row in rows:
            payload = row["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            cp = from_payload(payload)
            infos.append(
                ThreadInfo(
                    thread=row["thread"],
                    latest_step=row["step"],
                    interrupted=cp.interrupted,
                    done=cp.done,
                    ts=cp.ts,
                )
            )
        return infos
