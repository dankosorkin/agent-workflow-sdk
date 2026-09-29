"""Transactional SQLite checkpointer for durable, multi-writer-safe runs.

Unlike :class:`~agentflow.checkpoint.file.FileCheckpointer` (single-writer,
one JSON per step), this stores every checkpoint as a row in a SQLite database
and writes it inside a transaction. Each ``(thread, step)`` is a primary key
with a monotonic ``revision``; a write to an existing step bumps the revision
atomically, so a resume that re-runs a super-step overwrites cleanly and two
writers cannot silently corrupt history — the last committed transaction wins
and the revision records the overwrite.

``sqlite3`` is stdlib but blocking, so every call runs in a worker thread via
``asyncio.to_thread`` to preserve the async contract. WAL mode is enabled so a
reader and a writer don't block each other.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import sqlite3
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from agentflow.checkpoint._serde import from_payload, to_payload
from agentflow.checkpoint.base import Checkpoint, ThreadInfo
from agentflow.errors import CheckpointConflict, CheckpointError
from agentflow.redaction import Redactor, redact_none

__all__ = ["SqliteCheckpointer"]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS checkpoints (
    thread   TEXT    NOT NULL,
    step     INTEGER NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1,
    payload  TEXT    NOT NULL,
    ts       TEXT    NOT NULL,
    PRIMARY KEY (thread, step)
);
CREATE INDEX IF NOT EXISTS idx_checkpoints_thread ON checkpoints(thread, step);
"""


class SqliteCheckpointer:
    """Durable checkpointer backed by a SQLite database file."""

    def __init__(
        self,
        path: Path | str = ".runs/checkpoints.db",
        *,
        redact: Redactor | None = None,
    ) -> None:
        self.path = Path(path)
        self._redact = redact or redact_none
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_done = False
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, isolation_level=None, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    async def _ensure_schema(self) -> None:
        if self._init_done:
            return
        async with self._lock:
            if self._init_done:
                return
            await asyncio.to_thread(self._init_schema)
            self._init_done = True

    def _init_schema(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(_SCHEMA)
        finally:
            conn.close()

    # ------------------------------------------------------------------

    async def put(self, cp: Checkpoint, *, if_revision: int | None = None) -> str:
        await self._ensure_schema()
        payload = to_payload(cp)
        payload["state"] = self._redact(payload["state"])
        payload["interrupt_payload"] = self._redact(payload["interrupt_payload"])
        try:
            blob = json.dumps(payload, ensure_ascii=False)
        except TypeError as exc:
            raise CheckpointError(
                f"checkpoint state for thread {cp.thread!r} is not JSON-serializable: {exc}"
            ) from exc
        await asyncio.to_thread(self._put_row, cp.thread, cp.step, blob, cp.ts, if_revision)
        return f"{cp.thread}:{cp.step}"

    def _put_row(self, thread: str, step: int, blob: str, ts: str, if_revision: int | None) -> None:
        conn = self._connect()
        try:
            # One transaction: check-and-set the revision, then upsert with an
            # atomic revision bump. A re-run of the same step (resume)
            # overwrites and increments revision.
            conn.execute("BEGIN IMMEDIATE")
            if if_revision is not None:
                cur = conn.execute(
                    "SELECT revision FROM checkpoints WHERE thread=? AND step=?",
                    (thread, step),
                )
                row = cur.fetchone()
                current = row[0] if row else 0
                if current != if_revision:
                    conn.execute("ROLLBACK")
                    raise CheckpointConflict(thread, step, if_revision, current)
            conn.execute(
                """
                INSERT INTO checkpoints (thread, step, revision, payload, ts)
                VALUES (?, ?, 1, ?, ?)
                ON CONFLICT(thread, step) DO UPDATE SET
                    revision = revision + 1,
                    payload = excluded.payload,
                    ts = excluded.ts
                """,
                (thread, step, blob, ts),
            )
            conn.execute("COMMIT")
        except sqlite3.Error as exc:
            conn.execute("ROLLBACK")
            raise CheckpointError(f"sqlite put failed: {exc}") from exc
        finally:
            conn.close()

    async def get(self, thread: str, step: int | None = None) -> Checkpoint | None:
        await self._ensure_schema()
        row = await asyncio.to_thread(self._get_row, thread, step)
        if row is None:
            return None
        blob, revision = row
        return dataclasses.replace(from_payload(json.loads(blob)), revision=revision)

    def _get_row(self, thread: str, step: int | None) -> tuple[str, int] | None:
        conn = self._connect()
        try:
            if step is None:
                cur = conn.execute(
                    "SELECT payload, revision FROM checkpoints WHERE thread=? "
                    "ORDER BY step DESC LIMIT 1",
                    (thread,),
                )
            else:
                cur = conn.execute(
                    "SELECT payload, revision FROM checkpoints WHERE thread=? AND step=?",
                    (thread, step),
                )
            row = cur.fetchone()
            return (row[0], row[1]) if row else None
        finally:
            conn.close()

    async def history(self, thread: str) -> AsyncIterator[Checkpoint]:
        await self._ensure_schema()
        rows = await asyncio.to_thread(self._history_rows, thread)
        for blob, revision in rows:
            yield dataclasses.replace(from_payload(json.loads(blob)), revision=revision)

    def _history_rows(self, thread: str) -> list[tuple[str, int]]:
        conn = self._connect()
        try:
            cur = conn.execute(
                "SELECT payload, revision FROM checkpoints WHERE thread=? ORDER BY step ASC",
                (thread,),
            )
            return [(r[0], r[1]) for r in cur.fetchall()]
        finally:
            conn.close()

    async def delete_thread(self, thread: str) -> None:
        await self._ensure_schema()
        await asyncio.to_thread(
            self._exec_write, "DELETE FROM checkpoints WHERE thread=?", (thread,)
        )

    async def prune(
        self, thread: str, *, before_step: int | None = None, older_than: str | None = None
    ) -> int:
        await self._ensure_schema()
        return await asyncio.to_thread(self._prune_rows, thread, before_step, older_than)

    def _exec_write(self, sql: str, params: tuple) -> int:
        conn = self._connect()
        try:
            cur = conn.execute(sql, params)
            return cur.rowcount
        finally:
            conn.close()

    def _prune_rows(self, thread: str, before_step: int | None, older_than: str | None) -> int:
        clauses = ["thread=?"]
        params: list[Any] = [thread]
        if before_step is not None:
            clauses.append("step < ?")
            params.append(before_step)
        if older_than is not None:
            clauses.append("ts < ?")
            params.append(older_than)
        sql = f"DELETE FROM checkpoints WHERE {' AND '.join(clauses)}"
        return self._exec_write(sql, tuple(params))

    async def list_threads(self, *, limit: int = 100, offset: int = 0) -> list[ThreadInfo]:
        await self._ensure_schema()
        rows = await asyncio.to_thread(self._list_thread_rows, limit, offset)
        infos: list[ThreadInfo] = []
        for thread, step, blob in rows:
            cp = from_payload(json.loads(blob))
            infos.append(
                ThreadInfo(
                    thread=thread,
                    latest_step=step,
                    interrupted=cp.interrupted,
                    done=cp.done,
                    ts=cp.ts,
                )
            )
        return infos

    def _list_thread_rows(self, limit: int, offset: int) -> list[tuple[str, int, str]]:
        conn = self._connect()
        try:
            # Latest row per thread: join each thread's max(step) back to payload.
            cur = conn.execute(
                """
                SELECT c.thread, c.step, c.payload
                FROM checkpoints c
                JOIN (
                    SELECT thread, MAX(step) AS step FROM checkpoints GROUP BY thread
                ) m ON c.thread = m.thread AND c.step = m.step
                ORDER BY c.thread ASC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            )
            return [(r[0], r[1], r[2]) for r in cur.fetchall()]
        finally:
            conn.close()
