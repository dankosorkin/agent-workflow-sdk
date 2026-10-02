# Copyright 2026 Daniel Sorkin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""PostgreSQL-backed RunQueue for multi-process, multi-worker control planes.

Run requests live in a ``runs`` table. The key operation is ``claim``: an
``UPDATE ... WHERE runnable ... FOR UPDATE SKIP LOCKED RETURNING`` atomically
hands one row to exactly one worker even under heavy concurrency (SKIP LOCKED
means competing workers step over each other's locked rows instead of
blocking). A time-bounded lease plus periodic ``heartbeat`` means a crashed
worker's run becomes claimable again once its lease expires.

Uses asyncpg. Requires the ``postgres`` extra:
``pip install 'agent-workflow-sdk[postgres]'``.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from agentflow._pg import ensure_schema
from agentflow.controlplane.records import QueueStats, RunRecord, RunStatus
from agentflow.errors import RunNotFound

try:
    import asyncpg
except ModuleNotFoundError as exc:  # pragma: no cover - import-time guard
    raise ModuleNotFoundError(
        "PostgresRunQueue requires asyncpg. Install the extra: "
        "pip install 'agent-workflow-sdk[postgres]'"
    ) from exc

__all__ = ["PostgresRunQueue"]


class PostgresRunQueue:
    """Durable run queue backed by a PostgreSQL table.

    Pass either a ``dsn`` or an existing ``asyncpg`` pool via ``pool``.
    ``table`` names the runs table (created on first use).
    """

    def __init__(
        self,
        dsn: str = "postgresql://postgres@localhost:5432/postgres",
        *,
        pool: asyncpg.Pool | None = None,
        table: str = "runs",
    ) -> None:
        if not table.isidentifier():
            raise ValueError(f"unsafe table name {table!r}")
        self._dsn = dsn
        self._pool = pool
        self._owns_pool = pool is None
        self.table = table
        self._init_done = False
        self._schema_lock = asyncio.Lock()
        self._pool_lock = asyncio.Lock()

    # ------------------------------------------------------------------

    async def _get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            async with self._pool_lock:
                if self._pool is None:
                    self._pool = await asyncpg.create_pool(self._dsn)
        await self._ensure_schema()
        return self._pool

    async def _ensure_schema(self) -> None:
        if self._init_done:
            return
        assert self._pool is not None
        async with self._schema_lock:
            if self._init_done:
                return

            async def create(conn: asyncpg.Connection) -> None:
                await conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.table} (
                        run_id           TEXT        PRIMARY KEY,
                        graph            TEXT        NOT NULL,
                        thread           TEXT        NOT NULL,
                        status           TEXT        NOT NULL,
                        input            JSONB       NOT NULL DEFAULT '{{}}'::jsonb,
                        resume_value     JSONB,
                        error            TEXT,
                        attempt          INTEGER     NOT NULL DEFAULT 0,
                        created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        lease_until      TIMESTAMPTZ,
                        wake_at          TIMESTAMPTZ,
                        cancel_requested BOOLEAN     NOT NULL DEFAULT FALSE
                    )
                    """
                )
                await conn.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{self.table}_status "
                    f"ON {self.table} (status, created_at)"
                )

            await ensure_schema(self._pool, self.table, create)
            self._init_done = True

    async def close(self) -> None:
        """Close the connection pool (only if this queue created it)."""
        if self._pool is not None and self._owns_pool:
            await self._pool.close()
            self._pool = None

    # ------------------------------------------------------------------

    @staticmethod
    def _iso(dt: datetime | None) -> str | None:
        return dt.isoformat() if dt is not None else None

    def _row_to_record(self, row: asyncpg.Record) -> RunRecord:
        def _load(v: Any) -> Any:
            return json.loads(v) if isinstance(v, str) else v

        return RunRecord(
            run_id=row["run_id"],
            graph=row["graph"],
            thread=row["thread"],
            status=row["status"],
            input=_load(row["input"]) or {},
            resume_value=_load(row["resume_value"]),
            error=row["error"],
            attempt=row["attempt"],
            created_at=self._iso(row["created_at"]) or "",
            updated_at=self._iso(row["updated_at"]) or "",
            lease_until=self._iso(row["lease_until"]),
            wake_at=self._iso(row["wake_at"]),
            cancel_requested=row["cancel_requested"],
        )

    _COLS = (
        "run_id, graph, thread, status, input, resume_value, error, attempt, "
        "created_at, updated_at, lease_until, wake_at, cancel_requested"
    )

    async def enqueue(
        self, graph: str, input: Mapping[str, Any] | None = None, *, thread: str | None = None
    ) -> RunRecord:
        pool = await self._get_pool()
        run_id = uuid.uuid4().hex
        row = await pool.fetchrow(
            f"INSERT INTO {self.table} (run_id, graph, thread, status, input) "
            f"VALUES ($1, $2, $3, $4, $5::jsonb) RETURNING {self._COLS}",
            run_id,
            graph,
            thread or run_id,
            RunStatus.QUEUED,
            json.dumps(dict(input or {}), ensure_ascii=False),
        )
        return self._row_to_record(row)

    async def get(self, run_id: str) -> RunRecord | None:
        pool = await self._get_pool()
        row = await pool.fetchrow(f"SELECT {self._COLS} FROM {self.table} WHERE run_id=$1", run_id)
        return self._row_to_record(row) if row is not None else None

    async def list(
        self,
        *,
        status: str | None = None,
        graph: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[RunRecord]:
        pool = await self._get_pool()
        clauses = []
        params: list[Any] = []
        if status is not None:
            params.append(status)
            clauses.append(f"status = ${len(params)}")
        if graph is not None:
            params.append(graph)
            clauses.append(f"graph = ${len(params)}")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.extend([limit, offset])
        rows = await pool.fetch(
            f"SELECT {self._COLS} FROM {self.table} {where} "
            f"ORDER BY created_at DESC LIMIT ${len(params) - 1} OFFSET ${len(params)}",
            *params,
        )
        return [self._row_to_record(r) for r in rows]

    async def claim(self, *, lease_seconds: float = 60.0) -> RunRecord | None:
        pool = await self._get_pool()
        # Pick one runnable row (queued, or running with an expired lease),
        # locking it and skipping any row a competing worker already holds.
        row = await pool.fetchrow(
            f"""
            UPDATE {self.table} SET
                status = '{RunStatus.RUNNING}',
                attempt = attempt + 1,
                lease_until = NOW() + make_interval(secs => $1),
                wake_at = NULL,
                updated_at = NOW()
            WHERE run_id = (
                SELECT run_id FROM {self.table}
                WHERE status = '{RunStatus.QUEUED}'
                   OR (status = '{RunStatus.RUNNING}' AND lease_until < NOW())
                   OR (status = '{RunStatus.WAITING}' AND wake_at <= NOW())
                ORDER BY created_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING {self._COLS}
            """,
            float(lease_seconds),
        )
        return self._row_to_record(row) if row is not None else None

    async def heartbeat(self, run_id: str, *, lease_seconds: float = 60.0) -> None:
        pool = await self._get_pool()
        result = await pool.execute(
            f"UPDATE {self.table} SET "
            "lease_until = NOW() + make_interval(secs => $2), updated_at = NOW() "
            "WHERE run_id = $1",
            run_id,
            float(lease_seconds),
        )
        if result.split()[-1] == "0":
            raise RunNotFound(run_id)

    async def complete(
        self, run_id: str, *, status: str, error: str | None = None, wake_at: str | None = None
    ) -> None:
        pool = await self._get_pool()
        # wake_at applies only to a parked (waiting) run; any other completion
        # clears it so a terminal/queued row never carries a stale wake time.
        wake = wake_at if status == RunStatus.WAITING else None
        result = await pool.execute(
            f"UPDATE {self.table} SET "
            "status = $2, error = $3, lease_until = NULL, "
            "wake_at = $4::timestamptz, updated_at = NOW() "
            "WHERE run_id = $1",
            run_id,
            status,
            error,
            wake,
        )
        if result.split()[-1] == "0":
            raise RunNotFound(run_id)

    async def request_cancel(self, run_id: str) -> bool:
        pool = await self._get_pool()
        async with pool.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                f"SELECT status FROM {self.table} WHERE run_id=$1 FOR UPDATE", run_id
            )
            if row is None or row["status"] in RunStatus.TERMINAL:
                return False
            if row["status"] == RunStatus.QUEUED:
                await conn.execute(
                    f"UPDATE {self.table} SET status=$2, lease_until=NULL, updated_at=NOW() "
                    "WHERE run_id=$1",
                    run_id,
                    RunStatus.CANCELLED,
                )
            else:
                await conn.execute(
                    f"UPDATE {self.table} SET cancel_requested=TRUE, updated_at=NOW() "
                    "WHERE run_id=$1",
                    run_id,
                )
            return True

    async def enqueue_resume(self, run_id: str, value: Any = None) -> RunRecord:
        pool = await self._get_pool()
        row = await pool.fetchrow(
            f"""
            UPDATE {self.table} SET
                status = '{RunStatus.QUEUED}',
                resume_value = $2::jsonb,
                error = NULL,
                lease_until = NULL,
                cancel_requested = FALSE,
                updated_at = NOW()
            WHERE run_id = $1
            RETURNING {self._COLS}
            """,
            run_id,
            json.dumps(value, ensure_ascii=False),
        )
        if row is None:
            raise RunNotFound(run_id)
        return self._row_to_record(row)

    async def stats(self) -> QueueStats:
        pool = await self._get_pool()
        rows = await pool.fetch(f"SELECT status, COUNT(*) AS n FROM {self.table} GROUP BY status")
        by_status = {r["status"]: r["n"] for r in rows}
        expired = await pool.fetchval(
            f"SELECT COUNT(*) FROM {self.table} "
            f"WHERE status = '{RunStatus.RUNNING}' AND lease_until < NOW()"
        )
        return QueueStats(
            total=sum(by_status.values()),
            by_status=by_status,
            expired_leases=int(expired or 0),
        )
