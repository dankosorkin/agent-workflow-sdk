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

"""PostgreSQL-backed durable Store for cross-thread application memory.

Items live in a table keyed by ``(namespace, key)`` where ``namespace`` is a
``text[]`` so prefix search is a native array-slice comparison. Values are
``jsonb``; an optional ``expires_at timestamptz`` gives per-item TTL, filtered
out of every read (``NOW()``) so expired memory never surfaces.

Uses asyncpg. Requires the ``postgres`` extra:
``pip install 'agent-workflow-sdk[postgres]'``.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from agentflow.store._util import validate_namespace
from agentflow.store.base import Item, Namespace

try:
    import asyncpg
except ModuleNotFoundError as exc:  # pragma: no cover - import-time guard
    raise ModuleNotFoundError(
        "PostgresStore requires asyncpg. Install the extra: "
        "pip install 'agent-workflow-sdk[postgres]'"
    ) from exc

__all__ = ["PostgresStore"]


class PostgresStore:
    """Durable namespaced key-value store backed by a PostgreSQL table.

    Pass either a ``dsn`` or an existing ``asyncpg`` pool via ``pool``.
    ``table`` names the store table (created on first use).
    """

    def __init__(
        self,
        dsn: str = "postgresql://postgres@localhost:5432/postgres",
        *,
        pool: asyncpg.Pool | None = None,
        table: str = "store",
    ) -> None:
        if not table.isidentifier():
            raise ValueError(f"unsafe table name {table!r}")
        self._dsn = dsn
        self._pool = pool
        self._owns_pool = pool is None
        self.table = table
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
                namespace  TEXT[]      NOT NULL,
                key        TEXT        NOT NULL,
                value      JSONB       NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                expires_at TIMESTAMPTZ,
                PRIMARY KEY (namespace, key)
            )
            """
        )
        await self._pool.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{self.table}_ns ON {self.table} (namespace)"
        )
        self._init_done = True

    async def close(self) -> None:
        """Close the connection pool (only if this store created it)."""
        if self._pool is not None and self._owns_pool:
            await self._pool.close()
            self._pool = None

    # ------------------------------------------------------------------

    @staticmethod
    def _iso(dt: datetime | None) -> str | None:
        return dt.isoformat() if dt is not None else None

    def _row_to_item(self, row: asyncpg.Record) -> Item:
        value = row["value"]
        if isinstance(value, str):
            value = json.loads(value)
        return Item(
            namespace=tuple(row["namespace"]),
            key=row["key"],
            value=value,
            created_at=self._iso(row["created_at"]) or "",
            updated_at=self._iso(row["updated_at"]) or "",
            expires_at=self._iso(row["expires_at"]),
        )

    async def get(self, namespace: Namespace, key: str) -> Item | None:
        pool = await self._get_pool()
        row = await pool.fetchrow(
            f"SELECT namespace, key, value, created_at, updated_at, expires_at "
            f"FROM {self.table} "
            "WHERE namespace=$1 AND key=$2 AND (expires_at IS NULL OR expires_at > NOW())",
            list(namespace),
            key,
        )
        return self._row_to_item(row) if row is not None else None

    async def put(
        self,
        namespace: Namespace,
        key: str,
        value: Any,
        *,
        ttl: float | None = None,
    ) -> Item:
        validate_namespace(namespace)
        pool = await self._get_pool()
        blob = json.dumps(value, ensure_ascii=False)
        expires = f"NOW() + make_interval(secs => {float(ttl)})" if ttl is not None else "NULL"
        row = await pool.fetchrow(
            f"""
            INSERT INTO {self.table} (namespace, key, value, expires_at)
            VALUES ($1, $2, $3::jsonb, {expires})
            ON CONFLICT (namespace, key) DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = NOW(),
                expires_at = EXCLUDED.expires_at
            RETURNING namespace, key, value, created_at, updated_at, expires_at
            """,
            list(namespace),
            key,
            blob,
        )
        return self._row_to_item(row)

    async def delete(self, namespace: Namespace, key: str) -> bool:
        pool = await self._get_pool()
        result = await pool.execute(
            f"DELETE FROM {self.table} WHERE namespace=$1 AND key=$2",
            list(namespace),
            key,
        )
        return result.split()[-1] != "0"

    async def search(
        self,
        namespace_prefix: Namespace,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Item]:
        pool = await self._get_pool()
        prefix = list(namespace_prefix)
        n = len(prefix)
        rows = await pool.fetch(
            f"SELECT namespace, key, value, created_at, updated_at, expires_at "
            f"FROM {self.table} "
            "WHERE namespace[1:$1] = $2::text[] "
            "AND (expires_at IS NULL OR expires_at > NOW()) "
            "ORDER BY namespace, key LIMIT $3 OFFSET $4",
            n,
            prefix,
            limit,
            offset,
        )
        return [self._row_to_item(r) for r in rows]

    async def list_namespaces(
        self, *, prefix: Namespace = (), limit: int = 100, offset: int = 0
    ) -> list[Namespace]:
        pool = await self._get_pool()
        pre = list(prefix)
        n = len(pre)
        rows = await pool.fetch(
            f"SELECT DISTINCT namespace FROM {self.table} "
            "WHERE namespace[1:$1] = $2::text[] "
            "AND (expires_at IS NULL OR expires_at > NOW()) "
            "ORDER BY namespace LIMIT $3 OFFSET $4",
            n,
            pre,
            limit,
            offset,
        )
        return [tuple(r["namespace"]) for r in rows]
