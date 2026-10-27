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

"""SQLite-backed Store: durable cross-thread memory with no server and no extra.

The same contract as :class:`~agentflow.store.memory.MemoryStore` and
:class:`~agentflow.store.postgres.PostgresStore`, kept in a single SQLite file, so
a single machine gets memory that survives a restart without installing
PostgreSQL. It is the natural companion of
:class:`~agentflow.checkpoint.sqlite.SqliteCheckpointer`.

``sqlite3`` is stdlib but blocking, so every call runs in a worker thread via
``asyncio.to_thread`` to keep the async contract. WAL mode lets a reader and a
writer coexist. ``put`` runs in a ``BEGIN IMMEDIATE`` transaction, which takes the
write lock before the existence check, so ``if_absent=True`` is an atomic
create across threads, event loops and processes sharing the file: of any number
of racing writers exactly one wins and the rest get
:class:`~agentflow.errors.StoreConflict`.

Namespaces are stored as their segments joined by ``NUL``. A segment therefore
may not contain ``"\\x00"``; this is rejected with ``ValueError``. The encoding
makes SQLite's byte-wise ordering agree with tuple ordering (a shorter namespace
sorts before its extensions) and turns a prefix search into an indexable range
scan instead of a pattern match that would need escaping.

Expiry is lazy, like the other stores: an expired item is excluded from every
read but its row stays until it is overwritten or deleted. Purge long-lived
stores yourself with ``DELETE FROM store WHERE expires_ts <= strftime('%s','now')``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from agentflow.errors import StoreConflict, StoreError
from agentflow.store._util import expiry_iso, now_iso, validate_namespace
from agentflow.store.base import Item, Namespace

__all__ = ["SqliteStore"]

_SEP = "\x00"


def _encode(namespace: Namespace) -> str:
    for segment in namespace:
        if _SEP in segment:
            raise ValueError(f"namespace segments may not contain NUL, got {segment!r}")
    return _SEP.join(namespace)


def _decode(encoded: str) -> Namespace:
    return tuple(encoded.split(_SEP))


def _prefix_clause(prefix: Namespace) -> tuple[str, list[Any]]:
    """SQL for 'namespace starts with these segments' as an index-friendly range.

    Every namespace that extends ``p`` starts with ``p + NUL`` and so lies in
    ``[p + NUL, p + SOH)``; ``p`` itself is matched exactly.
    """
    if not prefix:
        return "1=1", []
    p = _encode(prefix)
    return "(namespace = ? OR (namespace >= ? AND namespace < ?))", [p, p + "\x00", p + "\x01"]


class SqliteStore:
    """Durable namespaced key-value memory in a SQLite file."""

    def __init__(self, path: Path | str = ".runs/store.db", *, table: str = "store") -> None:
        if not table.isidentifier():
            raise ValueError(f"unsafe table name {table!r}")
        self.path = Path(path)
        self.table = table
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
            conn.executescript(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    namespace  TEXT NOT NULL,
                    key        TEXT NOT NULL,
                    value      TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    expires_at TEXT,
                    expires_ts REAL,
                    PRIMARY KEY (namespace, key)
                );
                """
            )
        finally:
            conn.close()

    # ------------------------------------------------------------------

    async def get(self, namespace: Namespace, key: str) -> Item | None:
        enc = _encode(namespace)
        await self._ensure_schema()
        row = await asyncio.to_thread(self._get_row, enc, key)
        if row is None:
            return None
        value, created_at, updated_at, expires_at = row
        return Item(
            namespace=namespace,
            key=key,
            value=json.loads(value),
            created_at=created_at,
            updated_at=updated_at,
            expires_at=expires_at,
        )

    def _get_row(self, enc: str, key: str) -> tuple[str, str, str, str | None] | None:
        conn = self._connect()
        try:
            cur = conn.execute(
                f"SELECT value, created_at, updated_at, expires_at FROM {self.table} "
                "WHERE namespace=? AND key=? AND (expires_ts IS NULL OR expires_ts > ?)",
                (enc, key, time.time()),
            )
            row = cur.fetchone()
            return (row[0], row[1], row[2], row[3]) if row else None
        finally:
            conn.close()

    async def put(
        self,
        namespace: Namespace,
        key: str,
        value: Any,
        *,
        ttl: float | None = None,
        if_absent: bool = False,
    ) -> Item:
        validate_namespace(namespace)
        enc = _encode(namespace)
        try:
            blob = json.dumps(value, ensure_ascii=False)
        except (TypeError, ValueError) as exc:
            raise StoreError(
                f"value for {key!r} under {namespace} is not JSON-serializable: {exc}"
            ) from exc
        await self._ensure_schema()
        created, updated, expires = await asyncio.to_thread(
            self._put_row, enc, key, blob, ttl, if_absent, namespace
        )
        return Item(
            namespace=namespace,
            key=key,
            value=json.loads(blob),
            created_at=created,
            updated_at=updated,
            expires_at=expires,
        )

    def _put_row(
        self,
        enc: str,
        key: str,
        blob: str,
        ttl: float | None,
        if_absent: bool,
        namespace: Namespace,
    ) -> tuple[str, str, str | None]:
        now = now_iso()
        expires = expiry_iso(ttl)
        expires_ts = datetime.fromisoformat(expires).timestamp() if expires else None
        conn = self._connect()
        try:
            # The write lock is taken BEFORE the existence check, so the check and
            # the write are one atomic step for every writer sharing this file.
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    f"SELECT created_at, expires_ts FROM {self.table} WHERE namespace=? AND key=?",
                    (enc, key),
                ).fetchone()
                live = row is not None and (row[1] is None or row[1] > time.time())
                if if_absent and live:
                    raise StoreConflict(namespace, key)
                # An upsert keeps created_at; reclaiming an expired key is a fresh insert.
                created = row[0] if (live and row is not None) else now
                conn.execute(
                    f"""
                    INSERT INTO {self.table}
                        (namespace, key, value, created_at, updated_at, expires_at, expires_ts)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(namespace, key) DO UPDATE SET
                        value = excluded.value,
                        created_at = excluded.created_at,
                        updated_at = excluded.updated_at,
                        expires_at = excluded.expires_at,
                        expires_ts = excluded.expires_ts
                    """,
                    (enc, key, blob, created, now, expires, expires_ts),
                )
                conn.execute("COMMIT")
            except BaseException:
                with contextlib.suppress(sqlite3.Error):
                    conn.execute("ROLLBACK")
                raise
        except sqlite3.Error as exc:
            raise StoreError(f"sqlite put failed: {exc}") from exc
        finally:
            conn.close()
        return created, now, expires

    async def delete(self, namespace: Namespace, key: str) -> bool:
        enc = _encode(namespace)
        await self._ensure_schema()
        return await asyncio.to_thread(self._delete_row, enc, key)

    def _delete_row(self, enc: str, key: str) -> bool:
        conn = self._connect()
        try:
            cur = conn.execute(f"DELETE FROM {self.table} WHERE namespace=? AND key=?", (enc, key))
            return cur.rowcount > 0
        finally:
            conn.close()

    async def search(
        self,
        namespace_prefix: Namespace,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Item]:
        clause, params = _prefix_clause(namespace_prefix)
        await self._ensure_schema()
        rows = await asyncio.to_thread(self._search_rows, clause, params, limit, offset)
        return [
            Item(
                namespace=_decode(enc),
                key=key,
                value=json.loads(value),
                created_at=created_at,
                updated_at=updated_at,
                expires_at=expires_at,
            )
            for enc, key, value, created_at, updated_at, expires_at in rows
        ]

    def _search_rows(self, clause: str, params: list[Any], limit: int, offset: int) -> list[Any]:
        conn = self._connect()
        try:
            cur = conn.execute(
                f"SELECT namespace, key, value, created_at, updated_at, expires_at "
                f"FROM {self.table} "
                f"WHERE {clause} AND (expires_ts IS NULL OR expires_ts > ?) "
                "ORDER BY namespace, key LIMIT ? OFFSET ?",
                [*params, time.time(), limit, offset],
            )
            return cur.fetchall()
        finally:
            conn.close()

    async def list_namespaces(
        self, *, prefix: Namespace = (), limit: int = 100, offset: int = 0
    ) -> list[Namespace]:
        clause, params = _prefix_clause(prefix)
        await self._ensure_schema()
        rows = await asyncio.to_thread(self._namespace_rows, clause, params, limit, offset)
        return [_decode(enc) for (enc,) in rows]

    def _namespace_rows(self, clause: str, params: list[Any], limit: int, offset: int) -> list[Any]:
        conn = self._connect()
        try:
            cur = conn.execute(
                f"SELECT DISTINCT namespace FROM {self.table} "
                f"WHERE {clause} AND (expires_ts IS NULL OR expires_ts > ?) "
                "ORDER BY namespace LIMIT ? OFFSET ?",
                [*params, time.time(), limit, offset],
            )
            return cur.fetchall()
        finally:
            conn.close()
