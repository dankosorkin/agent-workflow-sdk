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

"""Shared helper for safe, idempotent schema creation on PostgreSQL.

``CREATE TABLE IF NOT EXISTS`` is *not* safe to run from two sessions at once:
PostgreSQL's catalog insert can collide on ``pg_type``'s unique index and raise
``UniqueViolationError``, even with ``IF NOT EXISTS`` (the check and the insert
are not atomic against a concurrent creator). The control-plane backends and
the store share one table across processes, so first use really can be
concurrent — a pool of workers starting at once, say.

:func:`ensure_schema` closes both races a backend faces on first use:

- In-process: many coroutines calling it before the one-time flag is set are
  serialized by the caller's :class:`asyncio.Lock`, and the double-checked flag
  means the DDL runs at most once per object.
- Cross-process: the DDL runs inside a transaction holding a
  ``pg_advisory_xact_lock`` keyed on the table name, so two processes creating
  the same table take turns instead of colliding. The lock is released when the
  transaction commits.

This module imports no third party package; it is handed an already-open
``asyncpg`` pool, so it stays importable without the ``postgres`` extra.
"""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable
from typing import Any

__all__ = ["advisory_key", "ensure_schema"]


def advisory_key(table: str) -> int:
    """Derive a stable 63-bit advisory-lock key from a table name.

    PostgreSQL advisory locks take a signed 64-bit integer. We hash the table
    name and keep 63 bits so the value is always positive and well within
    range, giving each table its own lock without a registry.
    """
    digest = hashlib.sha256(table.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") >> 1


async def ensure_schema(
    pool: Any,
    table: str,
    create: Callable[[Any], Awaitable[None]],
) -> None:
    """Run ``create`` once, serialized across sessions by an advisory lock.

    ``create`` receives a connection and issues the ``CREATE TABLE IF NOT
    EXISTS`` / index statements on it. It runs inside a transaction that first
    takes ``pg_advisory_xact_lock(advisory_key(table))``, so a concurrent
    creator of the same table waits rather than racing the catalog. ``create``
    must be safe to run more than once (use ``IF NOT EXISTS``): the advisory
    lock removes the collision, and idempotent DDL makes a second run a no-op.

    The caller still guards with its own ``asyncio.Lock`` and one-time flag to
    avoid redundant DDL within a single process; this function only closes the
    cross-process window.
    """
    key = advisory_key(table)
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock($1)", key)
        await create(conn)
