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

"""Store protocol: durable, cross-thread application memory.

A :class:`~agentflow.checkpoint.base.Checkpointer` persists the execution state
of one thread (run) so it can resume; it is keyed by ``(thread, step)`` and is
about control flow. A :class:`Store` is the opposite axis: durable key-value
data that outlives any single run and is shared *across* threads — user
profiles, learned facts, cached documents, long-term agent memory.

Items live under a ``namespace`` (a tuple of path segments, e.g.
``("users", user_id, "memories")``) and a string ``key``. The value is any
JSON-serializable object. ``list_namespaces`` and ``search`` let an agent
enumerate what it has stored. An optional per-item TTL expires stale memory.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

__all__ = ["Item", "Store"]

#: A namespace is an ordered tuple of non-empty path segments.
Namespace = tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Item:
    """One stored value plus its addressing and timestamps.

    ``created_at`` / ``updated_at`` are ISO-8601 strings (UTC). ``expires_at``,
    when set, is the ISO time after which the item is considered gone; a store
    must not return an expired item from ``get`` or ``search``.
    """

    namespace: Namespace
    key: str
    value: Any
    created_at: str = ""
    updated_at: str = ""
    expires_at: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class Store(Protocol):
    """Durable, namespaced key-value memory shared across threads."""

    async def get(self, namespace: Namespace, key: str) -> Item | None:
        """Return the item at ``(namespace, key)``, or None if absent/expired."""
        ...

    async def put(
        self,
        namespace: Namespace,
        key: str,
        value: Any,
        *,
        ttl: float | None = None,
        if_absent: bool = False,
    ) -> Item:
        """Upsert ``value`` at ``(namespace, key)`` and return the stored item.

        ``ttl`` is a lifetime in seconds from now; ``None`` means no expiry. An
        upsert preserves the original ``created_at`` and refreshes
        ``updated_at``.

        When ``if_absent`` is True the write is conditional: it succeeds only if
        no live item exists at ``(namespace, key)``, otherwise it raises
        :class:`~agentflow.errors.StoreConflict`. An expired item counts as
        absent, so its key can be reclaimed. This is an atomic create — two
        writers racing the same key give exactly one winner and one conflict —
        which is how a single-flight claim is built (see
        :class:`~agentflow.prebuilt.IdempotentOp`). ``if_absent`` ignores no
        other semantics: a successful conditional write behaves like a first
        insert (fresh ``created_at``).
        """
        ...

    async def delete(self, namespace: Namespace, key: str) -> bool:
        """Delete the item; return True if something was removed."""
        ...

    async def search(
        self,
        namespace_prefix: Namespace,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Item]:
        """Return items whose namespace starts with ``namespace_prefix``,
        ordered by namespace then key, skipping expired items. ``limit`` and
        ``offset`` paginate."""
        ...

    async def list_namespaces(
        self, *, prefix: Namespace = (), limit: int = 100, offset: int = 0
    ) -> list[Namespace]:
        """Return the distinct namespaces that currently hold items, filtered
        to those starting with ``prefix`` and ordered lexicographically."""
        ...
