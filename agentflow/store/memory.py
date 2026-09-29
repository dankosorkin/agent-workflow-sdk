"""In-memory Store: fast, ephemeral cross-thread memory for tests and single-process runs."""

from __future__ import annotations

from typing import Any

from agentflow.store._util import expiry_iso, is_expired, now_iso, validate_namespace
from agentflow.store.base import Item, Namespace

__all__ = ["MemoryStore"]


class MemoryStore:
    """Keeps items in a nested dict keyed by namespace then key. Not durable.

    Expiry is lazy: an expired item is skipped on read and dropped when next
    touched, so no background sweeper is needed.
    """

    def __init__(self) -> None:
        self._items: dict[Namespace, dict[str, Item]] = {}

    async def get(self, namespace: Namespace, key: str) -> Item | None:
        bucket = self._items.get(namespace)
        if not bucket:
            return None
        item = bucket.get(key)
        if item is None:
            return None
        if is_expired(item.expires_at):
            del bucket[key]
            return None
        return item

    async def put(
        self,
        namespace: Namespace,
        key: str,
        value: Any,
        *,
        ttl: float | None = None,
    ) -> Item:
        validate_namespace(namespace)
        bucket = self._items.setdefault(namespace, {})
        existing = bucket.get(key)
        created = existing.created_at if existing and not is_expired(existing.expires_at) else ""
        ts = now_iso()
        item = Item(
            namespace=namespace,
            key=key,
            value=value,
            created_at=created or ts,
            updated_at=ts,
            expires_at=expiry_iso(ttl),
        )
        bucket[key] = item
        return item

    async def delete(self, namespace: Namespace, key: str) -> bool:
        bucket = self._items.get(namespace)
        if not bucket or key not in bucket:
            return False
        del bucket[key]
        if not bucket:
            del self._items[namespace]
        return True

    async def search(
        self,
        namespace_prefix: Namespace,
        *,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Item]:
        matches: list[Item] = []
        for ns in sorted(self._items):
            if ns[: len(namespace_prefix)] != namespace_prefix:
                continue
            for key in sorted(self._items[ns]):
                item = self._items[ns][key]
                if is_expired(item.expires_at):
                    continue
                matches.append(item)
        return matches[offset : offset + limit]

    async def list_namespaces(
        self, *, prefix: Namespace = (), limit: int = 100, offset: int = 0
    ) -> list[Namespace]:
        seen: set[Namespace] = set()
        for ns, bucket in self._items.items():
            if ns[: len(prefix)] != prefix:
                continue
            if any(not is_expired(i.expires_at) for i in bucket.values()):
                seen.add(ns)
        return sorted(seen)[offset : offset + limit]
