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

"""Idempotency for long loops: skip a node when its work is already done.

Long autonomous runs re-run the same steps after a crash, a resume, or another
loop iteration. Expensive, deterministic work (a fetch, a build, an LLM call
you do not want to pay for twice) should run once per distinct input. This is a
recipe over the :class:`~agentflow.store.Store` you already have — not a new
runtime feature: hash the node's input, look for a cached result under that
hash, and store the fresh result keyed by it.

    store = MemoryStore()

    async def expensive(state, ctx):
        return {"result": await do_costly_thing(state["input"])}

    node = skip_if_done(
        expensive, store,
        namespace=("cache", "expensive"),
        key_from=lambda state: artifact_key(state["input"]),
    )
    g.add_node("expensive", node)

On the first visit the node runs and its update is cached; on any later visit
with the same key the cached update is returned without running the node. The
cache is a ``Store``, so it can be in-memory for a single process or Postgres
for a shared, durable one, and it can expire with a ``ttl``.

Only wrap deterministic work whose result depends solely on the hashed input.
A node that must run every time (it has side effects you always want, or reads
live state the key does not capture) is a poor fit.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from agentflow.store.base import Namespace, Store

__all__ = ["artifact_key", "skip_if_done"]

Node = Callable[[Mapping[str, Any], Any], Awaitable[Mapping[str, Any] | None]]


def artifact_key(value: Any) -> str:
    """Return a stable content hash for a JSON-serializable ``value``.

    The value is serialized with sorted keys so equal content always hashes the
    same, then hashed with SHA-256. Use it to key a cache by *what* the input
    is rather than when it was produced. Raises ``TypeError`` if ``value`` is
    not JSON-serializable — hash something you can round-trip, not a live
    object.
    """
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def skip_if_done(
    node: Node,
    store: Store,
    *,
    namespace: Namespace,
    key_from: Callable[[Mapping[str, Any]], str],
    ttl: float | None = None,
) -> Node:
    """Wrap ``node`` to run once per distinct key, caching its update in ``store``.

    ``key_from(state)`` computes the cache key from the node's input — pair it
    with :func:`artifact_key` to key by content hash. ``namespace`` scopes the
    cache in the store. ``ttl`` optionally expires a cached entry after that
    many seconds.

    The cached value is the node's update dict. A node returning ``None`` is
    cached as an empty update, so a "did nothing" result still short-circuits.
    The wrapper is a plain ``(state, ctx) -> update`` node, transparent to the
    engine.
    """

    async def wrapped(state: Mapping[str, Any], ctx: Any) -> Mapping[str, Any] | None:
        key = key_from(state)
        cached = await store.get(namespace, key)
        if cached is not None:
            return cached.value

        update = await node(state, ctx)
        await store.put(namespace, key, dict(update) if update else {}, ttl=ttl)
        return update

    wrapped.__name__ = f"skip_if_done({getattr(node, '__name__', 'node')})"
    return wrapped
