# Cross-thread memory (Store)

A checkpointer persists one run's execution state so it can resume. A `Store` is the other axis: durable key-value data shared across runs — user profiles, learned facts, long-term agent memory. This chapter covers the difference and the `Store` API.

## Checkpointer vs Store

Keep these two distinct. They solve different problems.

| | Checkpointer | Store |
| --- | --- | --- |
| Holds | One thread's execution state | Application data across threads |
| Keyed by | `(thread, step)` | `(namespace, key)` |
| Lifetime | A run's history | As long as you keep it (or a TTL) |
| Purpose | Resume, inspect, time-travel | Profiles, facts, memory, cache |

A checkpointer answers "where was this run?". A store answers "what do we know about this user, across all their runs?".

## The shape

Items live under a `namespace` — a tuple of path segments — and a string `key`. The value is any JSON-serializable object.

```python
from agentflow import MemoryStore

store = MemoryStore()

await store.put(("users", "u1", "memories"), "favorite_color", "blue")

item = await store.get(("users", "u1", "memories"), "favorite_color")
print(item.value)   # "blue"

# Everything under a namespace prefix, ordered by namespace then key.
recent = await store.search(("users", "u1"))
```

`put` returns the stored `Item`; `get` returns an `Item` or `None`; `search` returns a list of `Item`s whose namespace starts with the given prefix.

## The Item

```python
Item(
    namespace,      # the tuple it lives under
    key,            # its string key
    value,          # any JSON value
    created_at="",  # ISO-8601 UTC
    updated_at="",  # refreshed on each put; created_at is preserved
    expires_at=None # set when a TTL was given
)
```

## Expiry with TTL

`put(..., ttl=seconds)` gives an item a lifetime. Expired items never surface from `get` or `search` — they behave as if gone.

```python
from agentflow.store import PostgresStore

store = PostgresStore("postgresql://localhost/app")
await store.put(("cache",), "doc-42", payload, ttl=3600)   # gone after an hour
```

This makes a `Store` a natural cache as well as a memory: durable where you want permanence, self-expiring where you want freshness.

## Atomic claim with `if_absent`

`put(..., if_absent=True)` is a conditional create: it writes only if no live item exists at the key, and raises `StoreConflict` if one does. An expired item counts as absent, so its key can be reclaimed. Two writers racing the same key produce exactly one winner and one conflict — an atomic single-flight, within a process for `MemoryStore` and across processes for `PostgresStore` (a single conditional `INSERT`).

```python
from agentflow import StoreConflict

try:
    await store.put(("claims",), job_id, {"status": "running"}, if_absent=True)
    # we won the claim — do the work
except StoreConflict:
    # someone else claimed it first — back off or read their result
    ...
```

This is the primitive behind [`IdempotentOp`](../patterns/long-running-loops.md#side-effects-and-delivery-semantics), which uses it to run a side effect at most once per key.

## Listing and paginating

`search` and `list_namespaces` both paginate with `limit` and `offset`.

```python
page = await store.search(("users",), limit=50, offset=0)
spaces = await store.list_namespaces(prefix=("users",))   # distinct namespaces in use
```

`delete(namespace, key)` removes an item and returns whether something was removed.

## The two implementations

Both implement the same `Store` protocol, so they are interchangeable.

- `MemoryStore` — ephemeral, in-process. For tests and single-process scripts.
- `PostgresStore` — durable and shared across processes. Requires the `postgres` extra.

## Using a store in a node

A store is a plain object you construct and pass into your nodes, the same way you pass a backend. A common pattern is to key memory by something stable about the user, and to scope reads and writes to that namespace.

```python
async def remember(state, ctx):
    ns = ("users", state["user_id"], "memories")
    await store.put(ns, "last_topic", state["topic"])
    return {}

async def recall(state, ctx):
    ns = ("users", state["user_id"], "memories")
    item = await store.get(ns, "last_topic")
    return {"prior_topic": item.value if item else None}
```

Because the store is independent of any thread, `recall` in a brand-new run can read what `remember` wrote in a previous one. That is the whole point: memory that outlives a single run.

Next: [the control plane](control-plane.md).
