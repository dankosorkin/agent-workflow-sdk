# Checkpointing

Durability is a property of the engine, not something you code. Attach a checkpointer at `compile()` and every super-step is persisted for you. This chapter covers what a checkpoint is, the available backends, optimistic concurrency, and retention.

## Turning on durability

```python
from agentflow import FileCheckpointer

app = g.compile(checkpointer=FileCheckpointer(".runs"))
```

That is the whole change. With a checkpointer, each super-step writes a checkpoint before the next begins. If the process crashes, `app.resume(thread)` picks up from the last committed step. Without a checkpointer, a graph still runs — it just cannot resume, inspect history, or do human-in-the-loop.

## What a checkpoint holds

A `Checkpoint` is a frozen record of one point in a run:

```python
Checkpoint(
    thread,             # the run's id
    step,               # super-step number
    state,              # the full state at this step
    next=(),            # the frontier to run on resume
    interrupted=False,  # True if suspended for a human
    interrupt_node=...,  # which node interrupted
    interrupt_payload=..., # what the human must resolve
    ts="",              # ISO timestamp
    revision=0,         # write count for this (thread, step); see below
)

cp.done   # property: True when next is empty and not interrupted
```

The meaning of the frontier is the key: an empty `next` with `interrupted=False` means the run finished; a non-empty `next` means "resume by running these nodes"; `interrupted=True` means "resume once a human answers `interrupt_payload`".

## The available checkpointers

All implement the same `Checkpointer` protocol and are interchangeable at `compile(checkpointer=...)`.

| Checkpointer | Storage | Use for | Extra |
| --- | --- | --- | --- |
| `MemoryCheckpointer` | In-process dict | Tests, single-process scripts | — |
| `FileCheckpointer` | One atomic JSON file per step under a directory | Local durable single-writer runs | — |
| `SqliteCheckpointer` | SQLite, transactional (WAL) | Single-host durable, queryable | — |
| `RedisCheckpointer` | Redis server | Distributed / multi-process runners | `redis` |
| `PostgresCheckpointer` | Postgres table | Production durability, many workers | `postgres` |

```python
from agentflow import MemoryCheckpointer, FileCheckpointer, SqliteCheckpointer
from agentflow.checkpoint import RedisCheckpointer, PostgresCheckpointer

app = g.compile(checkpointer=PostgresCheckpointer("postgresql://localhost/app"))
```

### Which to choose

- Tests: `MemoryCheckpointer`.
- A local script or a single long-running process: `FileCheckpointer` or `SqliteCheckpointer`.
- Production with multiple workers: `PostgresCheckpointer`. A committed row survives a crash by design, with no extra tuning.
- Redis: durable only when you configure AOF/RDB persistence. Prefer Postgres when a committed run must survive a crash.

## Optimistic concurrency

Two resumes racing the same super-step could clobber each other. Each checkpoint carries a `revision` — the write count for its `(thread, step)`, set when you read it back. To make a read-modify-write safe, pass the revision you read as `if_revision`; a stale value raises `CheckpointConflict` instead of silently overwriting.

```python
cp = await checkpointer.get(thread, step)
# ... do work off cp ...
await checkpointer.put(new_cp, if_revision=cp.revision)   # raises CheckpointConflict on a race
```

A plain `put(cp)` with no `if_revision` is an unconditional upsert — the common case. You only need `if_revision` when two processes might resume the same thread at the same step.

## Retention

Checkpoints accumulate. Two methods trim them.

```python
# Drop an entire thread's history.
await checkpointer.delete_thread(thread)

# Delete old checkpoints; returns how many were removed.
removed = await checkpointer.prune(thread, before_step=50)
removed = await checkpointer.prune(thread, older_than="2026-01-01T00:00:00Z")
```

`prune` deletes steps strictly below `before_step`, or checkpoints whose timestamp precedes `older_than`. Give at least one bound.

## Enumerating runs

`list_threads` returns a `ThreadInfo` summary for each stored thread — its latest step and whether it is interrupted or done — without reading full state. This is the foundation the control plane builds on to enumerate running, interrupted, and finished runs.

```python
for info in await checkpointer.list_threads():
    print(info.thread, info.latest_step, "done" if info.done else "running")
```

## A security note

Checkpoints are plaintext by default. They contain prompts, model output, and tool arguments — potentially secrets. AgentFlow creates checkpoint files and directories owner-only (0o600 / 0o700) where the filesystem supports it, and you can mask sensitive keys with a redactor (see the [observability chapter](observability.md)). Note that a redacted checkpoint is not resumable to the exact original state, since masked values are lost — redaction is for telemetry you keep, not for state you plan to resume from.

Next: [human-in-the-loop](human-in-the-loop.md), which builds directly on checkpointing.
