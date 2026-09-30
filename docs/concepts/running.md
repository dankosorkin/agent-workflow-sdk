# Running a graph

A `CompiledGraph` is the immutable, runnable form of your graph. It offers a small, consistent surface: run to completion, stream events, resume a suspended run, and inspect history. Every method has an async form and, where it makes sense, a blocking `_sync` wrapper.

## invoke — run to completion

```python
final_state = await app.invoke(
    {"topic": "otters"},
    thread="run-1",       # the run's identity (default "default")
    timeout=None,         # optional whole-run timeout in seconds
)
```

`invoke` runs from the input to an empty frontier and returns the final state as a plain dict. If the run suspends on an interrupt, it returns the interrupted checkpoint's state instead — inspect `get_state(thread)` to tell the difference and then `resume`.

The `thread` names the run. Everything about it — its checkpoints, its resume point — hangs off that id. Use a fresh thread per run, reuse one to resume or inspect.

## stream — watch events as they happen

```python
async for ev in app.stream({"topic": "otters"}, thread="run-1"):
    ...
```

`stream` yields `StreamEvent`s as the run progresses. Each has a `kind`, a `step`, an optional `node`, and `data`:

| `kind` | When | `data` |
| --- | --- | --- |
| `node_start` | a node is about to run | — |
| `node_end` | a node finished | — |
| `backend` | a node called `ctx.emit` | the emitted event |
| `step` | a super-step completed | the next frontier |
| `interrupt` | the run suspended for a human | the interrupt payload |
| `done` | the run finished | the final state |

## resume — continue after an interrupt

When a run suspended on `ctx.interrupt`, `resume` continues it, feeding your value back into the paused node:

```python
final_state = await app.resume("run-1", value="yes")
```

`stream_resume(thread, value)` is the streaming twin. Both require a checkpointer — there's nothing to resume without persisted state. See [human-in-the-loop](../durability/human-in-the-loop.md).

## Inspection and time-travel

With a checkpointer configured, you can read the run's history:

```python
cp = await app.get_state("run-1")          # latest checkpoint
cp3 = await app.get_state("run-1", step=3) # a specific step
async for cp in app.history("run-1"):      # every step, oldest first
    print(cp.step, cp.next)
```

A `Checkpoint` tells you where a run stands: `cp.done` is true when it finished, `cp.interrupted` when it's parked for a human. See [checkpointing](../durability/checkpointing.md).

## Synchronous wrappers

For non-async callers:

```python
app.invoke_sync(input, thread="run-1")
app.resume_sync("run-1", value="yes")
app.stream_sync(input, thread="run-1")   # drains the stream into a list
```

Each runs the coroutine via `asyncio.run` and refuses to run inside an existing event loop — use the async form there.

Next: [subgraphs](subgraphs.md), for composing graphs out of graphs.
