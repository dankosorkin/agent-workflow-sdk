# Human-in-the-loop

Some steps need a person: approve a risky action, pick between options, supply a missing value. AgentFlow handles this with interrupts. A node suspends the run, the run persists, and later — possibly in another process — it resumes with the human's answer. This chapter walks the full pattern.

## The mechanism

Human-in-the-loop is built entirely on checkpointing and `ctx.interrupt`. There is no separate subsystem. A node calls `await ctx.interrupt(payload)`; the runtime writes an interrupted checkpoint and stops; `app.resume(thread, value=answer)` continues, and the interrupt call returns `answer`.

It requires a checkpointer — there is nothing to suspend to, or resume from, without one.

## A complete example

```python
import asyncio
from typing import Annotated
from agentflow import Graph, START, END, State, last, MemoryCheckpointer

class S(State):
    diff: Annotated[str, last]
    approved: Annotated[bool, last]

async def review(state, ctx):
    # Suspend until a human answers. On a fresh run this stops here;
    # on resume, `decision` is the value passed to resume().
    decision = await ctx.interrupt({"question": "Apply this diff?", "diff": state["diff"]})
    return {"approved": decision in ("y", "yes", True)}

def route(state):
    return "apply" if state["approved"] else "skip"

async def apply(state, ctx):
    return {}     # ... perform the change ...

async def skip(state, ctx):
    return {}

g = Graph(S)
g.add_node("review", review)
g.add_node("apply", apply)
g.add_node("skip", skip)
g.add_edge(START, "review")
g.add_conditional_edges("review", route, {"apply": "apply", "skip": "skip"})
g.add_edge("apply", END)
g.add_edge("skip", END)

app = g.compile(checkpointer=MemoryCheckpointer())

async def main():
    # 1. Start the run. It suspends at the interrupt and returns.
    await app.invoke({"diff": "- old\n+ new", "approved": False}, thread="pr-1")

    # 2. Inspect: the run is interrupted, not done.
    cp = await app.get_state("pr-1")
    assert cp.interrupted and not cp.done
    print("waiting on:", cp.interrupt_payload["question"])

    # 3. Resume with the human's answer.
    final = await app.resume("pr-1", value="yes")
    print("approved:", final["approved"])

asyncio.run(main())
```

The run stops cleanly at step one, persists, and only advances when you resume it. The human's `"yes"` becomes the return value of `ctx.interrupt` inside `review`.

## Inspecting a suspended run

After a run suspends, the latest checkpoint tells you everything you need to build a UI or a queue around it:

```python
cp = await app.get_state(thread)
if cp.interrupted:
    who = cp.interrupt_node          # which node is waiting
    ask = cp.interrupt_payload       # what to show the human
    # ... present `ask`, collect an answer, then resume ...
```

## Resuming across a restart

Because the frontier and the interrupt live in the checkpoint, the process that resumes need not be the process that suspended. Suspend in a web request handler, store nothing yourself, and resume from a worker hours later — the checkpointer holds the state. This is what makes durable approvals possible.

```python
# process A (e.g. an API handler)
await app.invoke(inp, thread="pr-1")     # suspends, persists, returns

# process B (e.g. a worker, later), sharing the same checkpointer
await app.resume("pr-1", value="approved")
```

## Streaming a suspend

When streaming, the suspend surfaces as a `StreamEvent` of kind `"interrupt"` whose `data` is the payload. Resume with the streaming form:

```python
async for ev in app.stream(inp, thread="pr-1"):
    if ev.kind == "interrupt":
        answer = collect_from_human(ev.data)
        break

async for ev in app.stream_resume("pr-1", value=answer):
    ...
```

## interrupt vs permission Callback, again

This is worth repeating because it is the most common design question. Both put a human in the loop:

- `ctx.interrupt` (this chapter) suspends the whole run durably. Use it when the human might take a while, or when the decision-maker is in a different process. It also underpins the `Interactive` permission policy.
- A permission `Callback` asks inline in a single open agent turn and never suspends. Use it for a persistent-session agent (Kiro) that is holding a turn open waiting for a yes/no on a tool.

If you are approving an agent's tool call in a live Kiro session, reach for `Callback`. If you are approving a workflow decision that might sit for minutes, reach for `interrupt`.

## In the control plane

The control plane turns this into a first-class run status. An interrupted run is re-queued with the human's answer via `enqueue_resume(run_id, value=...)`, and a worker resumes it. See the [control plane chapter](control-plane.md).

Next: [cross-thread memory](store.md).
