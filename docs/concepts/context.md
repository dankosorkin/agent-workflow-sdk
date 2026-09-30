# The Context object

Every node is called as `(state, ctx)`. The `state` is the data; the `ctx` is a small per-node handle that carries the node's identity and gives it two abilities: surface events, and suspend for a human.

## Identity

```python
async def node(state, ctx):
    ctx.node    # this node's name
    ctx.thread  # the run's thread id
    ctx.step    # the current super-step number
```

These are useful for logging and for keying external work by run.

## ctx.emit — surface an event

`ctx.emit(event)` pushes an event to two places at once: the run's live stream (so `app.stream` sees it) and the telemetry hooks (so `on_event` fires). Use it to stream a model's tokens or to record tool calls and gate decisions.

```python
from agentflow.events import TextChunk

async def chat_node(state, ctx):
    text = []
    async for ev in llm.chat(state["messages"]):
        if isinstance(ev, TextChunk):
            ctx.emit(ev)          # -> stream + telemetry
            text.append(ev.text)
    return {"reply": "".join(text)}
```

`emit` accepts any backend event (`TextChunk`, `ToolCall`, `ToolResult`, …) and the run-level `GateEvent`. It is synchronous and best-effort: the telemetry delivery is scheduled fire-and-forget, so its ordering relative to node-start/end callbacks isn't guaranteed, and calling it outside a running loop is simply skipped. Rely on it for content and tracing, not for lifecycle accounting.

## ctx.interrupt — suspend for a human

`await ctx.interrupt(payload)` pauses the run for a person. On a fresh run it raises internally; the runtime catches it, writes an interrupted checkpoint carrying your `payload`, and stops. When the run is later resumed with a value, the same `interrupt` call returns that value instead of raising.

```python
async def confirm(state, ctx):
    answer = await ctx.interrupt({"question": "Ship it?", "diff": state["diff"]})
    return {"approved": answer == "yes"}
```

That's the whole human-in-the-loop mechanism — there's no special node type. Because the frontier is persisted, it works across process restarts. See [human-in-the-loop](../durability/human-in-the-loop.md) for the resume side and [quality gates](../durability/quality-gates.md) for a structured way to build on it.

## How helpers reach the context

The runtime sets the current node's context in a contextvar around each node call. That's how an `Interactive` permission policy — running inside a backend the node is consuming — can call `ctx.interrupt` without you threading `ctx` through every layer. A node and any backend generator it iterates share one task, and contextvars are per-task, so the right context is always in scope.

Next: [running a graph](running.md).
