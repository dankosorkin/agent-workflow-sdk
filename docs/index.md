# AgentFlow

AgentFlow is an async-first, graph-based SDK for building agent workflows out of composable pieces. You describe a workflow as a graph of nodes over a typed, reducer-based state, then run it against a pluggable backend — a coding agent (Kiro, Codex, Claude Code) or a plain LLM (Ollama, any OpenAI-compatible endpoint, or Anthropic).

The import root is `agentflow`. The distribution name is `agent-workflow-sdk`.

## Why AgentFlow

- Build any workflow from primitives: nodes, edges, conditional routing, and a shared state. Not a fixed loop, not a linear chain.
- Swap the backend without touching workflow code. Agents and LLMs share one event vocabulary.
- Async everywhere: the engine, the backends, checkpointing, and streaming.
- Durable by default: every super-step is checkpointed, runs resume after a crash, and human-in-the-loop interrupts suspend and resume a run.

## The whole idea in one screen

```python
import asyncio
from typing import Annotated
from agentflow import Graph, START, END, State, add, append

class CountState(State):
    n: Annotated[int, add]         # updates are summed
    log: Annotated[list, append]   # updates are concatenated

async def tick(state, ctx):
    return {"n": 1, "log": f"tick {state.get('n', 0) + 1}"}

def route(state):
    return "again" if state["n"] < 5 else "done"

g = Graph(CountState)
g.add_node("tick", tick)
g.add_edge(START, "tick")
g.add_conditional_edges("tick", route, {"again": "tick", "done": END})
app = g.compile()

print(asyncio.run(app.invoke({"n": 0, "log": []})))
```

That is the entire mental model: a typed state, nodes that return partial updates, edges that decide what runs next, and a compiled app you `invoke`, `stream`, or `resume`.

## How this book is organized

This guide reads front to back like a book, but each chapter also stands on its own.

- Get started walks you from install to a running graph and the mental model behind it.
- Core concepts is the heart: state and reducers, nodes and edges, the super-step execution model, the `Context` object, running a graph, and subgraphs.
- Backends covers the two backend families, the concrete backends, permissions, and retries.
- Durability and operations covers checkpointing, human-in-the-loop, quality gates, the cross-thread store, the control plane, and observability.
- Patterns covers the prebuilt helpers, the long-running-loop recipes, and a full end-to-end tutorial.
- Reference is a quick API cheatsheet, a glossary, and a generated API reference.

## A note on scope

AgentFlow is a library, not a service. It gives you a durable execution engine, pluggable backends, and a control plane you can drive from your own process. It does not ship an HTTP server, auth, or multi-tenancy — those belong to a service layer you build on top. See the control plane chapter for where that line sits.

New here? Start with the [overview](get-started/overview.md), then the [quickstart](get-started/quickstart.md).
