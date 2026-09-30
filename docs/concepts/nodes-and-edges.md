# Nodes and edges

If state is the data, nodes and edges are the program. A node does work; an edge decides what runs next. You assemble them on a `Graph` builder and freeze it with `compile()`.

## Nodes

A node is an async callable `(state, ctx) -> update`. It reads the state it's handed and returns a partial update (or `None` for no change). It must not mutate the state.

```python
async def summarize(state, ctx):
    text = await do_work(state["doc"])
    return {"summary": text}

g.add_node("summarize", summarize)
```

The second argument, `ctx`, is the node's [Context](context.md) — its identity plus `ctx.emit` and `ctx.interrupt`. A node may also be a compiled graph, embedded as a [subgraph](subgraphs.md).

## Static edges

A static edge always fires. `START` marks the entry, `END` marks the exit.

```python
from agentflow import START, END

g.add_edge(START, "summarize")   # entry
g.add_edge("summarize", END)     # terminal
```

## Conditional edges

A conditional edge routes by a router — a plain `state -> key` function — through a mapping of keys to targets. This is how you branch and loop.

```python
def route(state):
    return "retry" if state["score"] < 0.8 else "done"

g.add_conditional_edges("grade", route, {"retry": "revise", "done": END})
```

The router may return a single key or a list of keys. A list is a fan-out: every mapped target joins the next frontier and they run together in the next super-step. Each returned key must exist in the mapping, and each mapped target must be a real node or `END` — both checked at compile time.

## Fan-out and fan-in

Because a super-step runs its whole frontier concurrently, edges that lead several nodes into the same step fan out; edges that lead several nodes back to one node fan in. There is nothing special to configure — the [reducers](state.md) on your channels merge whatever the parallel nodes produce. Bound the width with `compile(max_node_concurrency=...)` when the branches share a scarce resource.

## Compiling validates the graph

`g.compile()` returns an immutable `CompiledGraph` after checking the structure, so a malformed graph fails at build time, not mid-run. It verifies that:

- there is at least one edge from `START`, and its targets are real nodes;
- every static and conditional edge points at a known node or `END`;
- every node is reachable from `START`;
- every node has an outgoing edge (add one to `END` if it's terminal), so nothing silently dead-ends.

```python
app = g.compile(
    checkpointer=None,        # durability; see the checkpointing chapter
    step_limit=100,           # guard against a graph that never reaches END
    hooks=None,               # observability
    max_node_concurrency=None,
    isolate_state="fanout",   # defensive copying; see the execution chapter
)
```

Next: [the execution model](execution.md) — how a compiled graph actually runs.
