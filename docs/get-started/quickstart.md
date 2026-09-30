# Quickstart

This chapter builds a real, running graph from scratch and explains every line. No backend, no network — just the engine. By the end you will have written state, nodes, edges, and run the graph three different ways.

## 1. Declare a state

State is a `TypedDict` subclass of `State`. Each field is a channel. You annotate a channel with a reducer that decides how a node's update folds into the current value.

```python
from typing import Annotated
from agentflow import State, add, append

class CountState(State):
    n: Annotated[int, add]          # each update is summed into n
    log: Annotated[list, append]    # each update is concatenated onto log
```

`add` means "sum the updates". `append` means "concatenate onto the list". A field with no reducer annotation uses `last` (last-value-wins).

## 2. Write a node

A node is an async callable `(state, ctx) -> update`. It reads the state it is given and returns a partial update — a dict of channel names to new values. It never mutates the state.

```python
async def tick(state, ctx):
    next_n = state.get("n", 0) + 1
    return {"n": 1, "log": f"tick {next_n}"}
```

Note what the update means under the reducers: `{"n": 1}` adds 1 to `n` (because `n` uses `add`), and `{"log": "tick 3"}` appends one line to `log` (because `log` uses `append`). The node returns deltas, not absolute values.

## 3. Write a router

A router is a plain function `state -> key`. The key selects an edge. Routers are where loops and branches live.

```python
def route(state):
    return "again" if state["n"] < 5 else "done"
```

## 4. Build and compile the graph

```python
from agentflow import Graph, START, END

g = Graph(CountState)
g.add_node("tick", tick)
g.add_edge(START, "tick")
g.add_conditional_edges("tick", route, {"again": "tick", "done": END})
app = g.compile()
```

Read the edges out loud: start at `tick`; after `tick`, ask the router — if it says `again`, loop back to `tick`; if it says `done`, go to `END`.

`compile()` validates the graph. It checks that every node is reachable, that conditional routes map to real nodes, and that every edge points somewhere real. A broken graph fails here, not at run time.

## 5. Run it

There are three ways to run a compiled graph.

### invoke — run to completion

```python
import asyncio

result = asyncio.run(app.invoke({"n": 0, "log": []}))
print(result)
# {'n': 5, 'log': ['tick 1', 'tick 2', 'tick 3', 'tick 4', 'tick 5']}
```

`invoke` returns the final state as a plain dict.

### stream — watch it happen

```python
async def main():
    async for event in app.stream({"n": 0, "log": []}):
        if event.kind in ("node_end", "done"):
            print(event.kind, "step", event.step, "node", event.node)

asyncio.run(main())
```

`stream` yields `StreamEvent`s as the run progresses — one per node start, node end, step boundary, and a final `done` event whose `data` is the final state.

### invoke_sync — for non-async callers

If you are not in an async context, use the blocking wrapper:

```python
result = app.invoke_sync({"n": 0, "log": []})
```

It runs the coroutine via `asyncio.run` and refuses to run inside an existing event loop.

## The complete program

```python
import asyncio
from typing import Annotated
from agentflow import Graph, START, END, State, add, append

class CountState(State):
    n: Annotated[int, add]
    log: Annotated[list, append]

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

A runnable version of this lives in `examples/hello_graph.py`.

## What you just learned

- State is typed channels, each with a reducer.
- Nodes return partial updates; they never mutate state.
- Edges — static and conditional — drive control flow, including loops.
- `compile()` validates before you run.
- You run a graph with `invoke`, `stream`, or the `_sync` wrappers.

Next: the [mental model](mental-model.md) ties these together and explains why the engine works the way it does.
