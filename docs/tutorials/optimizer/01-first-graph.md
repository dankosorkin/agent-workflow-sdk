# 1. Your first graph

We start with the smallest thing that runs: one node that scores one plan. No branching, no loop, no model. By the end you will have seen the three ideas every graph is built from — state, a node, and a run.

Runnable file: `examples/tutorial/01_first_graph.py`.

## The problem, as data

The thing we optimize is a query plan, scored by a toy cost model. A plan is a set of applied strategy names; each strategy scales a base cost. This model is deterministic — the same plan always scores the same — so every run in this tutorial is reproducible.

```python
BASE_COST = 100.0
STRATEGY_FACTORS = {
    "add_index": 0.4,       # a big win
    "push_filter": 0.6,     # a solid win
    "reorder_stages": 0.9,  # a small win
    "cache_result": 1.1,    # actually a slight loss here
}

def cost_of(plan: frozenset[str]) -> float:
    cost = BASE_COST
    for strategy in plan:
        cost *= STRATEGY_FACTORS.get(strategy, 1.0)
    return round(cost, 2)
```

Keep this function in mind: it is the ground truth the optimizer tries to minimize, and it stays unchanged through all ten chapters. Everything we build is about *choosing which strategies to apply*; `cost_of` just tells us how good a choice was.

## State is typed channels

A graph operates on a state: a `TypedDict` subclass of `State`, where each field is a channel with a reducer. A reducer decides how a node's update folds into the current value. For now both channels use `last` (the new value replaces the old).

```python
from typing import Annotated
from agentflow import State, last

class OptState(State):
    plan: Annotated[frozenset, last]   # the applied strategies
    cost: Annotated[float, last]       # the modeled cost of that plan
```

## A node is an async function

A node takes the current state and a context, and returns a partial update — a dict of channel names to new values. It never mutates the state it is given; it describes a change.

```python
async def score(state, ctx) -> dict:
    plan = state["plan"]
    return {"cost": cost_of(plan)}
```

`score` reads the `plan` channel and writes the `cost` channel. That is the whole node.

## Wiring and running

A graph needs a path from `START` to `END`. Here it is a single hop through `score`.

```python
from agentflow import END, START, Graph

def build() -> Graph:
    g = Graph(OptState)
    g.add_node("score", score)
    g.add_edge(START, "score")
    g.add_edge("score", END)
    return g

app = build().compile()
out = await app.invoke({"plan": frozenset({"add_index"})})
print(out["cost"])   # -> 40.0
```

`compile` validates the structure (every node reachable, no dead ends) and returns something you can `invoke`. `invoke` seeds the initial state, runs the graph to `END`, and returns the final state.

Run the file and you will see the one-strategy plan score 40.0 (100 × 0.4) and the empty baseline score 100.0.

## What you learned

- State is typed channels, each with a reducer; `last` means last-value-wins.
- A node is `async (state, ctx) -> update` and returns a partial update, never mutating state.
- `compile` validates; `invoke` runs from `START` to `END`.

Right now the optimizer evaluates exactly the plan you hand it. In [chapter 2](02-branching.md) we let it *decide* what to try, with a router and a loop.
