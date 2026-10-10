# 2. Branching and loops

In chapter 1 the graph scored exactly the plan you handed it. Now we let the optimizer decide what to try and keep going until it cannot improve. That needs two new ideas: a conditional edge (a router) and a loop.

Runnable file: `examples/tutorial/02_branching.py`.

## A greedy improvement node

The node now does real work: it tries adding each strategy not yet in the plan, and keeps the single one that lowers the cost most this round. If nothing helps, it changes nothing.

```python
async def improve(state, ctx) -> dict:
    plan = state["plan"]
    current = cost_of(plan)

    best_strategy, best_cost = None, current
    for strategy in STRATEGY_FACTORS:
        if strategy in plan:
            continue
        trial = cost_of(plan | {strategy})
        if trial < best_cost:
            best_cost, best_strategy = trial, strategy

    if best_strategy is None:
        return {"steps": 1}                       # nothing improved
    return {"plan": plan | {best_strategy}, "cost": best_cost, "steps": 1}
```

Note the new `steps` channel. We declare it with the `add` reducer so each round's `{"steps": 1}` sums into a running count — a first taste of a reducer that is not `last`.

```python
class OptState(State):
    plan: Annotated[frozenset, last]
    cost: Annotated[float, last]
    steps: Annotated[int, add]     # rounds run, summed
```

## The router decides control flow

Control flow is not a callback inside the node. It is a separate function that reads state and returns a key. `add_conditional_edges` maps each key to a target — here, loop back to `improve` or finish.

```python
def route(state) -> str:
    plan = state["plan"]
    current = cost_of(plan)
    improvable = any(
        s not in plan and cost_of(plan | {s}) < current for s in STRATEGY_FACTORS
    )
    return "again" if improvable else "done"

g.add_node("improve", improve)
g.add_edge(START, "improve")
g.add_conditional_edges("improve", route, {"again": "improve", "done": END})
```

The edge from `improve` back to `improve` is the loop. Each pass is a super-step; the router runs after the node and picks the next one. There is no `while` loop in your code — the loop is a property of the graph.

## Run it

```text
optimizing from the empty plan:
  + add_index: 100.0 -> 40.0
  + push_filter: 40.0 -> 24.0
  + reorder_stages: 24.0 -> 21.6
final plan {'add_index', 'reorder_stages', 'push_filter'} -> cost 21.6 in 3 steps
```

The optimizer applied the three strategies that help and stopped before `cache_result`, which would have raised the cost. The loop terminated on its own because the router returned `done`.

## A word on safety

A conditional edge that can route back to itself is a loop, and a loop needs a reason to stop. Here the router stops when no move improves. `compile` also enforces a `step_limit` (default 100) as a backstop against a router that never says done — a runaway loop raises rather than hanging. We will lean on that guarantee later.

## What you learned

- A router is `state -> key`, wired by `add_conditional_edges`; it decides the next node.
- A loop is just an edge that leads back to an earlier node; the router ends it.
- Reducers other than `last`: `add` accumulates across rounds.
- `step_limit` guards against loops that never terminate.

The optimizer is still trying one strategy per round, sequentially. In [chapter 3](03-parallel.md) we try several at once — the moment the super-step model earns its keep.
