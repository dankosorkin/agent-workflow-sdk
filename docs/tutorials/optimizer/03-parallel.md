# 3. Parallel experiments

Chapter 2 tried one strategy per round, in sequence. But the trials are independent — evaluating `add_index` tells us nothing about evaluating `push_filter`. There is no reason to run them one at a time. In this chapter the optimizer tries every remaining strategy at once, in a single super-step, and a picker promotes the best. This is where the engine's execution model earns its keep.

Runnable file: `examples/tutorial/03_parallel.py`.

## The super-step model

The engine does not walk one node at a time. It advances in super-steps. Each super-step:

1. runs the current frontier of nodes — all of them, concurrently, against the same immutable state;
2. folds their partial updates into the state through the channel reducers, in a deterministic order;
3. computes the next frontier from the edges.

A single node is a frontier of one. Fan-out is a frontier of several. That is the whole idea: to run trials in parallel, we make them a frontier of several.

## Fan-out: one node per trial

A `dispatch` node fans out to one trial node per remaining strategy. The router returns a list of keys, and every mapped target joins the next frontier — so they run together.

```python
def fan_out(state) -> list[str]:
    plan = state["plan"]
    remaining = [s for s in STRATEGY_FACTORS if s not in plan]
    return remaining or ["none"]

targets = {s: f"trial_{s}" for s in STRATEGY_FACTORS}
targets["none"] = "none"
g.add_conditional_edges("dispatch", fan_out, targets)
```

Each trial node evaluates adding its one strategy and appends the result:

```python
async def trial(state, ctx) -> dict:
    plan = state["plan"]
    if strategy in plan:
        return {}
    return {"trials": [(plan, strategy, cost_of(plan | {strategy}))]}
```

## Fan-in: a reducer merges the results

All trial nodes write the same `trials` channel in the same super-step. If `trials` were a plain variable, the last writer would win and we would lose every other trial. A reducer makes the merge well-defined: `append` concatenates all of them, in a deterministic order.

```python
class OptState(State):
    plan: Annotated[frozenset, last]
    cost: Annotated[float, last]
    trials: Annotated[list, append]     # every trial folds in here
```

This is why reducers exist. Concurrent writes in one super-step are not a race; they are a fold through a function you chose. The trial nodes all edge into `pick`, which runs once the whole frontier is done:

```python
async def pick(state, ctx) -> dict:
    plan = state["plan"]
    current = cost_of(plan)
    improving = [(s, c) for p, s, c in state.get("trials", []) if p == plan and c < current]
    if not improving:
        return {}
    strategy, new_cost = min(improving, key=lambda t: t[1])
    return {"plan": plan | {strategy}, "cost": new_cost}
```

## A real subtlety: you cannot clear an append channel

Look at how `pick` filters trials: `if p == plan`. The `trials` channel is `append`, so it *accumulates across rounds* — writing `{"trials": []}` would not clear it, because appending an empty list adds nothing. So each trial tags its result with the plan it was based on, and `pick` considers only entries for the current plan. Trying to "reset" an append channel by writing an empty value is a common first mistake; keying by what is current is the honest way to drain one.

This friction is a hint. Carrying growing scratch data in a flat channel and filtering it every round is awkward. [Chapter 4](04-dual-state.md) removes the accumulation entirely by giving the search its own structured channel — the moment `by_key` enters.

## State isolation keeps fan-out safe

When several nodes run in one super-step, each gets its own view of the input state, so an in-place mutation in one cannot corrupt a sibling. By default the engine deep-copies the input to each node only when more than one runs that step (the `fanout` isolation mode), so a linear graph pays nothing. You do not configure anything here; it is why concurrent trials are safe out of the box.

## Run it

```text
optimizing with parallel trials each round:
  best this round: + add_index: 100.0 -> 40.0
  best this round: + push_filter: 40.0 -> 24.0
  best this round: + reorder_stages: 24.0 -> 21.6
final plan {'push_filter', 'reorder_stages', 'add_index'} -> cost 21.6
```

Same optimum as chapter 2, but each round now evaluates all remaining strategies concurrently instead of one by one. On real work — a trial that calls a database or a model — that concurrency is the difference between seconds and minutes.

## What you learned

- The engine runs in super-steps: the whole frontier runs concurrently, updates fold through reducers, the next frontier is computed.
- Fan-out is a router returning a list of keys; fan-in is several nodes edging into one.
- `append` merges concurrent writes deterministically — reducers turn a race into a fold.
- An append channel does not clear on an empty write; key off what is current instead.
- State isolation makes concurrent nodes safe without configuration.

The optimizer is fast now, but its state is one flat bag: plan, cost, and a pile of trials all mixed together. In [chapter 4](04-dual-state.md) we separate the fixed problem from the moving search with a structured `by_key` channel.
