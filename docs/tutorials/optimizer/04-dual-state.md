# 4. Dual-state with by_key

By chapter 3 the state was one flat bag: `plan`, `cost`, and a growing `trials` list we had to filter every round. Two different kinds of data are tangled together — the fixed facts of the problem, and the moving state of the search. This chapter separates them, and in doing so removes the awkward filtering with a channel whose fields each fold their own way.

Runnable file: `examples/tutorial/04_dual_state.py`.

## Two kinds of state

- The problem is fixed: the base cost, the available strategies, any constraints. It is set once and barely changes. A plain `merge` channel (last-wins per key) is enough.
- The search is everything the optimizer discovers: the candidates it has tried and the incumbent — the best plan so far. Its fields do not all fold the same way. Candidates accumulate; the incumbent is replaced whole each time a better plan wins.

A single `merge` cannot express "accumulate this field, replace that one". That is exactly what `by_key` is for: one dict channel where each field has its own reducer.

```python
from agentflow import State, append, by_key, last, merge

class OptState(State):
    problem: Annotated[dict, merge]            # fixed facts, last-wins per key
    search: Annotated[dict, by_key({
        "candidates": append,                  # every trial accumulates
        "incumbent": last,                     # the best plan, replaced whole
    })]
```

## Trials write into the search channel

A trial now extends the incumbent plan and appends a candidate under `search.candidates`:

```python
async def trial(state, ctx) -> dict:
    incumbent = state["search"].get("incumbent") or {"plan": frozenset(), "cost": BASE_COST}
    plan = incumbent["plan"]
    if strategy in plan:
        return {}
    cand = {"plan": plan | {strategy}, "cost": cost_of(plan | {strategy})}
    return {"search": {"candidates": [cand]}}
```

Note the shape of the update: `{"search": {"candidates": [cand]}}`. The outer key is the channel; the inner dict is folded field-by-field by `by_key`. Only `candidates` is touched, so `incumbent` is left exactly as it was — two trials in the same super-step both appending candidates never disturb the incumbent or each other.

## The promotion boundary

A single node decides what becomes the new incumbent. It looks only at this round's candidates (the incumbent plan plus one strategy) and promotes the best that beats the current cost:

```python
async def promote(state, ctx) -> dict:
    incumbent = state["search"].get("incumbent") or {"plan": frozenset(), "cost": BASE_COST}
    plan = incumbent["plan"]
    candidates = state["search"].get("candidates", [])
    this_round = [c for c in candidates if plan.issubset(c["plan"]) and len(c["plan"]) == len(plan) + 1]
    better = [c for c in this_round if c["cost"] < incumbent["cost"]]
    if not better:
        return {}
    best = min(better, key=lambda c: c["cost"])
    return {"search": {"incumbent": best}}
```

The incumbent is replaced whole — `{"search": {"incumbent": best}}` — because `incumbent` uses `last`. There is no partial patch of the incumbent's fields; a new best is a new object. This is deliberate: a single writer replacing a whole value is simpler to reason about than many writers patching pieces of it, and it is exactly what `last` gives you.

## Why this matters beyond tidiness

Separating `problem` from `search` is not just organization. It encodes a rule: deterministic facts live in `problem` and are written by code you trust; discovered results live in `search` and are the only thing the optimizer mutates as it runs. When we add a model-driven proposer in [chapter 5](05-backend.md), it will read `problem` and write `search` — and the boundary makes it obvious what the model is allowed to touch. That separation is the backbone of every later chapter.

## Run it

```text
optimizing with dual-state (problem vs search):
  promote {'add_index'} cost 40.0
  promote {'add_index', 'push_filter'} cost 24.0
  promote {'add_index', 'reorder_stages', 'push_filter'} cost 21.6
final plan {'add_index', 'reorder_stages', 'push_filter'} -> cost 21.6
candidates evaluated across the run: 9
```

## What you learned

- `by_key` gives one dict channel per-field reducers: `candidates` appends, `incumbent` replaces.
- An update writes only the fields it changes; concurrent writers touching different fields never clobber each other.
- A promotion boundary is a single node that replaces the incumbent whole with `last`.
- Separating fixed `problem` facts from the moving `search` encodes who may write what.

So far the proposer is hand-written: it tries every strategy. In [chapter 5](05-backend.md) we let a model propose moves — while keeping the verifier deterministic, so the optimizer never trusts the model's own claim about a plan.
