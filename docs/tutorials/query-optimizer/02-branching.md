# 2. Branching and loops

Chapter 1 scored one candidate. Now the optimizer searches: each round it tries adding one more index, keeps the best if it beats the current plan, and loops until nothing helps. Control flow is a router over state, exactly as in the mechanics tutorial — now driving the real EXPLAIN-based measure.

Runnable file: `examples/tutorial/queryopt/ch02_branching.py`.

## A greedy round, scored for real

The round node asks the proposer for candidates (each extends the applied indexes by one), scores them with the real scorer, and keeps the best only if it strictly beats the current value:

```python
async def improve(state, ctx) -> dict:
    applied = state.get("applied", ())
    current_value = state.get("value", float("-inf"))
    candidates = await proposer.propose(problem, applied)
    scored = await score_candidates(candidates, problem, scorer, max_concurrency=4)
    best = best_of(scored)
    if best is None or best[1].value <= current_value:
        return {"rounds": 1, "improved": False}
    best_cand, best_score = best
    return {"applied": best_cand.indexes, "value": best_score.value,
            "improved": True, "rounds": 1}
```

This already uses two pieces of the [`scoring`](../../reference/api/scoring.md) primitive — `score_candidates` and `best_of` — which we lean on throughout. `best_of` ignores infeasible candidates and keeps the incumbent unless strictly beaten, so the loop is stable.

## The router ends the loop

```python
def route(state):
    return "again" if state.get("improved") else "done"
```

The edge from `improve` back to itself is the loop; the router stops it when a round fails to improve.

```text
greedily adding indexes until nothing helps:
  round 1: +1 indexes -> value -15.0 (plan_cost 13.0)
  round 2: +2 indexes -> value -8.0 (plan_cost 4.0)
final value -8.0 with 2 indexes in 3 rounds
```

Notice round 1 adds an index but plan_cost stays 13 — a single index does not help this query. Round 2 finds the second index that, combined, drops the cost to 4. The optimizer discovered that indexes matter in combination, not individually. That is the combinatorial nature of the problem showing through on a real measure.

## What you learned

- A router over state drives the search loop; it ends when a round stops improving.
- `score_candidates` + `best_of` are the scoring primitive's batch-and-promote helpers.
- On a real query, the win comes from a combination of indexes, not any single one.

In [chapter 3](03-parallel.md) we score all of a round's candidates concurrently — the move that matters when each score is an expensive database operation.
