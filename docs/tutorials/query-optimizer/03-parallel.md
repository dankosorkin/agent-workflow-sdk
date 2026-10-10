# 3. Parallel experiments

Each candidate is scored by applying its indexes to its own scratch database and running `EXPLAIN` — independent work. There is no reason to score them one at a time. This chapter scores a whole round's candidates concurrently.

Runnable file: `examples/tutorial/queryopt/ch03_parallel.py`.

## score_candidates fans out

The scoring primitive already does the concurrency; you pass `max_concurrency` to bound it:

```python
scored = await score_candidates(candidates, problem, scorer, max_concurrency=8)
best = best_of(scored)
```

Each candidate's scorer builds a fresh scratch database, so they do not interfere — the concurrency is safe by construction. On SQLite the scores are fast; on a client-server database where a score means a real `EXPLAIN` round-trip, this is the difference between a search that takes seconds and one that takes minutes. `max_concurrency` is the knob that keeps a wide round from hammering the database with every candidate at once.

```text
each round scores all candidates concurrently:
  round 1: scored 6 candidates, best value -15.0 (plan_cost 13.0)
  round 2: scored 5 candidates, best value -8.0 (plan_cost 4.0)
final value -8.0 with 2 indexes; 15 candidate evaluations across 3 rounds
```

## The super-step connection

Under the hood this is the same bulk-synchronous model the mechanics tutorial explains: a frontier of work runs concurrently, results fold deterministically. Here the "fan-out" is inside `score_candidates` (concurrent scorer calls) rather than across graph nodes — a deliberate choice, because scoring is a batch operation, not a branching of control flow. The order of the returned list matches the input order, so a deterministic scorer gives a deterministic winner regardless of how the concurrency resolved.

## What you learned

- `score_candidates(..., max_concurrency=N)` scores a round's candidates concurrently and deterministically.
- Each score runs against its own scratch database, so concurrent scoring is safe.
- Bounded concurrency is what makes the search practical when a single score is an expensive database operation.

In [chapter 4](04-dual-state.md) we give the search a structured state — fixed problem facts apart from the moving incumbent — with a `by_key` channel.
