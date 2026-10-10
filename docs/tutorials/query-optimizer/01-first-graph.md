# 1. Your first graph, a real measure

The [mechanics tutorial](../optimizer/index.md) scored a plan with a toy formula. Here the score comes from a real database. This chapter is one node that scores one candidate against SQLite — enough to see that the measure is genuine before we build a search on top of it.

Runnable file: `examples/tutorial/queryopt/ch01_first_graph.py`. Run it with `python -m examples.tutorial.queryopt.ch01_first_graph`.

## The problem is real

The shared foundation (`examples/tutorial/queryopt/`) defines a seeded SQLite database — `customers`, `orders`, `order_items`, `products`, filled deterministically with ~40k orders — and the slow query under optimization: top products by revenue for enterprise customers in a quarter, joining all four tables. See the [overview](index.md) for the full schema.

A candidate is what we propose: a set of indexes and an optional query rewrite.

```python
@dataclass(frozen=True)
class Candidate:
    indexes: tuple[str, ...] = ()
    rewrite: str | None = None
```

## The node calls the real scorer

The engine shape is identical to the mechanics chapter 1 — a `State`, one node, `invoke`. What differs is the node body: it calls the deterministic SQLite [`Scorer`](../../reference/api/scoring.md), which applies the candidate's indexes to a scratch database, reads `EXPLAIN QUERY PLAN`, and returns a `Score`.

```python
scorer = make_scorer(problem)

async def score(state, ctx) -> dict:
    result = await scorer(state["candidate"], problem)
    return {"value": result.value, "metrics": result.metrics}
```

## The measure discriminates

Scoring the empty candidate versus one with indexes shows the plan actually change:

```text
empty plan:   value   -13.0  plan_cost 13.0
    SCAN oi
    SEARCH o USING INTEGER PRIMARY KEY (rowid=?)
    ...
indexed plan: value   -10.0  plan_cost 4.0
    SEARCH c USING COVERING INDEX i_c_segment (segment=?)
    SEARCH o USING INDEX i_o_customer (customer_id=?)
    SEARCH oi USING INDEX i_oi_order (order_id=?)
    ...
```

The empty plan starts with `SCAN oi` — a full scan of the largest table. With indexes, SQLite flips the join order to drive from the selective `segment` filter and turns every scan into an index search; the plan cost drops from 13 to 4. This is not a formula we wrote — it is SQLite's own planner, read back through `EXPLAIN`.

Note the value only moved from -13 to -10, not all the way: the score is `-(plan_cost + index_penalty * number_of_indexes)`, and three indexes carry a penalty. That trade-off is the whole reason the search is interesting — more indexes are not automatically better.

## What you learned

- The engine pieces are the same as the mechanics tutorial; only the node's measure changed.
- A candidate is indexes plus an optional rewrite; the scorer measures it against a real seeded SQLite database.
- The score is the plan cost from `EXPLAIN QUERY PLAN`, penalized by index count — deterministic, not a toy.

In [chapter 2](02-branching.md) we let the optimizer search: greedily add indexes until the plan stops improving.
