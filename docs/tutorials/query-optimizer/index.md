# Applied: a query optimizer with agents

The [mechanics tutorial](../optimizer/index.md) taught the engine on a toy cost model. This one is the real thing: an optimizer that is handed a slow SQL query and improves it by proposing indexes and rewrites, measuring each candidate against an actual database, and keeping the best. The proposer can be a local coding agent (Codex) or a hosted LLM; the measure is deterministic SQLite, so swapping the proposer never changes the score.

It is the same machinery as the mechanics tutorial — dual-state search, a promotion boundary, `plan_executor`, the control plane — pointed at a problem with real stakes.

## The problem: combinatorial optimization over a real query

We know what we want (a cheaper query plan that returns the same rows) but not how to get there (which of exponentially many index sets and rewrites is best). That gap — known objective, unknown path, expensive-to-evaluate candidates — is combinatorial optimization, and it is exactly where an agent earns its place: it reads the schema and query and proposes *meaningful* moves, instead of blindly enumerating every subset of indexes.

The database is SQLite, chosen deliberately: it is in-process, so a real `EXPLAIN QUERY PLAN` and a real query execution need no external service and are deterministic on fixed, seeded data. The whole tutorial runs offline and reproducibly, yet the measurement is genuine — not a stand-in like the mechanics tutorial's cost model.

### Schema and query

Four related tables — `customers`, `orders`, `order_items`, `products` — seeded deterministically with enough rows that a missing index actually hurts. The query under optimization is an analytical aggregation: top-N products by revenue over a date range for a customer segment, joining all four tables with a filter, `GROUP BY`, and `ORDER BY ... LIMIT`.

### The moves (what a candidate is)

A candidate is a set of indexes plus an optional query rewrite:

```python
{"indexes": ["CREATE INDEX ... ON orders(customer_id, order_date)", ...],
 "rewrite": "SELECT ... (an equivalent reformulation) ..." | None}
```

Indexes speed lookups but cost space and write time; rewrites can reorder joins or push filters down. The space of index combinations alone is exponential — the agent's job is to propose the few that matter.

### The measure (deterministic, backend-independent)

Scoring is the [`scoring`](../../reference/api/scoring.md) primitive with a real SQLite `Scorer`:

1. If the candidate has a rewrite, run both the original and rewritten query and assert the result sets are equal — otherwise the candidate is infeasible ("rewrite changed results"). Correctness is a hard gate, never traded for speed.
2. Apply the candidate's indexes to a scratch copy of the database.
3. Read `EXPLAIN QUERY PLAN` and derive a plan cost (table scans vs index searches, estimated rows examined) — the deterministic primary metric.
4. `value = -(plan_cost + index_penalty * number_of_indexes)` — the trade-off: fewer, higher-impact indexes beat a pile of them.
5. Wall-clock time is reported in `metrics` for insight but used only as a tie-break, never as the primary score — it is not deterministic across machines.

Because the score depends only on the candidate and the seeded database, it is identical no matter who proposed the candidate. That is the honesty guarantee: swap the proposer and the measure is unchanged, so a change in the result is a real change, not noise.

## Two fake↔real axes, each behind a flag

The tutorial has two independent switches, so it runs offline by default and goes live on your machine:

- Proposer: a deterministic heuristic (default, offline, for CI and reproducibility) ↔ a local Codex agent (`AGENTFLOW_TUTORIAL_BACKEND=codex`, needs `codex` on PATH) ↔ a hosted LLM (production). The agent reads the schema and query and proposes indexes and rewrites.
- Measure: always real. SQLite is in-process, so the `Scorer` runs genuine `EXPLAIN` and execution with no external service — no fake needed. (A port to a client-server database would add a recorded-plan fake here, same `Scorer` contract.)

The point this proves is the one that matters for "agents locally, LLM in production": the two axes move independently. You develop against a local agent, ship against an LLM, and the deterministic measure tells you honestly whether the swap helped or hurt.

## Chapters

This tutorial is being written. It will follow the same ten-layer arc as the mechanics tutorial — first graph, branching, parallel experiments, dual-state, a real agent proposer, the promotion gate, human approval, durability, waiting on a benchmark, and the plan-as-data finale — but every layer measured against a real database. Start with the [mechanics tutorial](../optimizer/index.md) for the engine fundamentals; this applied tutorial assumes them.
