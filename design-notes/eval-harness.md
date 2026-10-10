# Design note: evaluation harness (backend-independent scoring)

Status: draft for review. A working design note, like `DESIGN.md` and
`plan-executor.md`. Target: a new prebuilt in `agentflow/prebuilt/`.

## The problem this closes

An optimizer loop has two halves: a *proposer* that suggests candidate
solutions (a heuristic, a local Codex agent, a hosted LLM) and a *verifier*
that scores them. The whole design rests on the verifier being deterministic
and backend-independent — otherwise swapping the proposer (Codex locally -> LLM
in prod) changes the measured result and you cannot tell an improvement from
noise.

Today that scoring is ad-hoc code inside a node (the toy `cost_of` in the
mechanics tutorial). There is no reusable contract that says: "here is a
candidate, score it on a fixed, deterministic scale, and the score is
comparable across whoever proposed it." This primitive makes that contract
explicit, so an eval is a first-class, swappable, backend-independent component
— the thing that lets "agents locally, LLM in prod" stay honest.

## What an eval is, precisely

An eval maps a *candidate* to a *score*, deterministically. It is pure with
respect to the proposer: the same candidate always yields the same score, no
matter who produced it or when. Scoring may do real work (run a query, execute
tests, call `EXPLAIN`) — "deterministic" means same candidate -> same score,
not "cheap" and not "no I/O".

```python
@dataclass(frozen=True)
class Evaluation:
    score: float                 # higher is better; the single comparable number
    feasible: bool = True        # did the candidate even satisfy hard constraints?
    metrics: dict[str, Any] = {} # components behind the score, for observability
    reason: str | None = None    # why infeasible / notable, for logs & humans
```

Why `score` is "higher is better": one convention end to end removes a whole
class of bugs (min-vs-max confusion when comparing). A cost domain negates:
`score = -cost`. Documented once, applied everywhere.

Why `feasible` is separate from `score`: a candidate can be invalid (a query
rewrite that returns different rows; an index set over budget). Infeasible
candidates are never promoted regardless of score — collapsing that into a very
low score invites a feasible-but-worse candidate to lose to an
infeasible-but-"better"-looking one. Keep the gate and the ranking separate.

## The evaluator contract

```python
Evaluator = Callable[[Candidate, Problem], Awaitable[Evaluation]]
```

- `Candidate` and `Problem` are domain types the consumer defines (for the
  query optimizer: a candidate is `{indexes, rewrite}`; a problem is
  `{schema, query, index_budget}`). The harness is generic over them.
- Async, because real evals do I/O (run the query, run `EXPLAIN`).
- Deterministic: the harness does not enforce this (it cannot), but every
  helper and doc states it as the contract. A non-deterministic evaluator
  breaks the comparability the whole thing exists for.

## What the harness provides (the primitive itself)

Not a graph builder this time — a scoring utility the consumer's verifier node
calls. It adds three things over "just call the evaluator":

1. `score_candidates(candidates, problem, evaluator, *, max_concurrency=None)`
   — evaluate many candidates (optionally concurrently), returning
   `[(candidate, Evaluation)]`. This is the batch a fan-out of proposers
   produces.
2. `best_of(scored, *, incumbent=None)` — pick the best *feasible* candidate,
   or keep the incumbent if nothing feasible beats it. The promotion rule in
   one place, so every optimizer promotes identically.
3. `EvalCache` — wrap an evaluator so an identical candidate is scored once
   (keyed by a content hash of the candidate, via the existing `artifact_key`).
   Real evals are expensive; the same candidate recurs across rounds and across
   a resume. This reuses `Store`, like `skip_if_done`.

Deliberately NOT a graph builder: an eval is called from inside a verifier node
that the consumer wires (often next to a `plan_executor` or a `by_key` search
channel). Forcing a graph shape would duplicate what those already give. The
harness is the scoring contract plus batch/promote/cache helpers.

## Determinism and the fake/real axis

The query-optimizer tutorial has two independent fake<->real axes, each behind
a flag, and the eval primitive is what makes the second one swappable:

- proposer: heuristic (default, offline) <-> Codex agent (local, `codex` on
  PATH) <-> LLM (prod). Chosen by a flag/env.
- evaluator: for SQLite, the eval is *always real* (SQLite is in-process, so a
  real `EXPLAIN QUERY PLAN` + execution needs no external service and is
  deterministic on fixed, seeded data). No fake needed. For a Mongo port later,
  the fake axis (recorded `explain` outputs) would slot in here as a second
  `Evaluator` implementation — same contract, different backend.

The point the primitive proves: the proposer axis and the evaluator axis move
independently. Swap the proposer, the eval is unchanged, so the score is
comparable — that is the honesty guarantee the user asked for.

## How the query optimizer uses it (first consumer)

- Problem: `{schema, query, index_budget}` over a seeded SQLite db.
- Candidate: `{indexes: [...], rewrite: str | None}`.
- Evaluator (deterministic, real):
  1. If `rewrite` is present, run both the original and the rewritten query and
     assert the result sets are equal — otherwise `feasible=False`
     ("rewrite changed results"). This is the equivalence check the user
     asked for.
  2. Apply the candidate indexes to a scratch copy of the db.
  3. Read `EXPLAIN QUERY PLAN`; derive a plan cost (count of SCAN vs SEARCH
     steps, estimated rows examined). This is the deterministic primary metric.
  4. `score = -(plan_cost + index_penalty * len(indexes))` — the trade-off the
     user specified: fewer, higher-impact indexes beat a pile of them.
  5. `metrics` carries the raw plan, examined estimate, and wall-clock time
     (reported for insight, used only as a tie-break, never as the primary
     score — wall-clock is not deterministic across machines).
- The verifier node calls `score_candidates` on the round's proposals and
  `best_of` to update the incumbent. Dual-state (`by_key`) holds candidates and
  incumbent exactly as in the mechanics tutorial.

## Open questions for review

1. Score convention "higher is better" with cost domains negating — agree, or
   prefer an explicit `minimize=True` flag on the harness? (I prefer the fixed
   convention; a flag spreads min/max logic to every call site.)
2. `feasible` as a separate field vs a sentinel score — I argue separate (see
   above). Agree?
3. Does `best_of` belong in the primitive, or is it one line the consumer
   writes? I lean toward including it, because the "ignore infeasible, keep
   incumbent on a tie" rule is subtle and worth centralizing — the same reason
   `plan_executor` owns readiness.
4. `EvalCache` keyed by `artifact_key(candidate)` — reuse the existing hash
   helper (yes), and default to a `MemoryStore` (yes), with a `Store` injectable
   for a durable/shared cache. Agree?
5. Naming: `Evaluation` / `Evaluator` / `score_candidates` / `best_of` /
   `EvalCache`. `Evaluator` collides in spirit with the gate's `Evaluator`
   (which returns a `GateResult`). Rename one? Options: gate stays `Evaluator`
   (it is about pass/fail routing), this becomes `Scorer` returning a `Score`.
   I lean `Scorer`/`Score` to avoid the collision and because "score" is the
   honest noun here. Your call.
