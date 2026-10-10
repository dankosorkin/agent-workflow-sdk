# 6. The promotion gate

The promotion decision becomes an explicit quality gate — accept, retry, or escalate — with an attempt budget, so a search that stops improving ends cleanly instead of spinning.

Runnable file: `examples/tutorial/queryopt/ch06_gate.py`.

## The evaluator judges the round

Mix `GateState` into the schema, then write an evaluator that asks whether the round found an index set worth promoting:

```python
async def evaluate(state, ctx) -> GateResult:
    rb = state.get("round_best") or {}
    if not rb:
        return GateResult(passed=False, feedback="no candidate beat the incumbent")
    return GateResult(passed=True, score=rb["value"])
```

The three routes: pass → promote and search again, fail → let the proposer try again, escalate → the search stopped improving within the attempt budget, so stop.

```python
add_quality_gate(g, "gate", evaluate,
                 on_pass="promote", on_fail="propose", on_escalate="report")
```

```text
optimizing with a promotion gate:
  promote value -15.0 (1 indexes)
  promote value -8.0 (2 indexes)
  stop: best value -8.0 with 2 indexes
```

## Feasibility is already enforced in the scorer

The gate decides "good enough to promote?"; the scorer already decided "valid at all?". A candidate whose rewrite changed the query's results is marked infeasible by the scorer, and `best_of` never returns an infeasible candidate — so a result-changing rewrite can never reach the gate as a promotable option, however low its plan cost. Correctness is a hard floor below the gate's quality decision.

## What you learned

- A quality gate separates the verdict (an evaluator) from routing and the attempt budget.
- Three required routes mean a stalled search always ends somewhere you named.
- Feasibility (rewrite-equivalence) lives in the scorer, beneath the gate — correctness is never traded for a better score.

In [chapter 7](07-human.md) we pause for human approval before applying the chosen indexes.
