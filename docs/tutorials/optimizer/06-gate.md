# 6. The promotion gate

In chapter 5 the `verify` node promoted a candidate silently and the router decided whether to continue. That conflates two decisions — "is this candidate good enough?" and "where do we go next?" — in ad-hoc code. This chapter makes the first decision an explicit quality gate: a checked verdict with three routes and an attempt budget, using `add_quality_gate`.

Runnable file: `examples/tutorial/06_gate.py`.

## Why a gate here

A quality gate answers "is this good enough?" and routes three ways: accept, try again, or give up. The optimizer's promotion decision fits exactly:

- pass — this round produced a candidate that beats the incumbent, so promote it and keep searching.
- fail — nothing beat the incumbent this round; let the proposer try again (with attempts left).
- escalate — the proposer kept failing to improve for the whole attempt budget; stop and report.

The gate owns the attempt counting and the three-way routing. We just supply the verdict.

## The evaluator returns a verdict, not an action

Mix `GateState` into the schema (it adds the `gate_attempts` and `gate_results` bookkeeping channels), then write an evaluator that judges the round:

```python
from agentflow.prebuilt import GateResult, GateState, add_quality_gate

class OptState(GateState):
    problem: Annotated[dict, merge]
    search: Annotated[dict, by_key({"candidates": append, "incumbent": last})]

async def evaluate(state, ctx) -> GateResult:
    best = _best_this_round(state)
    if best is None:
        return GateResult(passed=False, feedback="no candidate beat the incumbent")
    improvement = state["search"]["incumbent"]["cost"] - best["cost"]
    return GateResult(passed=True, score=improvement, reasons=[f"best={best['cost']}"])
```

The evaluator does not promote anything or decide where to go. It returns a `GateResult`. The gate turns that into a route.

## Wiring the three routes

```python
add_quality_gate(
    g, "gate", evaluate,
    on_pass="promote",     # a worthy candidate -> promote it
    on_fail="propose",     # nothing good this round -> propose again
    on_escalate="report",  # repeatedly no improvement -> give up
    max_attempts=3,
)
g.add_edge(START, "propose")
g.add_edge("propose", "gate")
g.add_conditional_edges("promote", after_promote, {"again": "propose", "stop": "report"})
g.add_edge("report", END)
```

All three targets are required. That is the gate's safety property: it can never silently accept a failing result once it runs out of attempts — it must route somewhere you named, so an exhausted search ends at `report`, not in limbo.

## The routes in action

Two runs show both exits. Optimizing from the empty plan, every round promotes until no move helps, and `after_promote` stops cleanly:

```text
optimizing with a promotion gate:
  promote {'add_index'} cost 40.0
  promote {'add_index', 'push_filter'} cost 24.0
  promote {'add_index', 'reorder_stages', 'push_filter'} cost 21.6
  stop: best plan {'add_index', 'reorder_stages', 'push_filter'} cost 21.6
```

Starting already at the optimum, no candidate ever beats the incumbent. The gate fails, retries `propose` up to `max_attempts=3`, then escalates to `report`:

```text
propose -> gate -> propose -> gate -> propose -> gate -> report
```

That second path is the one that matters for an autonomous run: a search that stops making progress does not spin forever: it hits the attempt budget and escalates to a node you chose.

## Gates emit events

Each visit emits a `GateEvent` (`started`, then `passed` / `failed` / `escalated`) through `ctx.emit`, so a telemetry sink or the run stream shows every decision and how many times the search looped on quality. You get that for free; it is how you observe an autonomous optimizer's judgement after the fact.

## What you learned

- A quality gate separates the verdict (an evaluator returning `GateResult`) from the routing and attempt budget (owned by `add_quality_gate`).
- Three required routes — pass, fail, escalate — mean a stalled search always ends somewhere you named.
- `GateState` supplies the attempt/result bookkeeping channels; mix it into your schema.
- Gates emit decision events for observability.

The optimizer now decides for itself when to stop. But some decisions should not be automatic — applying an expensive plan might need a human's sign-off. In [chapter 7](07-human.md) we pause the run for approval with `ctx.interrupt`.
