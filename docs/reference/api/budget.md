# Token / cost budget

Accumulate token and cost usage, and stop an autonomous run at a cap. See the [budget-aware stopping recipe](../../patterns/long-running-loops.md#recipe-budget-aware-stopping).

Usage is surfaced on [`TurnEnd.usage`](events.md) by backends that report it.

::: agentflow.prebuilt.budget
    options:
      members:
        - BudgetState
        - record_usage
        - over_budget
        - TokenMeter
