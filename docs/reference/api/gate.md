# Quality gates

The gate contract and helper. See the [Quality gates](../../durability/quality-gates.md) guide, and the [idempotency recipe](../../patterns/long-running-loops.md) for `artifact_key` / `skip_if_done`.

::: agentflow.prebuilt.gate
    options:
      members:
        - GateResult
        - GateState
        - Evaluator
        - add_quality_gate

## Idempotency helpers

::: agentflow.prebuilt.idempotent
    options:
      members:
        - artifact_key
        - skip_if_done
