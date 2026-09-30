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

`skip_if_done` avoids recomputing deterministic *work*; `IdempotentOp` protects a *side effect*. See [long-running loops](../../patterns/long-running-loops.md#side-effects-and-delivery-semantics) for when to use which.

::: agentflow.prebuilt.idempotent
    options:
      members:
        - artifact_key
        - skip_if_done

::: agentflow.prebuilt.effects
    options:
      members:
        - IdempotentOp
        - IncompleteEffectError
        - effect_key
