# Errors

The exception hierarchy. See individual guides for where each is raised.

::: agentflow.errors
    options:
      members:
        - AgentFlowError
        - GraphError
        - CompilationError
        - NodeError
        - RunTimeout
        - InterruptError
        - BackendError
        - BackendTransportError
        - BackendRateLimitError
        - CheckpointError
        - CheckpointConflict
        - StoreError
        - StoreConflict
        - ControlPlaneError
        - RunNotFound

## Effect idempotency

::: agentflow.prebuilt.effects.IncompleteEffectError
