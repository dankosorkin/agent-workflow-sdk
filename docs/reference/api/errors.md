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
        - ControlPlaneError
        - RunNotFound

## Effect idempotency

::: agentflow.prebuilt.effects.IncompleteEffectError
