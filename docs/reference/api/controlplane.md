# Control plane

The registry, run queue, and workers. See [The control plane](../../durability/control-plane.md) guide.

::: agentflow.controlplane
    options:
      members:
        - GraphRegistry
        - RunQueue
        - MemoryRunQueue
        - PostgresRunQueue
        - Worker
        - WorkerPool
        - RunRecord
        - RunStatus
        - QueueStats
        - PoolHealth
