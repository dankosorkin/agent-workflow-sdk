# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""Control plane: manage graph runs outside the process that created them.

An execution plane (``CompiledGraph.invoke/stream/resume``) runs a graph inline
in your code. The control plane adds a durable :class:`RunQueue` of run
requests, a :class:`GraphRegistry` (name -> compiled graph), and a
:class:`Worker` pool that claims requests and executes them — so runs survive a
process restart, scale across workers, and can be listed, cancelled, and
resumed from anywhere sharing the same queue and registry.
"""

from typing import TYPE_CHECKING

from agentflow.controlplane.memory import MemoryRunQueue
from agentflow.controlplane.queue import RunQueue
from agentflow.controlplane.records import PoolHealth, QueueStats, RunRecord, RunStatus
from agentflow.controlplane.registry import GraphFactory, GraphRegistry
from agentflow.controlplane.worker import Worker, WorkerPool

if TYPE_CHECKING:  # for type checkers only; the runtime import is lazy below
    from agentflow.controlplane.postgres import PostgresRunQueue

__all__ = [
    "RunStatus",
    "RunRecord",
    "QueueStats",
    "PoolHealth",
    "RunQueue",
    "MemoryRunQueue",
    "PostgresRunQueue",
    "GraphRegistry",
    "GraphFactory",
    "Worker",
    "WorkerPool",
]


def __getattr__(name: str):
    # PostgresRunQueue needs the optional `postgres` extra; import it lazily.
    if name == "PostgresRunQueue":
        from agentflow.controlplane.postgres import PostgresRunQueue

        return PostgresRunQueue
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
