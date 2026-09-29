# Copyright 2026 Daniel Sorkin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
