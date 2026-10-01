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

"""AgentFlow — async-first agent workflow SDK.

Importing this package pulls in only the dependency-free core (state, graph,
runtime, checkpointing). Backends live under :mod:`agentflow.backends` and are
imported explicitly so the core never drags in a subprocess or HTTP library.
"""

from __future__ import annotations

from agentflow.checkpoint import (
    Checkpoint,
    Checkpointer,
    FileCheckpointer,
    MemoryCheckpointer,
    SqliteCheckpointer,
    ThreadInfo,
)
from agentflow.compiled import CompiledGraph
from agentflow.controlplane import (
    GraphRegistry,
    MemoryRunQueue,
    PoolHealth,
    QueueStats,
    RunQueue,
    RunRecord,
    RunStatus,
    Worker,
    WorkerPool,
)
from agentflow.errors import (
    AgentFlowError,
    BackendError,
    BackendRateLimitError,
    BackendTransportError,
    CheckpointConflict,
    CheckpointError,
    CompilationError,
    ControlPlaneError,
    GraphError,
    InterruptError,
    NodeError,
    RunNotFound,
    RunTimeout,
    StoreConflict,
    StoreError,
)
from agentflow.events import (
    Allow,
    ChatRequest,
    Deny,
    ErrorEvent,
    GateEvent,
    Message,
    PermissionOption,
    PermissionRequest,
    TextChunk,
    TextRequest,
    ToolCall,
    ToolCallSpec,
    ToolResult,
    ToolSpec,
    TurnEnd,
)
from agentflow.graph import END, START, Graph, Node, Router
from agentflow.interrupts import (
    ClarificationRequest,
    QualityGateReview,
    interrupt_type,
)
from agentflow.observability import Hooks, NodeStat, RunMetrics
from agentflow.redaction import RedactKeys, Redactor, redact_none
from agentflow.runtime import Context, StreamEvent
from agentflow.state import (
    Channel,
    Reducer,
    State,
    add,
    append,
    last,
    merge,
    union,
)
from agentflow.store import Item, MemoryStore, Store
from agentflow.telemetry import JsonlTelemetry, MultiHooks

__version__ = "0.2.0"

__all__ = [
    "__version__",
    # state
    "State",
    "Reducer",
    "Channel",
    "last",
    "append",
    "add",
    "merge",
    "union",
    # graph
    "Graph",
    "START",
    "END",
    "Node",
    "Router",
    "CompiledGraph",
    "Context",
    "StreamEvent",
    # observability + telemetry
    "Hooks",
    "RunMetrics",
    "NodeStat",
    "MultiHooks",
    "JsonlTelemetry",
    "Redactor",
    "RedactKeys",
    "redact_none",
    # events
    "Message",
    "ToolSpec",
    "ToolCallSpec",
    "TextRequest",
    "ChatRequest",
    "TextChunk",
    "ToolCall",
    "ToolResult",
    "PermissionRequest",
    "PermissionOption",
    "TurnEnd",
    "ErrorEvent",
    "GateEvent",
    "Allow",
    "Deny",
    # human-in-the-loop interrupt payloads
    "QualityGateReview",
    "ClarificationRequest",
    "interrupt_type",
    # checkpointing
    "Checkpoint",
    "Checkpointer",
    "ThreadInfo",
    "MemoryCheckpointer",
    "FileCheckpointer",
    "SqliteCheckpointer",
    "RedisCheckpointer",
    "PostgresCheckpointer",
    # store (cross-thread memory)
    "Item",
    "Store",
    "MemoryStore",
    "PostgresStore",
    # control plane
    "RunStatus",
    "RunRecord",
    "QueueStats",
    "PoolHealth",
    "RunQueue",
    "MemoryRunQueue",
    "PostgresRunQueue",
    "GraphRegistry",
    "Worker",
    "WorkerPool",
    # errors
    "AgentFlowError",
    "GraphError",
    "CompilationError",
    "NodeError",
    "RunTimeout",
    "BackendError",
    "BackendTransportError",
    "BackendRateLimitError",
    "CheckpointError",
    "CheckpointConflict",
    "StoreError",
    "StoreConflict",
    "ControlPlaneError",
    "RunNotFound",
    "InterruptError",
]


def __getattr__(name: str):
    # These need optional extras (`redis` / `postgres`); import lazily so
    # `import agentflow` works (and stays extra-free) without them installed.
    if name == "RedisCheckpointer":
        from agentflow.checkpoint import RedisCheckpointer

        return RedisCheckpointer
    if name == "PostgresCheckpointer":
        from agentflow.checkpoint import PostgresCheckpointer

        return PostgresCheckpointer
    if name == "PostgresStore":
        from agentflow.store import PostgresStore

        return PostgresStore
    if name == "PostgresRunQueue":
        from agentflow.controlplane import PostgresRunQueue

        return PostgresRunQueue
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
