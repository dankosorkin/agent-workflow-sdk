"""AgentFlow — async-first, LangGraph-style agent workflow SDK.

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
)
from agentflow.compiled import CompiledGraph
from agentflow.errors import (
    AgentFlowError,
    BackendError,
    BackendRateLimitError,
    BackendTransportError,
    CheckpointError,
    CompilationError,
    GraphError,
    InterruptError,
    NodeError,
    RunTimeout,
)
from agentflow.events import (
    Allow,
    ChatRequest,
    Deny,
    ErrorEvent,
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
from agentflow.telemetry import JsonlTelemetry, MultiHooks

__version__ = "0.1.0"

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
    "Allow",
    "Deny",
    # checkpointing
    "Checkpoint",
    "Checkpointer",
    "MemoryCheckpointer",
    "FileCheckpointer",
    "SqliteCheckpointer",
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
    "InterruptError",
]
