"""AgentFlow — async-first, LangGraph-style agent workflow SDK.

Importing this package pulls in only the dependency-free core (state, graph,
runtime, checkpointing). Backends live under :mod:`agentflow.backends` and are
imported explicitly so the core never drags in a subprocess or HTTP library.
"""

from __future__ import annotations

from agentflow.state import (
    State,
    Reducer,
    Channel,
    last,
    append,
    add,
    merge,
    union,
)
from agentflow.graph import Graph, START, END, Node, Router
from agentflow.compiled import CompiledGraph
from agentflow.runtime import Context, StreamEvent
from agentflow.events import (
    Message,
    ToolSpec,
    ToolCallSpec,
    TextRequest,
    ChatRequest,
    TextChunk,
    ToolCall,
    ToolResult,
    PermissionRequest,
    PermissionOption,
    TurnEnd,
    ErrorEvent,
    Allow,
    Deny,
)
from agentflow.checkpoint import (
    Checkpoint,
    Checkpointer,
    MemoryCheckpointer,
    FileCheckpointer,
)
from agentflow.errors import (
    AgentFlowError,
    GraphError,
    CompilationError,
    NodeError,
    BackendError,
    BackendTransportError,
    CheckpointError,
    InterruptError,
)

__version__ = "0.1.0"

__all__ = [
    "__version__",
    # state
    "State", "Reducer", "Channel", "last", "append", "add", "merge", "union",
    # graph
    "Graph", "START", "END", "Node", "Router", "CompiledGraph",
    "Context", "StreamEvent",
    # events
    "Message", "ToolSpec", "ToolCallSpec",
    "TextRequest", "ChatRequest",
    "TextChunk", "ToolCall", "ToolResult",
    "PermissionRequest", "PermissionOption", "TurnEnd", "ErrorEvent",
    "Allow", "Deny",
    # checkpointing
    "Checkpoint", "Checkpointer", "MemoryCheckpointer", "FileCheckpointer",
    # errors
    "AgentFlowError", "GraphError", "CompilationError", "NodeError",
    "BackendError", "BackendTransportError", "CheckpointError", "InterruptError",
]
