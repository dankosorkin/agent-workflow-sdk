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

"""Structured events, messages, and requests shared across backends.

Everything a backend emits is a typed, frozen dataclass. There is exactly
one vocabulary for both agent backends (Kiro/Codex/Claude) and LLM backends
(Ollama and OpenAI-compatible endpoints), so a node can move a tool call from
one to a message for the other without adapters.

Nothing here is async and nothing imports the engine — these are pure data
types.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

__all__ = [
    # chat vocabulary
    "Role",
    "ToolCallSpec",
    "Message",
    "ToolSpec",
    # requests
    "TextRequest",
    "ChatRequest",
    "BackendRequest",
    # events
    "TextChunk",
    "ToolCall",
    "ToolResult",
    "PermissionOption",
    "PermissionRequest",
    "TurnEnd",
    "ErrorEvent",
    "BackendEvent",
    # permission decisions
    "PermissionDecision",
    "Allow",
    "Deny",
]

Role = Literal["system", "user", "assistant", "tool"]


# ---------------------------------------------------------------------------
# Chat vocabulary
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ToolCallSpec:
    """A single tool call an assistant message requested."""

    id: str
    name: str
    args: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Message:
    """A neutral chat message.

    Used as the input unit for :class:`LLMBackend` and to represent turns in
    an agent's history. ``tool_calls`` is set on an assistant message that
    requested tools; ``tool_call_id`` links a ``role="tool"`` result back to
    the call that produced it.
    """

    role: Role
    content: str = ""
    tool_calls: tuple[ToolCallSpec, ...] = ()
    tool_call_id: str | None = None
    name: str | None = None


@dataclass(frozen=True, slots=True)
class ToolSpec:
    """Describes a tool offered to an LLM (JSON-Schema parameters)."""

    name: str
    description: str = ""
    schema: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Requests — the input side of Backend.invoke
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TextRequest:
    """Prompt an agent backend with free text."""

    text: str


@dataclass(frozen=True, slots=True)
class ChatRequest:
    """Call an LLM backend with a message list and optional tool schemas."""

    messages: tuple[Message, ...]
    tools: tuple[ToolSpec, ...] = ()
    options: dict[str, Any] = field(default_factory=dict)


BackendRequest = TextRequest | ChatRequest


# ---------------------------------------------------------------------------
# Events — the output side of Backend.invoke
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TextChunk:
    """A streamed delta of the assistant's message text."""

    text: str


@dataclass(frozen=True, slots=True)
class ToolCall:
    """The backend/model invoked a tool. For an LLM backend this is a
    *request* the graph must fulfil; for an agent backend it is informational
    (the agent runs the tool itself)."""

    id: str
    name: str
    args: dict[str, Any] = field(default_factory=dict)
    title: str | None = None


@dataclass(frozen=True, slots=True)
class ToolResult:
    """A tool call resolved (emitted by agent backends that run their own
    tools)."""

    id: str
    status: Literal["ok", "error", "pending"] = "ok"
    content: Any = None


@dataclass(frozen=True, slots=True)
class PermissionOption:
    """One choice a backend offers for a permission request."""

    option_id: str
    name: str
    kind: str  # e.g. "allow_always", "allow_once", "reject_once"


@dataclass(frozen=True, slots=True)
class PermissionRequest:
    """An agent backend asks the client to authorize a tool use.

    Resolved by the backend's :class:`PermissionPolicy`; it is not normally
    yielded to node code (agent backends handle it inline). It is a public
    type so an ``Interactive`` policy can surface it to a human.
    """

    id: str
    tool: str
    options: tuple[PermissionOption, ...] = ()
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TurnEnd:
    """Terminal event of a turn. Carries the fully assembled assistant text
    and the backend's stop reason (e.g. ``end_turn``, ``max_tokens``)."""

    text: str = ""
    stop_reason: str | None = None
    message: Message | None = None


@dataclass(frozen=True, slots=True)
class ErrorEvent:
    """A recoverable error surfaced as data (not raised). A node may branch
    on it. Fatal transport failures raise
    :class:`~agentflow.errors.BackendTransportError` instead."""

    message: str
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class GateEvent:
    """A quality-gate lifecycle event, surfaced via ``ctx.emit`` so telemetry
    and the run stream show where a process gated on quality.

    ``phase`` is one of ``"started"``, ``"passed"``, ``"failed"``, or
    ``"escalated"``. ``gate`` names the gate; ``attempt`` is the 1-based try
    count; ``score`` and ``reasons`` mirror the evaluator's
    :class:`~agentflow.prebuilt.GateResult`. This is an observability event —
    it is emitted, never returned as a node update. See the
    :doc:`quality gates guide </durability/quality-gates>`.
    """

    gate: str
    phase: str  # "started" | "passed" | "failed" | "escalated"
    attempt: int = 0
    score: float | None = None
    reasons: tuple[str, ...] = ()


BackendEvent = TextChunk | ToolCall | ToolResult | PermissionRequest | TurnEnd | ErrorEvent

#: Everything a node may pass to ``ctx.emit``: the backend vocabulary plus the
#: run-level :class:`GateEvent`. Kept as a separate alias so the backend event
#: union stays exactly the set a backend produces.
EmittableEvent = BackendEvent | GateEvent


# ---------------------------------------------------------------------------
# Permission decisions — the output of a PermissionPolicy
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Allow:
    """Approve a permission request. ``option_id`` selects a specific offered
    option; when ``None`` the backend picks the most permissive allow option
    it was given."""

    option_id: str | None = None
    remember: bool = False


@dataclass(frozen=True, slots=True)
class Deny:
    """Reject a permission request."""

    reason: str | None = None


PermissionDecision = Allow | Deny
