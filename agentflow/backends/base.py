"""Backend protocols and permission policies.

A *backend* drives one underlying engine — an agent process (Kiro, Codex,
Claude Code) or an LLM endpoint (Ollama, OpenAI-compatible). The graph engine
depends only on :class:`Backend`; user code that needs agent- or LLM-specific
features constructs the concrete subtype and gets the narrowed method.

This module imports only :mod:`agentflow.events` and
:mod:`agentflow.errors` — never the graph engine. Backends are a leaf layer.
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Protocol, runtime_checkable

from agentflow.errors import InterruptError
from agentflow.events import (
    Allow,
    BackendEvent,
    BackendRequest,
    ChatRequest,
    Deny,
    Message,
    PermissionDecision,
    PermissionRequest,
    TextRequest,
    ToolSpec,
)

__all__ = [
    "Backend",
    "AgentBackend",
    "LLMBackend",
    "PermissionPolicy",
    "AllowAll",
    "DenyAll",
    "Interactive",
]


# ---------------------------------------------------------------------------
# Backend protocols
# ---------------------------------------------------------------------------

@runtime_checkable
class Backend(Protocol):
    """Common surface for every backend.

    ``invoke`` is an async generator: it yields :class:`BackendEvent`s as they
    arrive and completes when the turn ends. A caller wanting only the final
    result drains it and keeps the terminal :class:`~agentflow.events.TurnEnd`;
    a streaming caller consumes events as they come.
    """

    async def start(self) -> None:
        """Bring the backend up (spawn a process, open a client)."""
        ...

    async def close(self) -> None:
        """Tear the backend down. Idempotent."""
        ...

    def invoke(
        self, request: BackendRequest, *, session: str | None = None
    ) -> AsyncIterator[BackendEvent]:
        """Run one turn for ``request``, yielding events until it ends."""
        ...


@runtime_checkable
class AgentBackend(Backend, Protocol):
    """A session-based agent that runs its own tools and asks permission.

    Accepts a :class:`~agentflow.events.TextRequest`. Exposes a
    :class:`PermissionPolicy` used to answer the agent's permission prompts.
    """

    permission: "PermissionPolicy"

    def prompt(
        self, text: str, *, session: str | None = None
    ) -> AsyncIterator[BackendEvent]:
        """Convenience wrapper over ``invoke(TextRequest(text))``."""
        ...


@runtime_checkable
class LLMBackend(Backend, Protocol):
    """A stateless chat model: messages in, token stream out.

    Accepts a :class:`~agentflow.events.ChatRequest`. It never executes tools
    itself — a :class:`~agentflow.events.ToolCall` it emits is a request the
    graph fulfils and feeds back on the next call.
    """

    def chat(
        self,
        messages: list[Message],
        *,
        tools: list[ToolSpec] | None = None,
        options: dict[str, Any] | None = None,
    ) -> AsyncIterator[BackendEvent]:
        """Convenience wrapper over ``invoke(ChatRequest(...))``."""
        ...


# ---------------------------------------------------------------------------
# Base classes with the convenience wrappers implemented once
# ---------------------------------------------------------------------------

class BaseAgentBackend:
    """Mixin implementing :meth:`prompt` on top of :meth:`invoke`.

    Concrete agent backends inherit this and implement ``start``, ``close``,
    and ``invoke``. ``permission`` defaults to :class:`AllowAll`, reproducing
    the previous auto-approve behavior.
    """

    permission: "PermissionPolicy"

    def __init__(self, permission: "PermissionPolicy | None" = None) -> None:
        self.permission = permission or AllowAll()

    def prompt(
        self, text: str, *, session: str | None = None
    ) -> AsyncIterator[BackendEvent]:
        return self.invoke(TextRequest(text), session=session)  # type: ignore[attr-defined]


class BaseLLMBackend:
    """Mixin implementing :meth:`chat` on top of :meth:`invoke`.

    Concrete LLM backends inherit this and implement ``start``, ``close``,
    and ``invoke``.
    """

    def chat(
        self,
        messages: list[Message],
        *,
        tools: list[ToolSpec] | None = None,
        options: dict[str, Any] | None = None,
    ) -> AsyncIterator[BackendEvent]:
        request = ChatRequest(
            messages=tuple(messages),
            tools=tuple(tools or ()),
            options=dict(options or {}),
        )
        return self.invoke(request)  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Permission policies (agent backends only)
# ---------------------------------------------------------------------------

@runtime_checkable
class PermissionPolicy(Protocol):
    """Decides how to answer an agent's permission request."""

    async def decide(self, req: PermissionRequest) -> PermissionDecision:
        ...


class AllowAll:
    """Approve every request. The default — reproduces auto-approve."""

    async def decide(self, req: PermissionRequest) -> PermissionDecision:
        return Allow()


class DenyAll:
    """Reject every request."""

    async def decide(self, req: PermissionRequest) -> PermissionDecision:
        return Deny(reason="denied by policy")


class Interactive:
    """Escalate to a human via an engine interrupt.

    Raises :class:`~agentflow.errors.InterruptError` carrying the request as
    its payload. The runtime catches it, persists an interrupted checkpoint,
    and suspends the run; on ``resume(value=...)`` the value is fed back as
    the :class:`PermissionDecision`. Outside a graph run there is nothing to
    catch the interrupt, so this policy is only meaningful inside the engine.
    """

    def __init__(self, node: str = "<permission>") -> None:
        self._node = node

    async def decide(self, req: PermissionRequest) -> PermissionDecision:
        raise InterruptError(self._node, req)
