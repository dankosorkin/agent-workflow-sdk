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
    """Escalate a permission request to a human via an engine interrupt.

    When resolved inside a running graph, this calls ``ctx.interrupt(req)`` on
    the node's :class:`~agentflow.runtime.Context`, which suspends the run and
    persists an interrupted checkpoint. The human inspects the request and
    calls ``app.resume(thread, value=...)``; the resumed value becomes this
    policy's :class:`PermissionDecision`. A plain :class:`Allow`/:class:`Deny`
    or a bare truthy/``None`` value is accepted and normalized.

    The node's context is found via :data:`agentflow.runtime.current_context`,
    which the runtime sets around each node call. Because a node and any
    backend generator it iterates share one task, the policy — invoked while
    the node consumes the backend's event stream — sees the right context.
    Outside a graph run (no current context) it falls back to raising
    :class:`~agentflow.errors.InterruptError` so misuse fails loudly.
    """

    async def decide(self, req: PermissionRequest) -> PermissionDecision:
        # Imported lazily to keep the backend layer importable without the
        # engine, and to avoid any import-order sensitivity.
        from agentflow.runtime import current_context

        ctx = current_context.get()
        if ctx is None:
            raise InterruptError("<permission>", req)

        answer = await ctx.interrupt(req)
        return _normalize_decision(answer)


def _normalize_decision(answer: Any) -> PermissionDecision:
    """Coerce a human's resume value into a PermissionDecision."""
    if isinstance(answer, (Allow, Deny)):
        return answer
    if isinstance(answer, str):
        low = answer.strip().lower()
        if low in ("allow", "yes", "y", "approve", "ok"):
            return Allow()
        if low in ("deny", "no", "n", "reject"):
            return Deny(reason=answer)
        return Allow(option_id=answer)  # treat as a specific option id
    if answer is None or answer is False:
        return Deny()
    return Allow()
