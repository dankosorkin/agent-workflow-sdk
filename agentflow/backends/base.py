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

"""Backend protocols and permission policies.

A *backend* drives one underlying engine — an agent process (Kiro, Codex,
Claude Code) or an LLM endpoint (Ollama, OpenAI-compatible). The graph engine
depends only on :class:`Backend`; user code that needs agent- or LLM-specific
features constructs the concrete subtype and gets the narrowed method.

This module imports only :mod:`agentflow.events` and
:mod:`agentflow.errors` — never the graph engine. Backends are a leaf layer.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

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
    "Callback",
    "ToolAllowlist",
    "PolicyIntent",
    "policy_intent",
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

    permission: PermissionPolicy

    def prompt(self, text: str, *, session: str | None = None) -> AsyncIterator[BackendEvent]:
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


class _AsyncResource:
    """Async context-manager support so ``start``/``close`` can't be skipped.

        async with KiroBackend("vibe", permission=AllowAll()) as agent:
            async for ev in agent.prompt("..."):
                ...
        # close() is guaranteed, even on error.

    Concrete backends implement ``start`` and ``close``; this mixin wires them
    to ``__aenter__``/``__aexit__``.
    """

    async def start(self) -> None:  # pragma: no cover - overridden
        ...

    async def close(self) -> None:  # pragma: no cover - overridden
        ...

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


class BaseAgentBackend(_AsyncResource):
    """Mixin implementing :meth:`prompt` on top of :meth:`invoke`.

    Concrete agent backends inherit this and implement ``start``, ``close``,
    and ``invoke``.

    A ``permission`` policy is REQUIRED — there is no default. Auto-approving
    an agent's tool requests is a security decision the caller must make
    explicitly. Pass :class:`AllowAll` for a trusted local sandbox,
    :class:`DenyAll` to block tools, or :class:`Interactive` for
    human-in-the-loop approval.

    NOTE on coverage: only agent backends that actually receive permission
    requests route them through this policy. Kiro (persistent ACP session)
    does. The one-shot CLI agents (Codex, Claude Code) manage their own
    permission model via CLI flags/sandbox and do NOT route prompts through
    this policy — the field still gates whether the SDK would auto-approve if
    a request surfaced, but the CLI's own controls apply first. See the
    capability matrix in the README.
    """

    permission: PermissionPolicy

    def __init__(self, permission: PermissionPolicy) -> None:
        if permission is None:
            raise TypeError(
                "an agent backend requires an explicit permission policy "
                "(no default). Pass AllowAll() for a trusted local sandbox, "
                "DenyAll(), or Interactive() for human-in-the-loop."
            )
        self.permission = permission

    def prompt(self, text: str, *, session: str | None = None) -> AsyncIterator[BackendEvent]:
        return self.invoke(TextRequest(text), session=session)  # type: ignore[attr-defined]


class BaseLLMBackend(_AsyncResource):
    """Mixin implementing :meth:`chat` on top of :meth:`invoke`.

    Concrete LLM backends inherit this and implement ``start``, ``close``,
    and ``invoke``.

    Set ``max_concurrency`` (an int) to cap in-flight requests from this
    backend instance; :meth:`concurrency_guard` returns an async context to
    wrap the request with. This bounds pressure when many graph nodes hit the
    same provider at once.
    """

    max_concurrency: int | None = None

    def concurrency_guard(self):
        """Return an async context manager limiting concurrent requests.

        A no-op when ``max_concurrency`` is unset; otherwise a lazily-created
        :class:`asyncio.Semaphore` shared across this backend instance.
        """
        import asyncio
        import contextlib

        if not self.max_concurrency:
            return contextlib.nullcontext()
        sem = getattr(self, "_sem", None)
        if sem is None:
            sem = asyncio.Semaphore(self.max_concurrency)
            self._sem = sem
        return sem

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

    async def decide(self, req: PermissionRequest) -> PermissionDecision: ...


class AllowAll:
    """Approve every request. Auto-approve — only for a trusted local sandbox."""

    async def decide(self, req: PermissionRequest) -> PermissionDecision:
        return Allow()


class DenyAll:
    """Reject every request."""

    async def decide(self, req: PermissionRequest) -> PermissionDecision:
        return Deny(reason="denied by policy")


class ToolAllowlist:
    """Approve only requests whose tool is on the allowlist; deny the rest.

    A pragmatic least-privilege default: name the tools an agent may use and
    everything else is refused. Matching is exact on the request's ``tool``.

        policy = ToolAllowlist({"read_file", "list_directory"})

    ``fallback`` decides unlisted tools (default :class:`DenyAll`); pass
    :class:`Interactive` to ask a human for anything not pre-approved.
    """

    def __init__(self, allowed, *, fallback: PermissionPolicy | None = None) -> None:
        self.allowed = set(allowed)
        self.fallback = fallback or DenyAll()

    async def decide(self, req: PermissionRequest) -> PermissionDecision:
        if req.tool in self.allowed:
            return Allow()
        return await self.fallback.decide(req)


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


class Callback:
    """Ask a human (or any resolver) inline, without interrupting the graph.

    Unlike :class:`Interactive` — which suspends the whole run via
    ``ctx.interrupt`` and requires a separate ``resume()`` — this awaits a
    caller-supplied async function *within the same turn* and answers the
    agent's permission request immediately. That matters for a persistent
    session backend (Kiro's ACP): the agent is holding one turn open waiting
    for the decision, so the answer must go back in that turn rather than
    abandoning it and starting a new one.

    ``resolver`` is ``async (PermissionRequest) -> decision`` where the
    decision is an :class:`Allow`/:class:`Deny`, or a value normalized the same
    way as a resume value (``"y"``/``"n"``, ``None``, an option id, ...).

        async def ask(req):
            answer = await asyncio.to_thread(input, f"allow {req.tool}? [y/N] ")
            return answer

        backend = KiroBackend("vibe", permission=Callback(ask))
    """

    def __init__(self, resolver: Callable[[PermissionRequest], Awaitable[Any]]) -> None:
        self._resolver = resolver

    async def decide(self, req: PermissionRequest) -> PermissionDecision:
        answer = await self._resolver(req)
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


@dataclass(frozen=True, slots=True)
class PolicyIntent:
    """A backend-agnostic reading of a permission policy, for CLI backends.

    The one-shot CLI agents (Codex, Claude Code) do not surface per-tool
    permission requests we can gate at runtime — they enforce permissions via
    their own launch flags. This distils a :class:`PermissionPolicy` into an
    upfront intent those backends can translate to flags:

    - ``deny_all``: the policy denies everything (``DenyAll``, or an empty
      ``ToolAllowlist`` whose fallback denies) — launch as restrictively as the
      CLI allows.
    - ``allow_all``: the policy approves everything (``AllowAll``).
    - ``allowlist``: the explicit set of permitted tools (``ToolAllowlist``),
      or ``None`` when the policy is not an allowlist.

    An ``Interactive``/``Callback`` policy has no static answer (it needs a
    live request), so it reads as neither allow_all nor deny_all — the CLI's
    own default applies. This is a best-effort mapping, not our runtime gate;
    the CLI's controls are authoritative.
    """

    deny_all: bool = False
    allow_all: bool = False
    allowlist: frozenset[str] | None = None


def policy_intent(policy: PermissionPolicy) -> PolicyIntent:
    """Best-effort static reading of ``policy`` for CLI-flag translation."""
    if isinstance(policy, AllowAll):
        return PolicyIntent(allow_all=True)
    if isinstance(policy, DenyAll):
        return PolicyIntent(deny_all=True)
    if isinstance(policy, ToolAllowlist):
        allowed = frozenset(policy.allowed)
        # An empty allowlist with a denying fallback means "deny everything".
        if not allowed and isinstance(policy.fallback, DenyAll):
            return PolicyIntent(deny_all=True, allowlist=allowed)
        return PolicyIntent(allowlist=allowed)
    # Interactive / Callback / custom: no static answer.
    return PolicyIntent()
