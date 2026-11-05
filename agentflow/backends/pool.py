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

"""A pool of agent backends, leased per step and reused where the backend allows.

Starting an agent CLI costs seconds (the process, its MCP servers, auth). What can be
shared differs by backend, so the pool asks a small *driver* per kind:

- ``kiro`` (engine v3): the agent is a session mode, so ONE process serves every agent
  of a working directory; a lease switches agent with ``use_agent``.
- ``claude_code``: a persistent ``claude`` process per (directory, agent, model,
  permissions) -- those are command-line flags, fixed for the process -- with a
  ``/clear`` between leases.
- ``codex``: one process per turn, nothing to keep alive, so every lease is a fresh
  backend and is closed afterwards (``reusable = False``). It still goes through the
  pool so the engine has a single way to get a backend.

Each lease gets a fresh conversation and the permission policy of its own step. A
backend whose lease raised, or that reports ``reusable = False`` afterwards, is
closed instead of returned. At most ``max_size`` leases are active at once.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Hashable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from agentflow.backends.base import PermissionPolicy, policy_intent
from agentflow.errors import BackendError


@dataclass(frozen=True, slots=True)
class BackendSpec:
    """What a step needs from a backend."""

    kind: str  # "kiro" | "claude_code" | "codex"
    cwd: Path | str
    permission: PermissionPolicy
    agent: str | None = None
    model: str | None = None


class _Driver(Protocol):
    def key(self, spec: BackendSpec) -> Hashable:
        """Backends with the same key are interchangeable after :meth:`rebind`."""

    def create(self, spec: BackendSpec) -> Any: ...

    async def rebind(self, backend: Any, spec: BackendSpec) -> None:
        """Make an idle backend ready for ``spec``: fresh conversation, right agent."""


def _cwd(spec: BackendSpec) -> str:
    return str(Path(spec.cwd).resolve())


class _KiroDriver:
    def __init__(self, engine: str) -> None:
        self.engine = engine
        # Engine v2 fixes the agent at process start: nothing to switch, so no reuse.
        self.reusable = engine == "v3"

    def key(self, spec: BackendSpec) -> Hashable:
        # v3: the agent is a session mode, so it is not part of the key. v2 fixes it
        # at process start (and such backends are never reused anyway).
        return ("kiro", self.engine, _cwd(spec), None if self.engine == "v3" else spec.agent)

    def create(self, spec: BackendSpec) -> Any:
        from agentflow.backends.kiro import KiroBackend

        return KiroBackend(
            spec.agent or "", engine=self.engine, cwd=spec.cwd, permission=spec.permission
        )

    async def rebind(self, backend: Any, spec: BackendSpec) -> None:
        await backend.use_agent(spec.agent or "")
        backend.permission = spec.permission


def _permission_signature(policy: PermissionPolicy) -> Hashable:
    intent = policy_intent(policy)
    allow = tuple(sorted(intent.allowlist)) if intent.allowlist is not None else None
    return (intent.deny_all, intent.allow_all, allow)


class _ClaudeDriver:
    def key(self, spec: BackendSpec) -> Hashable:
        return (
            "claude_code",
            _cwd(spec),
            spec.agent,
            spec.model,
            _permission_signature(spec.permission),
        )

    def create(self, spec: BackendSpec) -> Any:
        from agentflow.backends.claude_code import PersistentClaudeBackend

        return PersistentClaudeBackend(
            agent=spec.agent, model=spec.model, cwd=spec.cwd, permission=spec.permission
        )

    async def rebind(self, backend: Any, spec: BackendSpec) -> None:
        await backend.reset_session()
        backend.permission = spec.permission


class _OneShotDriver:
    """A backend that is a process per turn: nothing to keep, never reused."""

    reusable = False

    def __init__(self, kind: str) -> None:
        self.kind = kind

    def key(self, spec: BackendSpec) -> Hashable:
        return (self.kind, object())  # never matches an idle backend

    def create(self, spec: BackendSpec) -> Any:
        if self.kind == "codex":
            from agentflow.backends.codex import CodexBackend

            return CodexBackend(cwd=spec.cwd, permission=spec.permission)
        raise BackendError(f"the pool has no driver for backend {self.kind!r}")

    async def rebind(self, backend: Any, spec: BackendSpec) -> None:  # pragma: no cover
        return None


class BackendPool:
    """Reuses agent backends across leases. Close it with :meth:`aclose` (or ``async with``)."""

    def __init__(self, *, max_size: int = 4, kiro_engine: str = "v3") -> None:
        if max_size < 1:
            raise ValueError("max_size must be at least 1")
        self.max_size = max_size
        self._drivers: dict[str, _Driver] = {
            "kiro": _KiroDriver(kiro_engine),
            "claude_code": _ClaudeDriver(),
            "codex": _OneShotDriver("codex"),
        }
        self._idle: dict[Hashable, list[Any]] = {}
        self._slots = asyncio.Semaphore(max_size)
        self._closed = False

    async def __aenter__(self) -> BackendPool:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    def supports(self, kind: str) -> bool:
        return kind in self._drivers

    @contextlib.asynccontextmanager
    async def lease(self, spec: BackendSpec) -> AsyncIterator[Any]:
        """Borrow a started backend for ``spec``; wait if ``max_size`` leases are active."""
        if self._closed:
            raise BackendError("the backend pool is closed")
        driver = self._drivers.get(spec.kind)
        if driver is None:
            raise BackendError(f"the pool has no driver for backend {spec.kind!r}")
        key = driver.key(spec)
        async with self._slots:
            idle = self._idle.get(key)
            backend = idle.pop() if idle else None
            ok = False
            try:
                if backend is None:
                    backend = driver.create(spec)
                    await backend.start()
                else:
                    await driver.rebind(backend, spec)
                yield backend
                ok = True
            finally:
                if backend is not None:
                    keep = ok and not self._closed and getattr(backend, "reusable", True)
                    if keep and getattr(driver, "reusable", True):
                        self._idle.setdefault(key, []).append(backend)
                    else:
                        await self._discard(backend)

    @staticmethod
    async def _discard(backend: Any) -> None:
        with contextlib.suppress(Exception):
            await backend.close()

    async def aclose(self) -> None:
        """Close every idle backend. Leases still active close theirs when they end."""
        self._closed = True
        idle, self._idle = self._idle, {}
        for backends in idle.values():
            for backend in backends:
                await self._discard(backend)


class KiroPool(BackendPool):
    """:class:`BackendPool` for Kiro only, with the original ``lease(agent, ...)`` signature."""

    def __init__(self, *, engine: str = "v3", max_size: int = 4) -> None:
        super().__init__(max_size=max_size, kiro_engine=engine)
        self.engine = engine

    def lease(  # type: ignore[override]
        self, agent: str, *, cwd: Path | str, permission: PermissionPolicy
    ) -> contextlib.AbstractAsyncContextManager[Any]:
        return super().lease(BackendSpec("kiro", cwd, permission, agent=agent))
