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

"""A pool of long-lived Kiro processes, reused across steps and agents.

Starting ``kiro-cli`` costs over a second and re-spawns the MCP servers of the
agent it runs. On engine v3 the agent is a session mode, so one process can serve
many agents in turn (:meth:`KiroBackend.use_agent`). :class:`KiroPool` keeps idle
processes per working directory and hands one out for the length of a ``lease``.

Each lease gets a fresh session (no memory of the previous lease) and the
permission policy of its own step. A backend that raised inside a lease is closed,
never reused. Engine v2 cannot switch agents, so every lease there starts a process
and closes it again: correct, just not faster.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from pathlib import Path

from agentflow.backends.base import PermissionPolicy
from agentflow.backends.kiro import KiroBackend
from agentflow.errors import BackendError


class KiroPool:
    """Reuses Kiro processes across leases. Close it with :meth:`aclose` (or ``async with``)."""

    def __init__(self, *, engine: str = "v3", max_size: int = 4) -> None:
        if max_size < 1:
            raise ValueError("max_size must be at least 1")
        self.engine = engine
        self.max_size = max_size
        self._idle: dict[str, list[KiroBackend]] = {}
        self._slots = asyncio.Semaphore(max_size)
        self._closed = False

    async def __aenter__(self) -> KiroPool:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    @contextlib.asynccontextmanager
    async def lease(
        self, agent: str, *, cwd: Path | str, permission: PermissionPolicy
    ) -> AsyncIterator[KiroBackend]:
        """Borrow a started backend running ``agent`` in ``cwd`` with ``permission``.

        At most ``max_size`` leases are active at once; further ones wait.
        """
        if self._closed:
            raise BackendError("the Kiro pool is closed")
        key = str(Path(cwd).resolve())
        async with self._slots:
            backend = self._idle.get(key, []).pop() if self._idle.get(key) else None
            ok = False
            try:
                if backend is None:
                    backend = KiroBackend(
                        agent, engine=self.engine, cwd=cwd, permission=permission
                    )
                    await backend.start()
                else:
                    await backend.use_agent(agent)
                backend.permission = permission
                yield backend
                ok = True
            finally:
                if backend is not None:
                    if ok and self.engine == "v3" and not self._closed:
                        self._idle.setdefault(key, []).append(backend)
                    else:
                        await self._discard(backend)

    @staticmethod
    async def _discard(backend: KiroBackend) -> None:
        with contextlib.suppress(Exception):
            await backend.close()

    async def aclose(self) -> None:
        """Close every idle process. Leases still active close theirs when they end."""
        self._closed = True
        idle, self._idle = self._idle, {}
        for backends in idle.values():
            for backend in backends:
                await self._discard(backend)
