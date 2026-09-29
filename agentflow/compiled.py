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

"""CompiledGraph: the immutable, async-runnable form of a graph.

Produced by :meth:`agentflow.graph.Graph.compile`. Wraps the validated
structure and drives it through :mod:`agentflow.runtime`, exposing:

- :meth:`invoke` — run to completion, return the final state.
- :meth:`stream` — async-iterate observable events as the run progresses.
- :meth:`resume` — continue a run suspended by a human-in-the-loop interrupt.
- :meth:`get_state` / :meth:`history` — inspect checkpoints (time-travel).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from typing import Any

from agentflow.checkpoint.base import Checkpoint
from agentflow.errors import CheckpointError, GraphError, RunTimeout
from agentflow.runtime import StreamEvent, _Graph, run
from agentflow.state import RESERVED_NAMES, RESERVED_PREFIX, Channel

__all__ = ["CompiledGraph"]


class CompiledGraph:
    def __init__(
        self,
        *,
        schema: type,
        channels: Mapping[str, Channel],
        nodes: Mapping[str, Any],
        edges: Mapping[Any, list[Any]],
        branches: Mapping[str, Any],
        checkpointer: Any = None,
        step_limit: int = 100,
        hooks: Any = None,
        max_node_concurrency: int | None = None,
        isolate_state: str = "fanout",
    ) -> None:
        from agentflow.observability import Hooks

        self.schema = schema
        self.checkpointer = checkpointer
        self.hooks = hooks or Hooks()
        self._graph = _Graph(
            channels=channels,
            nodes=nodes,
            edges=edges,
            branches=branches,
            step_limit=step_limit,
            max_node_concurrency=max_node_concurrency,
            isolate_state=isolate_state,
        )

    # ------------------------------------------------------------------
    # Running
    # ------------------------------------------------------------------

    async def invoke(
        self,
        input: Mapping[str, Any] | None = None,
        *,
        thread: str = "default",
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Run from ``input`` to completion and return the final state.

        If the run suspends on an interrupt, the interrupted checkpoint's
        state is returned; inspect ``get_state(thread)`` to see it is not done
        and call :meth:`resume`.

        ``timeout`` bounds the whole run. On expiry the in-flight super-step is
        cancelled and :class:`~agentflow.errors.RunTimeout` is raised; the last
        completed step's checkpoint is intact, so a checkpointed run resumes
        from there.
        """
        data = self._validate_input(input)
        coro = run(
            self._graph,
            data,
            thread=thread,
            start_frontier=None,
            start_step=0,
            checkpointer=self.checkpointer,
            hooks=self.hooks,
        )
        cp = await self._run_with_timeout(coro, thread, timeout)
        return dict(cp.state)

    async def stream(
        self, input: Mapping[str, Any] | None = None, *, thread: str = "default"
    ) -> AsyncIterator[StreamEvent]:
        """Run from ``input``, yielding :class:`StreamEvent`s as they occur."""
        async for ev in self._stream_run(
            thread=thread,
            initial_state=self._validate_input(input),
            start_frontier=None,
            start_step=0,
        ):
            yield ev

    # ------------------------------------------------------------------
    # Resume (human-in-the-loop)
    # ------------------------------------------------------------------

    async def resume(
        self, thread: str, value: Any = None, *, timeout: float | None = None
    ) -> dict[str, Any]:
        """Continue a suspended run, feeding ``value`` to the interrupted node."""
        cp = await self._load_for_resume(thread)
        resume_values = {cp.interrupt_node: value} if cp.interrupt_node else {}
        coro = run(
            self._graph,
            dict(cp.state),
            thread=thread,
            start_frontier=list(cp.next),
            start_step=cp.step,
            checkpointer=self.checkpointer,
            resume_values=resume_values,
            hooks=self.hooks,
        )
        result = await self._run_with_timeout(coro, thread, timeout)
        return dict(result.state)

    async def stream_resume(self, thread: str, value: Any = None) -> AsyncIterator[StreamEvent]:
        """Streaming form of :meth:`resume`."""
        cp = await self._load_for_resume(thread)
        resume_values = {cp.interrupt_node: value} if cp.interrupt_node else {}
        async for ev in self._stream_run(
            thread=thread,
            initial_state=dict(cp.state),
            start_frontier=list(cp.next),
            start_step=cp.step,
            resume_values=resume_values,
        ):
            yield ev

    # ------------------------------------------------------------------
    # Synchronous facade (convenience for non-async callers)
    # ------------------------------------------------------------------

    def invoke_sync(
        self,
        input: Mapping[str, Any] | None = None,
        *,
        thread: str = "default",
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Blocking wrapper over :meth:`invoke` via ``asyncio.run``.

        For synchronous callers only. Raises :class:`RuntimeError` if called
        from within a running event loop — use ``await invoke(...)`` there.
        """
        return _run_sync(self.invoke(input, thread=thread, timeout=timeout))

    def resume_sync(
        self, thread: str, value: Any = None, *, timeout: float | None = None
    ) -> dict[str, Any]:
        """Blocking wrapper over :meth:`resume`."""
        return _run_sync(self.resume(thread, value, timeout=timeout))

    def stream_sync(
        self, input: Mapping[str, Any] | None = None, *, thread: str = "default"
    ) -> list[StreamEvent]:
        """Blocking wrapper that drains :meth:`stream` into a list.

        Returns all events at once (not incremental) — a convenience for sync
        scripts; use the async ``stream`` for live consumption.
        """

        async def _collect() -> list[StreamEvent]:
            return [ev async for ev in self.stream(input, thread=thread)]

        return _run_sync(_collect())

    # ------------------------------------------------------------------
    # Inspection / time-travel
    # ------------------------------------------------------------------

    async def get_state(self, thread: str, step: int | None = None) -> Checkpoint | None:
        if self.checkpointer is None:
            raise CheckpointError("no checkpointer configured")
        return await self.checkpointer.get(thread, step)

    def history(self, thread: str) -> AsyncIterator[Checkpoint]:
        if self.checkpointer is None:
            raise CheckpointError("no checkpointer configured")
        return self.checkpointer.history(thread)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _validate_input(self, input: Mapping[str, Any] | None) -> dict[str, Any]:
        """Reject input keys that are not declared channels, before running.

        A typo in an input key would otherwise sit silently in the state until
        a node happened to read it. Fail fast with a clear message instead.
        """
        data = dict(input or {})
        if not data:
            return data
        declared = set(self._graph.channels)
        unknown = [
            k
            for k in data
            if k not in declared or k.startswith(RESERVED_PREFIX) or k in RESERVED_NAMES
        ]
        if unknown:
            raise GraphError(
                f"input has undeclared channel(s) {sorted(unknown)}; "
                f"declared channels: {sorted(declared)}"
            )
        return data

    async def _run_with_timeout(self, coro, thread: str, timeout: float | None):
        if timeout is None:
            return await coro
        try:
            return await asyncio.wait_for(coro, timeout=timeout)
        except TimeoutError:
            # The in-flight super-step was cancelled; the last completed step
            # is already checkpointed. Report the step reached, if known.
            step = 0
            if self.checkpointer is not None:
                last = await self.checkpointer.get(thread)
                if last is not None:
                    step = last.step
            raise RunTimeout(thread, timeout, step) from None

    async def _load_for_resume(self, thread: str) -> Checkpoint:
        if self.checkpointer is None:
            raise CheckpointError("resume requires a checkpointer")
        cp = await self.checkpointer.get(thread)
        if cp is None:
            raise GraphError(f"no checkpoint for thread {thread!r}")
        if cp.done:
            raise GraphError(f"run {thread!r} already finished; nothing to resume")
        return cp

    async def _stream_run(
        self,
        *,
        thread: str,
        initial_state: Mapping[str, Any],
        start_frontier: list[str] | None,
        start_step: int,
        resume_values: Mapping[str, Any] | None = None,
    ) -> AsyncIterator[StreamEvent]:
        """Run in a background task, draining the emit queue as a stream."""
        queue: asyncio.Queue[Any] = asyncio.Queue()
        done = object()

        async def _driver() -> None:
            try:
                await run(
                    self._graph,
                    dict(initial_state),
                    thread=thread,
                    start_frontier=start_frontier,
                    start_step=start_step,
                    checkpointer=self.checkpointer,
                    resume_values=resume_values,
                    emit_queue=queue,
                    hooks=self.hooks,
                )
            finally:
                queue.put_nowait(done)

        task = asyncio.create_task(_driver())
        try:
            while True:
                item = await queue.get()
                if item is done:
                    break
                yield item
        finally:
            # Surface a driver exception rather than swallowing it.
            await task


def _run_sync(coro):
    """Run a coroutine to completion from synchronous code.

    Refuses to run inside an existing event loop (that would deadlock);
    the caller should ``await`` the async method there instead.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass  # no running loop — safe to drive one
    else:
        coro.close()
        raise RuntimeError(
            "the *_sync helpers cannot be called from within a running event "
            "loop; await the async method (invoke/stream/resume) instead"
        )
    return asyncio.run(coro)
