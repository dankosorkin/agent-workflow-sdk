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
from typing import Any, AsyncIterator, Mapping

from agentflow.checkpoint.base import Checkpoint
from agentflow.errors import CheckpointError, GraphError, RunTimeout
from agentflow.runtime import StreamEvent, _Graph, run
from agentflow.state import Channel

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
    ) -> None:
        self.schema = schema
        self.checkpointer = checkpointer
        self._graph = _Graph(
            channels=channels,
            nodes=nodes,
            edges=edges,
            branches=branches,
            step_limit=step_limit,
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
        coro = run(
            self._graph,
            dict(input or {}),
            thread=thread,
            start_frontier=None,
            start_step=0,
            checkpointer=self.checkpointer,
        )
        cp = await self._run_with_timeout(coro, thread, timeout)
        return dict(cp.state)

    async def stream(
        self, input: Mapping[str, Any] | None = None, *, thread: str = "default"
    ) -> AsyncIterator[StreamEvent]:
        """Run from ``input``, yielding :class:`StreamEvent`s as they occur."""
        async for ev in self._stream_run(
            thread=thread,
            initial_state=dict(input or {}),
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
        )
        result = await self._run_with_timeout(coro, thread, timeout)
        return dict(result.state)

    async def stream_resume(
        self, thread: str, value: Any = None
    ) -> AsyncIterator[StreamEvent]:
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
    # Inspection / time-travel
    # ------------------------------------------------------------------

    async def get_state(
        self, thread: str, step: int | None = None
    ) -> Checkpoint | None:
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

    async def _run_with_timeout(self, coro, thread: str, timeout: float | None):
        if timeout is None:
            return await coro
        try:
            return await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError:
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
