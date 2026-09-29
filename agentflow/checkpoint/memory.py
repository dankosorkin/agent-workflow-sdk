"""In-memory checkpointer: fast, ephemeral, for tests and short runs."""

from __future__ import annotations

from collections.abc import AsyncIterator

from agentflow.checkpoint.base import Checkpoint

__all__ = ["MemoryCheckpointer"]


class MemoryCheckpointer:
    """Keeps checkpoints in a dict keyed by thread. Not durable."""

    def __init__(self) -> None:
        self._threads: dict[str, dict[int, Checkpoint]] = {}

    async def put(self, cp: Checkpoint) -> str:
        self._threads.setdefault(cp.thread, {})[cp.step] = cp
        return f"{cp.thread}:{cp.step}"

    async def get(self, thread: str, step: int | None = None) -> Checkpoint | None:
        steps = self._threads.get(thread)
        if not steps:
            return None
        if step is None:
            step = max(steps)
        return steps.get(step)

    async def history(self, thread: str) -> AsyncIterator[Checkpoint]:
        steps = self._threads.get(thread, {})
        for step in sorted(steps):
            yield steps[step]
