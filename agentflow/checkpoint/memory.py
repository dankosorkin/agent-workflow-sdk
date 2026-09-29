"""In-memory checkpointer: fast, ephemeral, for tests and short runs."""

from __future__ import annotations

import dataclasses
from collections.abc import AsyncIterator

from agentflow.checkpoint.base import Checkpoint
from agentflow.errors import CheckpointConflict

__all__ = ["MemoryCheckpointer"]


class MemoryCheckpointer:
    """Keeps checkpoints in a dict keyed by thread. Not durable."""

    def __init__(self) -> None:
        self._threads: dict[str, dict[int, Checkpoint]] = {}

    async def put(self, cp: Checkpoint, *, if_revision: int | None = None) -> str:
        steps = self._threads.setdefault(cp.thread, {})
        existing = steps.get(cp.step)
        current_rev = existing.revision if existing else 0
        if if_revision is not None and if_revision != current_rev:
            raise CheckpointConflict(cp.thread, cp.step, if_revision, current_rev)
        steps[cp.step] = dataclasses.replace(cp, revision=current_rev + 1)
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

    async def delete_thread(self, thread: str) -> None:
        self._threads.pop(thread, None)

    async def prune(
        self, thread: str, *, before_step: int | None = None, older_than: str | None = None
    ) -> int:
        steps = self._threads.get(thread)
        if not steps:
            return 0
        to_remove = [
            s
            for s, cp in steps.items()
            if (before_step is not None and s < before_step)
            or (older_than is not None and cp.ts and cp.ts < older_than)
        ]
        for s in to_remove:
            del steps[s]
        return len(to_remove)
