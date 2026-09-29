# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""In-memory checkpointer: fast, ephemeral, for tests and short runs."""

from __future__ import annotations

import dataclasses
from collections.abc import AsyncIterator

from agentflow.checkpoint.base import Checkpoint, ThreadInfo
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

    async def list_threads(self, *, limit: int = 100, offset: int = 0) -> list[ThreadInfo]:
        infos: list[ThreadInfo] = []
        for thread in sorted(self._threads):
            steps = self._threads[thread]
            if not steps:
                continue
            cp = steps[max(steps)]
            infos.append(
                ThreadInfo(
                    thread=thread,
                    latest_step=cp.step,
                    interrupted=cp.interrupted,
                    done=cp.done,
                    ts=cp.ts,
                )
            )
        return infos[offset : offset + limit]
