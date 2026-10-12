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

"""In-memory checkpointer: fast, ephemeral, for tests and short runs."""

from __future__ import annotations

import copy
import dataclasses
from collections.abc import AsyncIterator

from agentflow.checkpoint.base import Checkpoint, ThreadInfo
from agentflow.errors import CheckpointConflict

__all__ = ["MemoryCheckpointer"]


def _isolate(cp: Checkpoint) -> Checkpoint:
    """Return a deep copy of the mutable payload fields of ``cp``.

    The in-memory store holds Python objects directly, so without copying, a
    caller mutating a nested container in a checkpoint's ``state`` (before or
    after it is stored) would silently rewrite history. Deep-copy at both the
    write and read boundaries so a stored checkpoint is an immutable snapshot.
    """
    return dataclasses.replace(
        cp,
        state=copy.deepcopy(dict(cp.state)),
        interrupt_payload=copy.deepcopy(cp.interrupt_payload),
        interrupt_answers=copy.deepcopy(tuple(cp.interrupt_answers)),
        invocations=copy.deepcopy(dict(cp.invocations)),
        extra=copy.deepcopy(dict(cp.extra)),
    )


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
        isolated = _isolate(cp)
        steps[cp.step] = dataclasses.replace(isolated, revision=current_rev + 1)
        return f"{cp.thread}:{cp.step}"

    async def get(self, thread: str, step: int | None = None) -> Checkpoint | None:
        steps = self._threads.get(thread)
        if not steps:
            return None
        if step is None:
            step = max(steps)
        stored = steps.get(step)
        return _isolate(stored) if stored is not None else None

    async def history(self, thread: str) -> AsyncIterator[Checkpoint]:
        steps = self._threads.get(thread, {})
        for step in sorted(steps):
            yield _isolate(steps[step])

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
