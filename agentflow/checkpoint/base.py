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

"""Checkpointer protocol and the Checkpoint record.

A checkpoint captures the full workflow state plus the pending frontier at a
super-step boundary, so a run can be resumed, inspected, or forked
(time-travel). It also records whether the run is suspended on a
human-in-the-loop interrupt and, if so, what payload the human must answer.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from agentflow.errors import CheckpointConflict

__all__ = ["Checkpoint", "Checkpointer", "CheckpointConflict", "ThreadInfo"]


@dataclass(frozen=True, slots=True)
class ThreadInfo:
    """A one-line summary of a thread for control-plane listing.

    Derived from the thread's latest checkpoint so a caller can enumerate runs
    (running / interrupted / finished) without reading full state.
    """

    thread: str
    latest_step: int
    interrupted: bool
    done: bool
    ts: str = ""


@dataclass(frozen=True, slots=True)
class Checkpoint:
    """One persisted point in a run's history.

    ``next`` is the frontier of node names to run when resuming. An empty
    ``next`` with ``interrupted=False`` means the run finished. When
    ``interrupted`` is True the run is suspended: ``interrupt_node`` requested
    it and ``interrupt_payload`` is what a human must resolve; ``next`` still
    holds the frontier to resume once an answer is supplied.

    A suspend has a *kind* (:attr:`suspend_kind`). ``"human"`` waits for an
    external :meth:`resume` with a value (human-in-the-loop). ``"timer"`` waits
    only for the clock: :attr:`wake_at` is the earliest ISO time the run should
    be resumed, and resuming re-runs the suspending node (which polls again).
    Both share the interrupt machinery — a timer wait is an interrupt whose
    resolver is the clock, not a person.
    """

    thread: str
    step: int
    state: Mapping[str, Any]
    next: tuple[str, ...] = ()
    parent: str | None = None
    ts: str = ""
    interrupted: bool = False
    interrupt_node: str | None = None
    interrupt_payload: Any = None
    #: ISO-8601 UTC time the run should wake, for a ``"timer"`` suspend; None
    #: for a ``"human"`` suspend (which waits for an explicit resume value).
    wake_at: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)
    #: Monotonic write count for this (thread, step). Set by the store on read;
    #: pass the value you read back as ``put(..., if_revision=)`` to guard a
    #: read-modify-write against a concurrent resume (optimistic concurrency).
    #: 0 means "never written" / not tracked by this backend.
    revision: int = 0

    @property
    def done(self) -> bool:
        return not self.next and not self.interrupted

    @property
    def suspend_kind(self) -> str | None:
        """``"human"`` or ``"timer"`` while suspended, else None.

        Derived so the on-disk shape stays compatible: a suspend with
        ``wake_at`` set is a timer wait; a suspend without it waits for a human.
        """
        if not self.interrupted:
            return None
        return "timer" if self.wake_at is not None else "human"


@runtime_checkable
class Checkpointer(Protocol):
    """Persists and retrieves checkpoints by thread and step."""

    async def put(self, cp: Checkpoint, *, if_revision: int | None = None) -> str:
        """Persist ``cp`` and return an id (thread + step is sufficient).

        When ``if_revision`` is given, the write is conditional: it succeeds
        only if the currently stored revision for ``(cp.thread, cp.step)``
        equals ``if_revision`` (or nothing is stored and ``if_revision`` is 0),
        otherwise it raises :class:`~agentflow.errors.CheckpointConflict`. This
        gives optimistic concurrency for two resumes racing the same step.
        ``None`` (default) is an unconditional upsert.
        """
        ...

    async def get(self, thread: str, step: int | None = None) -> Checkpoint | None:
        """Return the checkpoint at ``step``, or the latest when ``step`` is
        None. Returns None if the thread has no checkpoints. The returned
        checkpoint's ``revision`` reflects how many times that step was
        written (0 if the backend doesn't track it)."""
        ...

    def history(self, thread: str) -> AsyncIterator[Checkpoint]:
        """Yield every checkpoint for ``thread`` in ascending step order."""
        ...

    async def list_threads(self, *, limit: int = 100, offset: int = 0) -> list[ThreadInfo]:
        """Return a summary of each stored thread (latest step + status),
        ordered by thread id. Enables control-plane enumeration of runs
        without reading full state. ``limit``/``offset`` paginate."""
        ...

    async def delete_thread(self, thread: str) -> None:
        """Remove all checkpoints for ``thread`` (retention/cleanup)."""
        ...

    async def prune(
        self, thread: str, *, before_step: int | None = None, older_than: str | None = None
    ) -> int:
        """Delete checkpoints for ``thread`` older than a bound; return the
        count removed. ``before_step`` deletes steps strictly less than it;
        ``older_than`` is an ISO timestamp compared against each checkpoint's
        ``ts``. At least one bound should be given."""
        ...
