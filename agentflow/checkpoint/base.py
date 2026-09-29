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

__all__ = ["Checkpoint", "Checkpointer"]


@dataclass(frozen=True, slots=True)
class Checkpoint:
    """One persisted point in a run's history.

    ``next`` is the frontier of node names to run when resuming. An empty
    ``next`` with ``interrupted=False`` means the run finished. When
    ``interrupted`` is True the run is suspended: ``interrupt_node`` requested
    it and ``interrupt_payload`` is what a human must resolve; ``next`` still
    holds the frontier to resume once an answer is supplied.
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
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def done(self) -> bool:
        return not self.next and not self.interrupted


@runtime_checkable
class Checkpointer(Protocol):
    """Persists and retrieves checkpoints by thread and step."""

    async def put(self, cp: Checkpoint) -> str:
        """Persist ``cp`` and return an id (thread + step is sufficient)."""
        ...

    async def get(self, thread: str, step: int | None = None) -> Checkpoint | None:
        """Return the checkpoint at ``step``, or the latest when ``step`` is
        None. Returns None if the thread has no checkpoints."""
        ...

    def history(self, thread: str) -> AsyncIterator[Checkpoint]:
        """Yield every checkpoint for ``thread`` in ascending step order."""
        ...
