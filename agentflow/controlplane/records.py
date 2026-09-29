# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""Control-plane data records: run status model and the RunRecord row.

These are the serializable facts about a queued/executing run — everything a
worker or an HTTP layer needs without touching the graph code itself.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Final

__all__ = ["RunStatus", "RunRecord", "QueueStats", "PoolHealth"]


class RunStatus:
    """The lifecycle states of a run (string constants, JSON-friendly).

    ``queued`` -> ``running`` -> ``succeeded`` | ``interrupted`` | ``failed`` |
    ``cancelled``. An ``interrupted`` run returns to ``queued`` via
    ``enqueue_resume``; a ``queued``/``running`` run can be cancelled.
    """

    QUEUED: Final = "queued"
    RUNNING: Final = "running"
    SUCCEEDED: Final = "succeeded"
    INTERRUPTED: Final = "interrupted"
    FAILED: Final = "failed"
    CANCELLED: Final = "cancelled"

    #: Terminal states — a run in one of these will not run again on its own.
    TERMINAL: Final = frozenset({SUCCEEDED, FAILED, CANCELLED})
    #: States a worker may claim (queued, or running with an expired lease).
    CLAIMABLE: Final = frozenset({QUEUED, RUNNING})


@dataclass(frozen=True, slots=True)
class RunRecord:
    """One run's queue entry and status.

    ``graph`` names a factory in a :class:`~agentflow.controlplane.registry.
    GraphRegistry`; ``thread`` is the checkpointer thread the run executes on
    (defaults to ``run_id``). ``resume_value`` carries the human answer when an
    interrupted run is re-queued. ``lease_until`` is the ISO time a worker's
    claim holds until; an expired lease lets another worker re-claim (crash
    recovery). ``cancel_requested`` is a cooperative flag a worker honors at a
    super-step boundary.
    """

    run_id: str
    graph: str
    thread: str
    status: str
    input: Mapping[str, Any] = field(default_factory=dict)
    resume_value: Any = None
    error: str | None = None
    attempt: int = 0
    created_at: str = ""
    updated_at: str = ""
    lease_until: str | None = None
    cancel_requested: bool = False
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class QueueStats:
    """A point-in-time snapshot of a run queue's depth, for monitoring.

    ``by_status`` counts runs in every lifecycle state. ``expired_leases`` is
    the number of ``running`` runs whose lease is past due — i.e. work stranded
    by a crashed/stalled worker and awaiting re-claim. A rising value there is
    the signal that workers are dying or falling behind their heartbeat.
    """

    total: int = 0
    by_status: Mapping[str, int] = field(default_factory=dict)
    expired_leases: int = 0

    @property
    def queued(self) -> int:
        return self.by_status.get(RunStatus.QUEUED, 0)

    @property
    def running(self) -> int:
        return self.by_status.get(RunStatus.RUNNING, 0)


@dataclass(frozen=True, slots=True)
class PoolHealth:
    """Health snapshot of a :class:`~agentflow.controlplane.worker.WorkerPool`.

    ``workers`` is the configured size; ``alive`` counts worker loops still
    running (not stopped and not crashed); ``busy`` counts workers currently
    executing a run; ``idle`` = alive - busy. ``healthy`` is True when every
    configured worker loop is alive.
    """

    workers: int = 0
    alive: int = 0
    busy: int = 0

    @property
    def idle(self) -> int:
        return max(self.alive - self.busy, 0)

    @property
    def healthy(self) -> bool:
        return self.alive == self.workers and self.workers > 0
