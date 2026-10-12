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

"""RunQueue protocol: durable queue of run requests, worked by a pool.

A queue decouples *who asked for a run* from *who executes it*. An enqueuer
adds a request; workers ``claim`` requests under a time-bounded lease, execute
the graph, and ``complete`` them. A crashed worker's lease expires so its run
can be re-claimed. Cancellation is cooperative: ``request_cancel`` sets a flag
the worker honors at a super-step boundary.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol, runtime_checkable

from agentflow.controlplane.records import QueueStats, RunRecord

__all__ = ["RunQueue"]


@runtime_checkable
class RunQueue(Protocol):
    """Persists run requests and hands them to workers under a lease.

    Ownership: :meth:`claim` stamps the run with an incrementing ``attempt``
    number and returns it on the record; that number is the claiming worker's
    *owner token*. Every subsequent mutation of the claim — :meth:`heartbeat`
    and :meth:`complete` — must pass the same ``owner`` and is a no-op (returns
    ``False``) when the run has since been re-claimed by a newer attempt. This
    stops a stale worker (one whose lease expired and whose run was handed to a
    fresh attempt) from extending the lease or writing a terminal status over
    the live attempt's work.
    """

    async def enqueue(
        self, graph: str, input: Mapping[str, Any] | None = None, *, thread: str | None = None
    ) -> RunRecord:
        """Add a new run for ``graph`` and return its record (status queued).
        ``thread`` defaults to the generated run id."""
        ...

    async def get(self, run_id: str) -> RunRecord | None:
        """Return the run record, or None if unknown."""
        ...

    async def list(
        self,
        *,
        status: str | None = None,
        graph: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[RunRecord]:
        """Return run records, newest first, optionally filtered."""
        ...

    async def claim(self, *, lease_seconds: float = 60.0) -> RunRecord | None:
        """Atomically take one runnable request and mark it running under a
        fresh lease. Runnable means: queued, running with an expired lease, or
        waiting (parked on ``ctx.wait``) with its ``wake_at`` now past. Returns
        None if nothing is available. Safe for concurrent workers."""
        ...

    async def heartbeat(self, run_id: str, owner: int, *, lease_seconds: float = 60.0) -> bool:
        """Extend the lease on a claimed run (call periodically while working).

        ``owner`` is the ``attempt`` returned by :meth:`claim`. Returns True
        when the lease was extended; False when the run is unknown, no longer
        running, or has been re-claimed by a newer attempt (the caller is a
        stale worker and should stop)."""
        ...

    async def complete(
        self,
        run_id: str,
        owner: int,
        *,
        status: str,
        error: str | None = None,
        wake_at: str | None = None,
    ) -> bool:
        """Move a claimed run to a terminal-ish status (succeeded / failed /
        interrupted / cancelled), clearing its lease. For a ``waiting`` status
        (parked on ``ctx.wait``), pass ``wake_at`` so the run becomes claimable
        again once that time passes.

        ``owner`` is the ``attempt`` returned by :meth:`claim`. Returns True
        when the run was updated; False when the run is unknown or has been
        re-claimed by a newer attempt (the caller is a stale worker and its
        completion is discarded)."""
        ...

    async def request_cancel(self, run_id: str) -> bool:
        """Request cooperative cancellation; return True if the run exists and
        is not already terminal. A queued run is cancelled immediately; a
        running one is flagged for the worker to stop at the next boundary."""
        ...

    async def enqueue_resume(self, run_id: str, value: Any = None) -> RunRecord:
        """Re-queue an interrupted run with the human ``value`` so a worker
        resumes it. Returns the updated record (status queued)."""
        ...

    async def stats(self) -> QueueStats:
        """Return a point-in-time snapshot of queue depth by status plus the
        count of expired-lease (stranded) runs — for health/monitoring."""
        ...
