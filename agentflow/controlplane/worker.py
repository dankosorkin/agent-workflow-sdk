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

"""Worker + WorkerPool: claim run requests from a queue and execute them.

A worker loops: ``claim`` a run under a lease, resolve its graph from the
registry, execute (``invoke`` or ``resume``) while a background heartbeat keeps
the lease fresh and watches for a cooperative cancel, then ``complete`` the run
with the resulting status. A pool runs several workers concurrently.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

from agentflow.controlplane.queue import RunQueue
from agentflow.controlplane.records import PoolHealth, RunRecord, RunStatus
from agentflow.controlplane.registry import GraphRegistry
from agentflow.errors import InterruptError, LeaseLost

__all__ = ["Worker", "WorkerPool"]

log = logging.getLogger(__name__)


class _Cancelled(Exception):
    """Internal: the run was cancelled cooperatively."""


#: Private alias so the lease-lost arm reads symmetrically with ``_Cancelled``.
_LeaseLost = LeaseLost


class Worker:
    """Executes runs claimed from a :class:`RunQueue`.

    ``lease_seconds`` is the claim lease; ``heartbeat_interval`` how often it is
    renewed (should be well under the lease). ``poll_interval`` is how long
    ``run_forever`` waits when the queue is empty.
    """

    def __init__(
        self,
        queue: RunQueue,
        registry: GraphRegistry,
        *,
        lease_seconds: float = 60.0,
        heartbeat_interval: float = 15.0,
        poll_interval: float = 1.0,
    ) -> None:
        self.queue = queue
        self.registry = registry
        self.lease_seconds = lease_seconds
        self.heartbeat_interval = heartbeat_interval
        self.poll_interval = poll_interval
        self._stopping = False
        self._busy = False

    @property
    def busy(self) -> bool:
        """True while this worker is executing a claimed run."""
        return self._busy

    @property
    def stopping(self) -> bool:
        return self._stopping

    async def run_once(self) -> bool:
        """Claim and execute at most one run. Return True if one was handled."""
        rec = await self.queue.claim(lease_seconds=self.lease_seconds)
        if rec is None:
            return False
        self._busy = True
        try:
            await self._execute(rec)
        finally:
            self._busy = False
        return True

    async def run_forever(self) -> None:
        """Loop claiming and executing runs until :meth:`stop` is called."""
        self._stopping = False
        while not self._stopping:
            did = await self.run_once()
            if not did:
                await asyncio.sleep(self.poll_interval)

    def stop(self) -> None:
        self._stopping = True

    # ------------------------------------------------------------------

    async def _execute(self, rec: RunRecord) -> None:
        # The attempt stamped on the claim is this worker's owner token; every
        # lease mutation carries it so a stale worker cannot clobber a re-claim.
        owner = rec.attempt
        try:
            graph = self.registry.get(rec.graph)
        except KeyError as exc:
            await self.queue.complete(rec.run_id, owner, status=RunStatus.FAILED, error=str(exc))
            return

        # The thread may already sit on a finished checkpoint — e.g. the run
        # crashed after the graph completed but before the queue was marked
        # terminal. Re-running would repeat the whole graph, so just reconcile
        # the queue status from the checkpoint and return.
        if await self._already_done(graph, rec):
            fresh = await self.queue.get(rec.run_id)
            status = (
                RunStatus.CANCELLED
                if (fresh is not None and fresh.cancel_requested)
                else RunStatus.SUCCEEDED
            )
            await self.queue.complete(rec.run_id, owner, status=status)
            return

        lost = asyncio.Event()
        heartbeat = asyncio.create_task(self._heartbeat_loop(rec.run_id, owner, lost))
        try:

            async def owner_check() -> bool:
                return await self.queue.is_owner(rec.run_id, owner)

            work = self._start_work(graph, rec, await self._is_resume(graph, rec), owner_check)
            await self._race_with_cancel(rec.run_id, work, lost)
        except _LeaseLost:
            # Lease lost mid-run: a newer attempt owns the run and its
            # completion. Relinquish entirely — do NOT call queue.complete (the
            # stale owner would be rejected anyway) so the live attempt's work
            # stands. Placed before the cancel/interrupt/exception arms.
            log.warning("run %s lease lost; discarding stale execution", rec.run_id)
            return
        except _Cancelled:
            await self.queue.complete(rec.run_id, owner, status=RunStatus.CANCELLED)
            return
        except InterruptError:
            # Should not escape the runtime, but treat defensively as suspended.
            await self.queue.complete(rec.run_id, owner, status=RunStatus.INTERRUPTED)
            return
        except Exception as exc:  # noqa: BLE001 - report any node/graph failure
            await self.queue.complete(
                rec.run_id, owner, status=RunStatus.FAILED, error=f"{type(exc).__name__}: {exc}"
            )
            return
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat

        # Read the checkpoint the run just wrote to tell finished from
        # suspended, and a human interrupt from a timer wait (ctx.wait).
        status, wake_at = await self._final_status(graph, rec.thread)
        if not await self.queue.complete(rec.run_id, owner, status=status, wake_at=wake_at):
            log.warning(
                "run %s was re-claimed; completion by attempt %d discarded", rec.run_id, owner
            )

    def _start_work(self, graph, rec: RunRecord, is_resume: bool, owner_check):
        if is_resume:
            return graph.resume(rec.thread, rec.resume_value, owner_check=owner_check)
        return graph.invoke(dict(rec.input), thread=rec.thread, owner_check=owner_check)

    async def _is_resume(self, graph, rec: RunRecord) -> bool:
        """A claim is a resume iff the thread already sits on an unfinished
        checkpoint. This covers BOTH a human/timer interrupt AND a plain
        checkpoint left after a completed super-step whose run crashed before
        finishing: resuming continues from the saved frontier instead of
        re-running the graph from START (which would repeat earlier steps and
        their side effects). Authoritative regardless of attempt count or
        whether a stored human answer happened to be None."""
        if graph.checkpointer is None:
            return False
        cp = await graph.get_state(rec.thread)
        return cp is not None and not cp.done

    async def _already_done(self, graph, rec: RunRecord) -> bool:
        """True when the thread already sits on a finished checkpoint, so there
        is nothing left to run — only the queue status to reconcile."""
        if graph.checkpointer is None:
            return False
        cp = await graph.get_state(rec.thread)
        return cp is not None and cp.done

    async def _final_status(self, graph, thread: str) -> tuple[str, str | None]:
        cp = await graph.get_state(thread) if graph.checkpointer is not None else None
        if cp is None or not cp.interrupted:
            return RunStatus.SUCCEEDED, None
        # A timer wait (ctx.wait) parks the run until wake_at; a human interrupt
        # waits for an explicit resume. Same checkpoint flag, different queue
        # state and resolver.
        if cp.suspend_kind == "timer":
            return RunStatus.WAITING, cp.wake_at
        return RunStatus.INTERRUPTED, None

    async def _heartbeat_loop(self, run_id: str, owner: int, lost: asyncio.Event) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            if not await self.queue.heartbeat(run_id, owner, lease_seconds=self.lease_seconds):
                # The run was re-claimed by a newer attempt (or finished); this
                # worker is stale. Signal the race to cancel the graph task so
                # it stops running nodes and writing checkpoints, not merely
                # stop renewing the lease.
                log.warning("run %s lease lost by attempt %d; cancelling work", run_id, owner)
                lost.set()
                return

    async def _race_with_cancel(self, run_id: str, work, lost: asyncio.Event) -> None:
        """Run ``work`` while watching for a lost lease or a cancel request.

        Raises :class:`_LeaseLost` when the heartbeat loop reports the lease
        gone (checked FIRST — the harder stop), else :class:`_Cancelled` on a
        cooperative cancel request. Whatever ends the race — the work finishing,
        a lost lease, a cooperative cancel, or this coroutine itself being
        cancelled from outside — the ``finally`` cancels and awaits both the
        work task and the lost-wait so no graph execution is left running
        detached after the lease is relinquished.

        A simultaneous normal completion and lease loss resolves in favor of the
        lease loss (the newer attempt owns the run and re-derives terminal state
        on its own), so the stale completion is discarded by design.
        """
        work_task = asyncio.ensure_future(work)
        lost_wait = asyncio.ensure_future(lost.wait())
        try:
            while True:
                await asyncio.wait(
                    {work_task, lost_wait},
                    timeout=self.poll_interval,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if lost.is_set():  # lease loss — the harder stop, checked first
                    raise _LeaseLost(run_id)
                if work_task.done():
                    work_task.result()  # propagate exception / completion
                    return
                rec = await self.queue.get(run_id)
                if rec is not None and rec.cancel_requested:
                    raise _Cancelled
        finally:
            for t in (work_task, lost_wait):
                if not t.done():
                    t.cancel()
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await t


class WorkerPool:
    """Runs ``concurrency`` workers over one queue+registry."""

    def __init__(
        self,
        queue: RunQueue,
        registry: GraphRegistry,
        *,
        concurrency: int = 4,
        lease_seconds: float = 60.0,
        poll_interval: float = 1.0,
    ) -> None:
        self._workers = [
            Worker(
                queue,
                registry,
                lease_seconds=lease_seconds,
                poll_interval=poll_interval,
            )
            for _ in range(concurrency)
        ]
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        self._tasks = [asyncio.create_task(w.run_forever()) for w in self._workers]

    async def stop(self) -> None:
        """Signal workers to stop after their current run, then await them."""
        for w in self._workers:
            w.stop()
        for t in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await t
        self._tasks = []

    def health(self) -> PoolHealth:
        """Snapshot of the pool: configured size, live loops, and busy count.

        A loop is "alive" if it was started and its task has not finished
        (neither stopped cleanly nor crashed). ``busy`` counts workers mid-run.
        Use ``health().healthy`` as a readiness signal — False means a worker
        loop died and the pool is running degraded."""
        alive = sum(1 for t in self._tasks if not t.done())
        busy = sum(1 for w in self._workers if w.busy)
        return PoolHealth(workers=len(self._workers), alive=alive, busy=busy)
