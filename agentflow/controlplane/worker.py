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
from agentflow.errors import InterruptError

__all__ = ["Worker", "WorkerPool"]

log = logging.getLogger(__name__)


class _Cancelled(Exception):
    """Internal: the run was cancelled cooperatively."""


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
            await self.queue.complete(
                rec.run_id, owner, status=RunStatus.FAILED, error=str(exc)
            )
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

        heartbeat = asyncio.create_task(self._heartbeat_loop(rec.run_id, owner))
        try:
            work = self._start_work(graph, rec, await self._is_resume(graph, rec))
            await self._race_with_cancel(rec.run_id, work)
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

    def _start_work(self, graph, rec: RunRecord, is_resume: bool):
        if is_resume:
            return graph.resume(rec.thread, rec.resume_value)
        return graph.invoke(dict(rec.input), thread=rec.thread)

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

    async def _heartbeat_loop(self, run_id: str, owner: int) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            if not await self.queue.heartbeat(run_id, owner, lease_seconds=self.lease_seconds):
                # The run was re-claimed by a newer attempt (or finished); this
                # worker is stale. Stop renewing a lease it no longer owns.
                log.warning("run %s lease lost by attempt %d; stopping heartbeat", run_id, owner)
                return

    async def _race_with_cancel(self, run_id: str, work) -> None:
        """Run ``work`` while polling for a cancel request; raise _Cancelled.

        Whatever ends the race — the work finishing, a cooperative cancel, or
        this coroutine itself being cancelled from outside (the worker task is
        cancelled) — the ``finally`` guarantees the child ``work_task`` is
        cancelled and awaited so no graph execution is left running detached in
        the background after the lease is relinquished.
        """
        work_task = asyncio.ensure_future(work)
        try:
            while True:
                done, _ = await asyncio.wait({work_task}, timeout=self.poll_interval)
                if work_task in done:
                    work_task.result()  # propagate exception / completion
                    return
                rec = await self.queue.get(run_id)
                if rec is not None and rec.cancel_requested:
                    raise _Cancelled
        finally:
            if not work_task.done():
                work_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await work_task


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
