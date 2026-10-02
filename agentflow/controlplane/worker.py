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

from agentflow.controlplane.queue import RunQueue
from agentflow.controlplane.records import PoolHealth, RunRecord, RunStatus
from agentflow.controlplane.registry import GraphRegistry
from agentflow.errors import InterruptError

__all__ = ["Worker", "WorkerPool"]


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
        try:
            graph = self.registry.get(rec.graph)
        except KeyError as exc:
            await self.queue.complete(rec.run_id, status=RunStatus.FAILED, error=str(exc))
            return

        heartbeat = asyncio.create_task(self._heartbeat_loop(rec.run_id))
        try:
            work = self._start_work(graph, rec, await self._is_resume(graph, rec))
            await self._race_with_cancel(rec.run_id, work)
        except _Cancelled:
            await self.queue.complete(rec.run_id, status=RunStatus.CANCELLED)
            return
        except InterruptError:
            # Should not escape the runtime, but treat defensively as suspended.
            await self.queue.complete(rec.run_id, status=RunStatus.INTERRUPTED)
            return
        except Exception as exc:  # noqa: BLE001 - report any node/graph failure
            await self.queue.complete(
                rec.run_id, status=RunStatus.FAILED, error=f"{type(exc).__name__}: {exc}"
            )
            return
        finally:
            heartbeat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat

        # Read the checkpoint the run just wrote to tell finished from
        # suspended, and a human interrupt from a timer wait (ctx.wait).
        status, wake_at = await self._final_status(graph, rec.thread)
        await self.queue.complete(rec.run_id, status=status, wake_at=wake_at)

    def _start_work(self, graph, rec: RunRecord, is_resume: bool):
        if is_resume:
            return graph.resume(rec.thread, rec.resume_value)
        return graph.invoke(dict(rec.input), thread=rec.thread)

    async def _is_resume(self, graph, rec: RunRecord) -> bool:
        """A claim is a resume iff the thread already sits on an interrupted,
        not-yet-finished checkpoint. Authoritative regardless of attempt count
        or whether the human answer happened to be None."""
        if graph.checkpointer is None:
            return False
        cp = await graph.get_state(rec.thread)
        return cp is not None and cp.interrupted and not cp.done

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

    async def _heartbeat_loop(self, run_id: str) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            await self.queue.heartbeat(run_id, lease_seconds=self.lease_seconds)

    async def _race_with_cancel(self, run_id: str, work) -> None:
        """Run ``work`` while polling for a cancel request; raise _Cancelled."""
        work_task = asyncio.ensure_future(work)
        while True:
            done, _ = await asyncio.wait({work_task}, timeout=self.poll_interval)
            if work_task in done:
                work_task.result()  # propagate exception / completion
                return
            rec = await self.queue.get(run_id)
            if rec is not None and rec.cancel_requested:
                work_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await work_task
                raise _Cancelled


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
