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

"""Control plane: GraphRegistry, MemoryRunQueue, and end-to-end Worker."""

from __future__ import annotations

import asyncio
import contextlib
from typing import Annotated

import pytest
from agentflow import (
    END,
    START,
    Graph,
    GraphRegistry,
    MemoryCheckpointer,
    MemoryRunQueue,
    RunStatus,
    State,
    Worker,
    append,
    last,
)


class Doubler(State):
    n: Annotated[int, last]


def _linear_graph(checkpointer):
    g = Graph(Doubler)

    async def double(state, ctx):
        return {"n": state["n"] * 2}

    g.add_node("double", double)
    g.add_edge(START, "double")
    g.add_edge("double", END)
    return g.compile(checkpointer=checkpointer)


class HitlState(State):
    answer: Annotated[str, last]
    stage: Annotated[list, append]


def _hitl_graph(checkpointer):
    g = Graph(HitlState)

    async def ask(state, ctx):
        human = await ctx.interrupt({"q": "ok?"})
        return {"answer": human, "stage": "asked"}

    async def finish(state, ctx):
        return {"stage": "finished"}

    g.add_node("ask", ask)
    g.add_node("finish", finish)
    g.add_edge(START, "ask")
    g.add_edge("ask", "finish")
    g.add_edge("finish", END)
    return g.compile(checkpointer=checkpointer)


class SlowState(State):
    n: Annotated[int, last]


def _slow_graph(checkpointer, delay=5.0):
    g = Graph(SlowState)

    async def slow(state, ctx):
        await asyncio.sleep(delay)
        return {"n": 1}

    g.add_node("slow", slow)
    g.add_edge(START, "slow")
    g.add_edge("slow", END)
    return g.compile(checkpointer=checkpointer)


# --- GraphRegistry ---------------------------------------------------------


def test_registry_caches_instance():
    reg = GraphRegistry()
    calls = {"n": 0}

    def factory():
        calls["n"] += 1
        return _linear_graph(MemoryCheckpointer())

    reg.register("g", factory)
    a = reg.get("g")
    b = reg.get("g")
    assert a is b and calls["n"] == 1
    assert reg.names() == ["g"] and "g" in reg


def test_registry_rejects_dupe_and_unknown():
    reg = GraphRegistry()
    reg.register("g", lambda: _linear_graph(MemoryCheckpointer()))
    with pytest.raises(ValueError):
        reg.register("g", lambda: _linear_graph(MemoryCheckpointer()))
    with pytest.raises(KeyError):
        reg.get("nope")


# --- MemoryRunQueue --------------------------------------------------------


async def test_enqueue_and_get():
    q = MemoryRunQueue()
    rec = await q.enqueue("g", {"n": 5})
    assert rec.status == RunStatus.QUEUED and rec.thread == rec.run_id
    assert (await q.get(rec.run_id)).input == {"n": 5}


async def test_claim_marks_running_and_is_exclusive():
    q = MemoryRunQueue()
    await q.enqueue("g", {"n": 1})
    first = await q.claim()
    assert first is not None and first.status == RunStatus.RUNNING
    # No other queued run -> second claim finds nothing (lease not expired).
    assert await q.claim() is None


async def test_claim_reclaims_expired_lease():
    q = MemoryRunQueue()
    await q.enqueue("g", {"n": 1})
    await q.claim(lease_seconds=0.01)
    await asyncio.sleep(0.03)
    again = await q.claim(lease_seconds=60)
    assert again is not None and again.attempt == 2


async def test_complete_and_list_filter():
    q = MemoryRunQueue()
    r1 = await q.enqueue("g", {"n": 1})
    await q.enqueue("h", {"n": 2})
    claimed = await q.claim()  # claims r1 (oldest)
    await q.complete(r1.run_id, claimed.attempt, status=RunStatus.SUCCEEDED)
    done = await q.list(status=RunStatus.SUCCEEDED)
    assert [r.run_id for r in done] == [r1.run_id]
    assert len(await q.list(graph="h")) == 1


async def test_request_cancel_queued_is_immediate():
    q = MemoryRunQueue()
    rec = await q.enqueue("g", {"n": 1})
    assert await q.request_cancel(rec.run_id) is True
    assert (await q.get(rec.run_id)).status == RunStatus.CANCELLED


async def test_request_cancel_unknown_and_terminal():
    q = MemoryRunQueue()
    assert await q.request_cancel("missing") is False
    rec = await q.enqueue("g")
    claimed = await q.claim()
    await q.complete(rec.run_id, claimed.attempt, status=RunStatus.SUCCEEDED)
    assert await q.request_cancel(rec.run_id) is False


async def test_heartbeat_unknown_returns_false():
    q = MemoryRunQueue()
    assert await q.heartbeat("missing", 1) is False


async def test_stale_worker_cannot_complete():
    """Bug 4: after a lease expires and the run is re-claimed, the first
    worker's attempt token no longer owns the run — its heartbeat and
    completion must be no-ops so it cannot clobber the live attempt."""
    q = MemoryRunQueue()
    rec = await q.enqueue("g", {"n": 1})
    c1 = await q.claim(lease_seconds=0.01)  # owner = attempt 1
    assert c1 is not None and c1.attempt == 1
    await asyncio.sleep(0.03)  # let the lease expire
    c2 = await q.claim(lease_seconds=60)  # re-claim, owner = attempt 2
    assert c2 is not None and c2.attempt == 2

    # Stale attempt 1 may neither complete nor heartbeat the re-claimed run.
    assert await q.complete(rec.run_id, 1, status=RunStatus.SUCCEEDED) is False
    assert await q.heartbeat(rec.run_id, 1) is False
    now = await q.get(rec.run_id)
    assert now.status == RunStatus.RUNNING and now.attempt == 2

    # The live attempt 2 completes normally.
    assert await q.complete(rec.run_id, 2, status=RunStatus.SUCCEEDED) is True
    assert (await q.get(rec.run_id)).status == RunStatus.SUCCEEDED


# --- Worker end-to-end -----------------------------------------------------


async def test_worker_runs_to_success():
    cp = MemoryCheckpointer()
    reg = GraphRegistry()
    reg.register("dbl", lambda: _linear_graph(cp))
    q = MemoryRunQueue()
    rec = await q.enqueue("dbl", {"n": 21})

    worker = Worker(q, reg, poll_interval=0.01)
    handled = await worker.run_once()
    assert handled is True
    final = await q.get(rec.run_id)
    assert final.status == RunStatus.SUCCEEDED
    assert (await cp.get(rec.run_id)).state["n"] == 42


async def test_worker_missing_graph_fails_run():
    reg = GraphRegistry()  # nothing registered
    q = MemoryRunQueue()
    rec = await q.enqueue("ghost")
    worker = Worker(q, reg, poll_interval=0.01)
    await worker.run_once()
    final = await q.get(rec.run_id)
    assert final.status == RunStatus.FAILED and "ghost" in final.error


async def test_worker_interrupt_then_resume():
    cp = MemoryCheckpointer()
    reg = GraphRegistry()
    reg.register("hitl", lambda: _hitl_graph(cp))
    q = MemoryRunQueue()
    rec = await q.enqueue("hitl", {"stage": []})

    worker = Worker(q, reg, poll_interval=0.01)
    await worker.run_once()
    assert (await q.get(rec.run_id)).status == RunStatus.INTERRUPTED

    await q.enqueue_resume(rec.run_id, value="yes")
    assert (await q.get(rec.run_id)).status == RunStatus.QUEUED
    await worker.run_once()

    final = await q.get(rec.run_id)
    assert final.status == RunStatus.SUCCEEDED
    state = await cp.get(rec.run_id)
    assert state.state["answer"] == "yes"
    assert state.state["stage"] == ["asked", "finished"]


class CrashState(State):
    trace: Annotated[list, append]


_crash_counter = {"a": 0, "b": 0}


def _crash_graph(checkpointer):
    """Two-node graph: A records that it ran; B fails on its first call and
    succeeds on its second. Used to prove a crashed run resumes from the saved
    checkpoint rather than re-running A."""
    g = Graph(CrashState)

    async def a(state, ctx):
        _crash_counter["a"] += 1
        return {"trace": "a"}

    async def b(state, ctx):
        _crash_counter["b"] += 1
        if _crash_counter["b"] == 1:
            raise RuntimeError("boom")
        return {"trace": "b"}

    g.add_node("a", a)
    g.add_node("b", b)
    g.add_edge(START, "a")
    g.add_edge("a", "b")
    g.add_edge("b", END)
    return g.compile(checkpointer=checkpointer)


async def test_worker_resumes_after_crash_not_restart():
    """Bug 1: a crash after step A's checkpoint must resume at B, not re-run
    the whole graph from START (which would run A twice, repeating side
    effects)."""
    _crash_counter["a"] = 0
    _crash_counter["b"] = 0
    cp = MemoryCheckpointer()
    reg = GraphRegistry()
    reg.register("crash", lambda: _crash_graph(cp))
    q = MemoryRunQueue()
    rec = await q.enqueue("crash", {"trace": []})

    worker = Worker(q, reg, poll_interval=0.01)
    await worker.run_once()  # A ok, B raises -> FAILED
    assert (await q.get(rec.run_id)).status == RunStatus.FAILED
    assert _crash_counter["a"] == 1 and _crash_counter["b"] == 1

    # Re-queue and re-run; must resume from A's checkpoint (frontier = [b]).
    await q.enqueue_resume(rec.run_id)
    await worker.run_once()

    final = await q.get(rec.run_id)
    assert final.status == RunStatus.SUCCEEDED
    # A ran exactly once across the crash; only B retried.
    assert _crash_counter["a"] == 1
    assert (await cp.get(rec.thread)).state["trace"] == ["a", "b"]


class ExternalCancelState(State):
    n: Annotated[int, last]


_external_counter = {"n": 0}


def _side_effect_graph(checkpointer, delay):
    """A single slow node that performs an observable side effect only AFTER a
    sleep — so cancelling the worker mid-sleep must prevent the increment."""
    g = Graph(ExternalCancelState)

    async def slow(state, ctx):
        await asyncio.sleep(delay)
        _external_counter["n"] += 1
        return {"n": _external_counter["n"]}

    g.add_node("slow", slow)
    g.add_edge(START, "slow")
    g.add_edge("slow", END)
    return g.compile(checkpointer=checkpointer)


async def test_external_cancel_stops_graph_execution():
    """Bug 5: cancelling the worker task from outside must cancel the in-flight
    graph work, not leave it running detached in the background."""
    _external_counter["n"] = 0
    cp = MemoryCheckpointer()
    reg = GraphRegistry()
    reg.register("se", lambda: _side_effect_graph(cp, delay=0.3))
    q = MemoryRunQueue()
    await q.enqueue("se", {"n": 0})

    worker = Worker(q, reg, poll_interval=0.02)
    task = asyncio.ensure_future(worker.run_once())
    await asyncio.sleep(0.1)  # let the worker claim and enter the slow node
    task.cancel()  # external cancel (NOT request_cancel)
    with contextlib.suppress(asyncio.CancelledError):
        await task

    # Wait well past when the node would have incremented had it kept running.
    await asyncio.sleep(0.5)
    assert _external_counter["n"] == 0  # the detached work was cancelled


async def test_worker_cooperative_cancel():
    cp = MemoryCheckpointer()
    reg = GraphRegistry()
    reg.register("slow", lambda: _slow_graph(cp, delay=5.0))
    q = MemoryRunQueue()
    rec = await q.enqueue("slow", {"n": 0})

    worker = Worker(q, reg, poll_interval=0.02)
    task = asyncio.ensure_future(worker.run_once())
    await asyncio.sleep(0.1)  # let the worker claim and start the slow node
    assert await q.request_cancel(rec.run_id) is True
    await asyncio.wait_for(task, timeout=2.0)
    assert (await q.get(rec.run_id)).status == RunStatus.CANCELLED


# --- Queue stats + pool health --------------------------------------------


async def test_cancel_interrupted_run_finishes():
    """Bug 6: cancelling an interrupted (suspended) run must transition it to
    CANCELLED immediately. A worker never re-claims an interrupted run, so the
    old cancel_requested=True flag left it suspended forever."""
    cp = MemoryCheckpointer()
    reg = GraphRegistry()
    reg.register("hitl", lambda: _hitl_graph(cp))
    q = MemoryRunQueue()
    rec = await q.enqueue("hitl", {"stage": []})

    worker = Worker(q, reg, poll_interval=0.01)
    await worker.run_once()
    assert (await q.get(rec.run_id)).status == RunStatus.INTERRUPTED

    assert await q.request_cancel(rec.run_id) is True
    # Terminal immediately — not left as interrupted.
    assert (await q.get(rec.run_id)).status == RunStatus.CANCELLED
    # And it is no longer claimable (the run is terminal).
    assert await q.claim() is None


async def test_queue_stats_by_status():
    from agentflow import QueueStats

    q = MemoryRunQueue()
    r1 = await q.enqueue("g")
    await q.enqueue("g")
    claimed = await q.claim()  # claims r1 -> running
    await q.complete(r1.run_id, claimed.attempt, status=RunStatus.SUCCEEDED)

    stats = await q.stats()
    assert isinstance(stats, QueueStats)
    assert stats.total == 2
    assert stats.queued == 1
    assert stats.by_status.get(RunStatus.SUCCEEDED) == 1
    assert stats.running == 0
    assert stats.expired_leases == 0


async def test_queue_stats_counts_expired_leases():
    q = MemoryRunQueue()
    await q.enqueue("g")
    await q.claim(lease_seconds=0.01)  # running with a lease about to expire
    await asyncio.sleep(0.03)
    stats = await q.stats()
    assert stats.running == 1
    assert stats.expired_leases == 1  # stranded, awaiting re-claim


async def test_worker_busy_flag_transitions():
    cp = MemoryCheckpointer()
    reg = GraphRegistry()
    reg.register("slow", lambda: _slow_graph(cp, delay=0.3))
    q = MemoryRunQueue()
    await q.enqueue("slow", {"n": 0})

    worker = Worker(q, reg, poll_interval=0.01)
    assert worker.busy is False
    task = asyncio.ensure_future(worker.run_once())
    await asyncio.sleep(0.1)
    assert worker.busy is True  # mid-run
    await asyncio.wait_for(task, timeout=2.0)
    assert worker.busy is False  # settled after completion


async def test_worker_pool_health_lifecycle():
    from agentflow import PoolHealth, WorkerPool

    reg = GraphRegistry()
    reg.register("dbl", lambda: _linear_graph(MemoryCheckpointer()))
    q = MemoryRunQueue()

    pool = WorkerPool(q, reg, concurrency=3, poll_interval=0.01)
    # Before start: nothing alive -> not healthy.
    h0 = pool.health()
    assert isinstance(h0, PoolHealth)
    assert h0.workers == 3 and h0.alive == 0 and h0.healthy is False

    await pool.start()
    await asyncio.sleep(0.05)
    h1 = pool.health()
    assert h1.workers == 3 and h1.alive == 3 and h1.healthy is True
    assert h1.idle == 3 and h1.busy == 0

    await pool.stop()
    h2 = pool.health()
    assert h2.alive == 0 and h2.healthy is False


# --- bug 1: a stale worker must stop on lease loss -------------------------


class OwnState(State):
    n: Annotated[int, last]


_lease_side_effect: list[str] = []


def _lease_graph(checkpointer, delay):
    """A single node that sleeps (so the lease can be lost mid-run), then
    performs an observable side effect, then returns an update (which triggers a
    checkpoint write). Both the side effect and the write must be blocked once
    the lease is lost."""
    g = Graph(OwnState)

    async def work(state, ctx):
        await asyncio.sleep(delay)
        _lease_side_effect.append("ran")
        return {"n": 1}

    g.add_node("work", work)
    g.add_edge(START, "work")
    g.add_edge("work", END)
    return g.compile(checkpointer=checkpointer)


async def test_lost_lease_cancels_graph_and_blocks_side_effect():
    """Bug 1: a worker that loses its lease mid-run must (a) stop the graph task
    (no further side effect) and (b) refuse to persist a checkpoint, and it must
    NOT complete the run successfully — the newer attempt owns it.

    Pre-fix failure mode: the heartbeat loop only logged on lease loss, so the
    graph task kept running, the side effect fired, the owner-less checkpoint
    was written, and attempt #1 completed the run SUCCEEDED over the re-claim.
    """
    _lease_side_effect.clear()
    cp = MemoryCheckpointer()
    reg = GraphRegistry()
    reg.register("own", lambda: _lease_graph(cp, delay=0.6))
    q = MemoryRunQueue()
    rec = await q.enqueue("own", {"n": 0})

    # Lease shorter than the heartbeat interval, so the lease expires before
    # attempt 1 renews it — the external re-claim then wins, and attempt 1's
    # first heartbeat (fired after the re-claim) detects the loss.
    worker = Worker(q, reg, poll_interval=0.02, heartbeat_interval=0.2, lease_seconds=0.05)
    task = asyncio.ensure_future(worker.run_once())
    await asyncio.sleep(0.08)  # worker claimed as attempt 1, now in the sleep

    # Externally re-claim: the short lease has expired, claim as attempt 2.
    reclaimed = await q.claim(lease_seconds=60)
    assert reclaimed is not None and reclaimed.attempt == 2

    # Attempt 1's worker must settle without blowing up.
    await asyncio.wait_for(task, timeout=2.0)

    # (a) side effect blocked (cancelled at the await before it ran).
    assert _lease_side_effect == []
    # (b) no checkpoint written by the stale owner.
    assert await cp.get(rec.thread) is None
    # Attempt 1 did NOT complete the run; attempt 2 still owns it (RUNNING).
    now = await q.get(rec.run_id)
    assert now.attempt == 2 and now.status == RunStatus.RUNNING


_child_lease_side_effect: list[str] = []


class ChildLease(State):
    n: Annotated[int, last]


class ParentLease(State):
    n: Annotated[int, last]


def _child_lease_graph(checkpointer, delay):
    g = Graph(ChildLease)

    async def cwork(state, ctx):
        await asyncio.sleep(delay)
        _child_lease_side_effect.append("child-ran")
        return {"n": 1}

    g.add_node("cwork", cwork)
    g.add_edge(START, "cwork")
    g.add_edge("cwork", END)
    return g.compile(checkpointer=checkpointer)


async def test_lost_lease_blocks_child_subgraph_write():
    """Bug 1 (subgraph child): when the lease is lost while a subgraph child is
    mid-super-step, the stale owner must not persist the CHILD sub-thread
    checkpoint (owner_check forwarded through run_subgraph), and the stale
    subgraph worker must land in the worker's lease-lost arm (log + return, NO
    queue.complete, run NOT marked FAILED).

    Pre-fix failure mode: owner_check never reached the child run, and a child
    LeaseLost would have been re-wrapped by _run_node as NodeError and completed
    the run as FAILED.
    """
    _child_lease_side_effect.clear()
    child_cp = MemoryCheckpointer()
    child = _child_lease_graph(child_cp, delay=0.4)

    def _parent():
        g = Graph(ParentLease)
        g.add_subgraph("sub", child, input_map={}, output_map={})
        g.add_edge(START, "sub")
        g.add_edge("sub", END)
        return g.compile(checkpointer=MemoryCheckpointer())

    reg = GraphRegistry()
    reg.register("own", _parent)
    q = MemoryRunQueue()
    rec = await q.enqueue("own", {"n": 0})

    worker = Worker(q, reg, poll_interval=0.02, heartbeat_interval=0.2, lease_seconds=0.05)
    task = asyncio.ensure_future(worker.run_once())
    await asyncio.sleep(0.08)  # claimed as attempt 1, child in its sleep

    reclaimed = await q.claim(lease_seconds=60)
    assert reclaimed is not None and reclaimed.attempt == 2

    await asyncio.wait_for(task, timeout=2.0)

    # Child side effect blocked, child checkpoint not written by the stale owner.
    assert _child_lease_side_effect == []
    assert await child_cp.get(f"{rec.thread}::sub#0") is None
    # Stale worker landed in the lease-lost arm: run NOT FAILED, attempt 2 owns.
    now = await q.get(rec.run_id)
    assert now.status == RunStatus.RUNNING and now.attempt == 2


# --- is_owner unit tests ---------------------------------------------------


async def test_is_owner_true_for_current_running_attempt():
    q = MemoryRunQueue()
    await q.enqueue("g")
    c = await q.claim()
    assert c is not None
    assert await q.is_owner(c.run_id, c.attempt) is True
    # A different owner token is not the owner.
    assert await q.is_owner(c.run_id, c.attempt + 1) is False
    # Unknown run is not owned.
    assert await q.is_owner("missing", 1) is False


async def test_is_owner_false_after_reclaim():
    q = MemoryRunQueue()
    rec = await q.enqueue("g")
    c1 = await q.claim(lease_seconds=0.01)
    assert c1 is not None and c1.attempt == 1
    await asyncio.sleep(0.03)  # lease expires
    c2 = await q.claim(lease_seconds=60)  # re-claim -> attempt 2
    assert c2 is not None and c2.attempt == 2
    # Stale attempt 1 is no longer the owner; attempt 2 is.
    assert await q.is_owner(rec.run_id, 1) is False
    assert await q.is_owner(rec.run_id, 2) is True


async def test_is_owner_false_after_complete():
    q = MemoryRunQueue()
    rec = await q.enqueue("g")
    c = await q.claim()
    assert c is not None
    await q.complete(rec.run_id, c.attempt, status=RunStatus.SUCCEEDED)
    # A completed run is not RUNNING, so nobody owns it.
    assert await q.is_owner(rec.run_id, c.attempt) is False
