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

"""PostgresRunQueue contract against a real server (marker: pg).

Mirrors the MemoryRunQueue coverage in tests/test_controlplane.py and adds the
concurrency guarantee that only a real server can prove: a claim under
contention hands each run to exactly one worker (FOR UPDATE SKIP LOCKED).
"""

from __future__ import annotations

import asyncio
from typing import Annotated

import pytest
from agentflow import (
    END,
    START,
    Graph,
    GraphRegistry,
    RunStatus,
    State,
    Worker,
    append,
    last,
)

from conftest import POSTGRES_TEST_DSN, drop_table, unique_table

pytestmark = [
    pytest.mark.pg,
    pytest.mark.skipif(not POSTGRES_TEST_DSN, reason="POSTGRES_TEST_DSN not set"),
]


@pytest.fixture
async def queue():
    from agentflow.controlplane import PostgresRunQueue

    table = unique_table("runs_test")
    q = PostgresRunQueue(POSTGRES_TEST_DSN, table=table)
    try:
        yield q
    finally:
        await q.close()
        await drop_table(POSTGRES_TEST_DSN, table)


# --- queue mechanics -------------------------------------------------------


async def test_enqueue_and_get(queue):
    rec = await queue.enqueue("g", {"n": 5})
    assert rec.status == RunStatus.QUEUED and rec.thread == rec.run_id
    assert (await queue.get(rec.run_id)).input == {"n": 5}


async def test_claim_marks_running_and_is_exclusive(queue):
    await queue.enqueue("g", {"n": 1})
    first = await queue.claim()
    assert first is not None and first.status == RunStatus.RUNNING
    assert await queue.claim() is None  # nothing else runnable


async def test_claim_reclaims_expired_lease(queue):
    await queue.enqueue("g", {"n": 1})
    await queue.claim(lease_seconds=0.01)
    await asyncio.sleep(0.05)
    again = await queue.claim(lease_seconds=60)
    assert again is not None and again.attempt == 2


async def test_complete_and_list_filter(queue):
    r1 = await queue.enqueue("g", {"n": 1})
    await queue.enqueue("h", {"n": 2})
    await queue.claim()
    await queue.complete(r1.run_id, status=RunStatus.SUCCEEDED)
    done = await queue.list(status=RunStatus.SUCCEEDED)
    assert [r.run_id for r in done] == [r1.run_id]
    assert len(await queue.list(graph="h")) == 1


async def test_request_cancel_queued_is_immediate(queue):
    rec = await queue.enqueue("g", {"n": 1})
    assert await queue.request_cancel(rec.run_id) is True
    assert (await queue.get(rec.run_id)).status == RunStatus.CANCELLED


async def test_stats_snapshot(queue):
    await queue.enqueue("g", {"n": 1})
    await queue.enqueue("g", {"n": 2})
    await queue.claim()
    stats = await queue.stats()
    assert stats.queued == 1
    assert stats.running == 1


# --- the real-server concurrency guarantee ---------------------------------


async def test_concurrent_claim_hands_each_run_to_one_worker(queue):
    # Enqueue N runs, fire 2N concurrent claims. Each run must be claimed by
    # exactly one claimer; no run is handed out twice (FOR UPDATE SKIP LOCKED).
    n = 8
    for i in range(n):
        await queue.enqueue("g", {"i": i})

    async def claim_one():
        rec = await queue.claim(lease_seconds=60)
        return rec.run_id if rec else None

    results = await asyncio.gather(*[claim_one() for _ in range(2 * n)])
    claimed = [r for r in results if r is not None]
    assert len(claimed) == n  # exactly the N runs were claimed
    assert len(set(claimed)) == n  # no run claimed twice


# --- Worker end-to-end over Postgres ---------------------------------------


class Doubler(State):
    n: Annotated[int, last]


class HitlState(State):
    answer: Annotated[str, last]
    stage: Annotated[list, append]


async def test_worker_runs_to_success(queue):
    from agentflow.checkpoint import PostgresCheckpointer

    ckpt_table = unique_table("runs_ckpt")
    cp = PostgresCheckpointer(POSTGRES_TEST_DSN, table=ckpt_table)

    def build():
        g = Graph(Doubler)

        async def double(state, ctx):
            return {"n": state["n"] * 2}

        g.add_node("double", double)
        g.add_edge(START, "double")
        g.add_edge("double", END)
        return g.compile(checkpointer=cp)

    reg = GraphRegistry()
    reg.register("dbl", build)
    try:
        rec = await queue.enqueue("dbl", {"n": 21})
        worker = Worker(queue, reg, poll_interval=0.01)
        assert await worker.run_once() is True
        assert (await queue.get(rec.run_id)).status == RunStatus.SUCCEEDED
        assert (await cp.get(rec.run_id)).state["n"] == 42
    finally:
        await cp.close()
        await drop_table(POSTGRES_TEST_DSN, ckpt_table)


async def test_worker_interrupt_then_resume(queue):
    from agentflow.checkpoint import PostgresCheckpointer

    ckpt_table = unique_table("runs_ckpt")
    cp = PostgresCheckpointer(POSTGRES_TEST_DSN, table=ckpt_table)

    def build():
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
        return g.compile(checkpointer=cp)

    reg = GraphRegistry()
    reg.register("hitl", build)
    try:
        rec = await queue.enqueue("hitl", {"stage": []})
        worker = Worker(queue, reg, poll_interval=0.01)
        await worker.run_once()
        assert (await queue.get(rec.run_id)).status == RunStatus.INTERRUPTED

        await queue.enqueue_resume(rec.run_id, value="yes")
        assert (await queue.get(rec.run_id)).status == RunStatus.QUEUED
        await worker.run_once()

        assert (await queue.get(rec.run_id)).status == RunStatus.SUCCEEDED
        state = await cp.get(rec.run_id)
        assert state.state["answer"] == "yes"
        assert state.state["stage"] == ["asked", "finished"]
    finally:
        await cp.close()
        await drop_table(POSTGRES_TEST_DSN, ckpt_table)
