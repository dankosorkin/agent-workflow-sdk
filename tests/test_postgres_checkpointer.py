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

"""PostgresCheckpointer contract against a real server (marker: pg).

Mirrors tests/test_redis_checkpointer.py but exercises the real transactional
compare-and-set (SELECT ... FOR UPDATE + ON CONFLICT revision bump) that only
runs against Postgres.
"""

from __future__ import annotations

from typing import Annotated

import pytest
from agentflow import END, START, Graph, State, ThreadInfo, append, last
from agentflow.checkpoint.base import Checkpoint
from agentflow.errors import CheckpointConflict

from conftest import POSTGRES_TEST_DSN, drop_table, unique_table

pytestmark = [
    pytest.mark.pg,
    pytest.mark.skipif(not POSTGRES_TEST_DSN, reason="POSTGRES_TEST_DSN not set"),
]


@pytest.fixture
async def cp():
    from agentflow.checkpoint import PostgresCheckpointer

    table = unique_table("ckpt_test")
    c = PostgresCheckpointer(POSTGRES_TEST_DSN, table=table)
    try:
        yield c
    finally:
        await c.close()
        await drop_table(POSTGRES_TEST_DSN, table)


async def test_put_get_roundtrip(cp):
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 5}, next=("a",)))
    got = await cp.get("t")
    assert got is not None
    assert got.step == 1 and got.state["n"] == 5 and got.next == ("a",)


async def test_get_specific_step(cp):
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 1}, next=("a",)))
    await cp.put(Checkpoint(thread="t", step=2, state={"n": 2}, next=()))
    assert (await cp.get("t", 1)).state["n"] == 1
    assert (await cp.get("t")).step == 2


async def test_history_ordered(cp):
    for i in (3, 1, 2):
        await cp.put(Checkpoint(thread="t", step=i, state={"n": i}, next=()))
    steps = [c.step async for c in cp.history("t")]
    assert steps == [1, 2, 3]


async def test_revision_tracking(cp):
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 1}))
    assert (await cp.get("t", 1)).revision == 1
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 2}))
    got = await cp.get("t", 1)
    assert got.revision == 2 and got.state["n"] == 2


async def test_if_revision_match_and_conflict(cp):
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 1}))
    rev = (await cp.get("t", 1)).revision
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 2}), if_revision=rev)
    assert (await cp.get("t", 1)).state["n"] == 2
    with pytest.raises(CheckpointConflict):
        await cp.put(Checkpoint(thread="t", step=1, state={"n": 3}), if_revision=rev)
    assert (await cp.get("t", 1)).state["n"] == 2  # loser did not apply


async def test_missing_thread_returns_none(cp):
    assert await cp.get("nope") is None


async def test_delete_thread(cp):
    for i in range(1, 4):
        await cp.put(Checkpoint(thread="t", step=i, state={"n": i}))
    await cp.delete_thread("t")
    assert await cp.get("t") is None
    assert [c async for c in cp.history("t")] == []


async def test_prune_before_step(cp):
    for i in range(1, 6):
        await cp.put(Checkpoint(thread="t", step=i, state={"n": i}))
    removed = await cp.prune("t", before_step=3)
    assert removed == 2
    assert [c.step async for c in cp.history("t")] == [3, 4, 5]


async def test_list_threads(cp):
    await cp.put(Checkpoint(thread="a", step=1, state={"n": 1}, next=("x",)))
    await cp.put(Checkpoint(thread="a", step=2, state={"n": 2}, next=()))
    await cp.put(Checkpoint(thread="b", step=1, state={"n": 1}, interrupted=True, next=("z",)))
    infos = await cp.list_threads()
    assert all(isinstance(i, ThreadInfo) for i in infos)
    by = {i.thread: i for i in infos}
    assert set(by) == {"a", "b"}
    assert by["a"].latest_step == 2 and by["a"].done is True
    assert by["b"].interrupted is True


async def test_drives_graph_and_resumes(cp):
    class S(State):
        answer: Annotated[str, last]
        stage: Annotated[list, append]

    async def ask(state, ctx):
        human = await ctx.interrupt({"q": "ok?"})
        return {"answer": human, "stage": "asked"}

    async def finish(state, ctx):
        return {"stage": "finished"}

    g = Graph(S)
    g.add_node("ask", ask)
    g.add_node("finish", finish)
    g.add_edge(START, "ask")
    g.add_edge("ask", "finish")
    g.add_edge("finish", END)
    app = g.compile(checkpointer=cp)

    await app.invoke({"stage": []}, thread="hitl")
    state = await app.get_state("hitl")
    assert state.interrupted and state.interrupt_payload == {"q": "ok?"}

    out = await app.resume("hitl", value="yes")
    assert out["answer"] == "yes"
    assert out["stage"] == ["asked", "finished"]


async def test_redaction_masks_state():
    from agentflow import RedactKeys
    from agentflow.checkpoint import PostgresCheckpointer

    table = unique_table("ckpt_redact")
    c = PostgresCheckpointer(POSTGRES_TEST_DSN, table=table, redact=RedactKeys())
    try:
        await c.put(
            Checkpoint(thread="t", step=1, state={"api_key": "sk-SECRET", "note": "keep"}, next=())
        )
        got = await c.get("t", 1)
        assert got.state["api_key"] == "***REDACTED***"
        assert got.state["note"] == "keep"
    finally:
        await c.close()
        await drop_table(POSTGRES_TEST_DSN, table)
