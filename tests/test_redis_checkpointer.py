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

"""RedisCheckpointer tests via fakeredis (offline) — mirrors the sqlite suite."""

from __future__ import annotations

from typing import Annotated

import pytest

fakeredis = pytest.importorskip("fakeredis")

from agentflow import END, START, Graph, State, append, last  # noqa: E402
from agentflow.checkpoint.base import Checkpoint  # noqa: E402
from agentflow.checkpoint.redis import RedisCheckpointer  # noqa: E402


def _checkpointer():
    # fakeredis async server, decode_responses to match our client config.
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    return RedisCheckpointer(client=client)


async def test_put_get_roundtrip():
    cp = _checkpointer()
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 5}, next=("a",)))
    got = await cp.get("t")
    assert got is not None
    assert got.step == 1 and got.state["n"] == 5 and got.next == ("a",)


async def test_get_specific_step():
    cp = _checkpointer()
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 1}, next=("a",)))
    await cp.put(Checkpoint(thread="t", step=2, state={"n": 2}, next=()))
    assert (await cp.get("t", 1)).state["n"] == 1
    assert (await cp.get("t")).step == 2  # latest


async def test_history_ordered():
    cp = _checkpointer()
    for i in (3, 1, 2):  # insert out of order; history must sort ascending
        await cp.put(Checkpoint(thread="t", step=i, state={"n": i}, next=()))
    steps = [c.step async for c in cp.history("t")]
    assert steps == [1, 2, 3]


async def test_overwrite_same_step():
    cp = _checkpointer()
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 1}, next=("a",)))
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 9}, next=()))
    got = await cp.get("t", 1)
    assert got.state["n"] == 9  # last write wins
    # index still has exactly one entry for the step
    steps = [c.step async for c in cp.history("t")]
    assert steps == [1]


async def test_missing_thread_returns_none():
    cp = _checkpointer()
    assert await cp.get("nope") is None


async def test_drives_graph_and_resumes():
    class S(State):
        answer: Annotated[str, last]
        stage: Annotated[list, append]

    g = Graph(S)

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

    cp = _checkpointer()
    app = g.compile(checkpointer=cp)

    await app.invoke({"stage": []}, thread="hitl")
    state = await app.get_state("hitl")
    assert state.interrupted and state.interrupt_payload == {"q": "ok?"}

    out = await app.resume("hitl", value="yes")
    assert out["answer"] == "yes"
    assert out["stage"] == ["asked", "finished"]


async def test_redaction_masks_state():
    from agentflow import RedactKeys

    cp = RedisCheckpointer(
        client=fakeredis.aioredis.FakeRedis(decode_responses=True),
        redact=RedactKeys(),
    )
    await cp.put(
        Checkpoint(thread="t", step=1, state={"api_key": "sk-SECRET", "note": "keep"}, next=())
    )
    got = await cp.get("t", 1)
    assert got.state["api_key"] == "***REDACTED***"
    assert got.state["note"] == "keep"


async def test_revision_tracking():
    cp = _checkpointer()
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 1}))
    assert (await cp.get("t", 1)).revision == 1
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 2}))
    got = await cp.get("t", 1)
    assert got.revision == 2 and got.state["n"] == 2


async def test_if_revision_match_and_conflict():
    from agentflow.errors import CheckpointConflict

    cp = _checkpointer()
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 1}))
    rev = (await cp.get("t", 1)).revision
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 2}), if_revision=rev)
    assert (await cp.get("t", 1)).state["n"] == 2
    with pytest.raises(CheckpointConflict):
        await cp.put(Checkpoint(thread="t", step=1, state={"n": 3}), if_revision=rev)
    assert (await cp.get("t", 1)).state["n"] == 2  # loser did not apply


async def test_delete_thread():
    cp = _checkpointer()
    for i in range(1, 4):
        await cp.put(Checkpoint(thread="t", step=i, state={"n": i}))
    await cp.delete_thread("t")
    assert await cp.get("t") is None
    assert [c async for c in cp.history("t")] == []


async def test_prune_before_step():
    cp = _checkpointer()
    for i in range(1, 6):
        await cp.put(Checkpoint(thread="t", step=i, state={"n": i}))
    removed = await cp.prune("t", before_step=3)
    assert removed == 2
    assert [c.step async for c in cp.history("t")] == [3, 4, 5]


async def test_prune_older_than():
    cp = _checkpointer()
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 1}, ts="2020-01-01T00:00:00"))
    await cp.put(Checkpoint(thread="t", step=2, state={"n": 2}, ts="2030-01-01T00:00:00"))
    removed = await cp.prune("t", older_than="2025-01-01T00:00:00")
    assert removed == 1
    assert [c.step async for c in cp.history("t")] == [2]


async def test_list_threads():
    from agentflow import ThreadInfo

    cp = _checkpointer()
    await cp.put(Checkpoint(thread="a", step=1, state={"n": 1}, next=("x",)))
    await cp.put(Checkpoint(thread="a", step=2, state={"n": 2}, next=()))
    await cp.put(Checkpoint(thread="b", step=1, state={"n": 1}, interrupted=True, next=("z",)))
    infos = await cp.list_threads()
    assert all(isinstance(i, ThreadInfo) for i in infos)
    by = {i.thread: i for i in infos}
    assert set(by) == {"a", "b"}
    assert by["a"].latest_step == 2 and by["a"].done is True
    assert by["b"].interrupted is True
    # delete removes from listing
    await cp.delete_thread("a")
    assert [i.thread for i in await cp.list_threads()] == ["b"]
