# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""Transactional SQLite checkpointer: round-trip, history, resume, overwrite."""

from __future__ import annotations

import sqlite3
from typing import Annotated

from agentflow import (
    END,
    START,
    Graph,
    SqliteCheckpointer,
    State,
    add,
    append,
    last,
)
from agentflow.checkpoint.base import Checkpoint


class Counter(State):
    n: Annotated[int, add]


async def test_put_get_roundtrip(tmp_path):
    cp = SqliteCheckpointer(tmp_path / "cp.db")
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 5}, next=("a",)))
    got = await cp.get("t")
    assert got is not None
    assert got.step == 1 and got.state["n"] == 5 and got.next == ("a",)


async def test_get_specific_step(tmp_path):
    cp = SqliteCheckpointer(tmp_path / "cp.db")
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 1}, next=("a",)))
    await cp.put(Checkpoint(thread="t", step=2, state={"n": 2}, next=()))
    assert (await cp.get("t", 1)).state["n"] == 1
    assert (await cp.get("t")).step == 2  # latest


async def test_history_ordered(tmp_path):
    cp = SqliteCheckpointer(tmp_path / "cp.db")
    for i in range(1, 4):
        await cp.put(Checkpoint(thread="t", step=i, state={"n": i}, next=()))
    steps = [c.step async for c in cp.history("t")]
    assert steps == [1, 2, 3]


async def test_overwrite_bumps_revision(tmp_path):
    dbpath = tmp_path / "cp.db"
    cp = SqliteCheckpointer(dbpath)
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 1}, next=("a",)))
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 9}, next=()))  # re-run same step
    got = await cp.get("t", 1)
    assert got.state["n"] == 9  # last write wins
    # revision incremented to 2
    conn = sqlite3.connect(dbpath)
    try:
        rev = conn.execute(
            "SELECT revision FROM checkpoints WHERE thread='t' AND step=1"
        ).fetchone()[0]
    finally:
        conn.close()
    assert rev == 2


async def test_missing_thread_returns_none(tmp_path):
    cp = SqliteCheckpointer(tmp_path / "cp.db")
    assert await cp.get("nope") is None


async def test_drives_a_real_graph_and_resumes(tmp_path):
    """End-to-end: a checkpointed graph run + HITL interrupt/resume on SQLite."""

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

    cp = SqliteCheckpointer(tmp_path / "cp.db")
    app = g.compile(checkpointer=cp)

    await app.invoke({"stage": []}, thread="hitl")
    state = await app.get_state("hitl")
    assert state.interrupted and state.interrupt_payload == {"q": "ok?"}

    out = await app.resume("hitl", value="yes")
    assert out["answer"] == "yes"
    assert out["stage"] == ["asked", "finished"]
