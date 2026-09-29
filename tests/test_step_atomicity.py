"""All-or-nothing super-step semantics.

If any node in a super-step raises, the whole step is discarded before its
updates are applied: siblings' updates do not land, the state remains at the
last committed checkpoint, and (with a checkpointer) the run is resumable.
"""

from __future__ import annotations

from typing import Annotated

import pytest

from agentflow import (
    END, START, Graph, State, add, append, MemoryCheckpointer, NodeError,
)


class S(State):
    total: Annotated[int, add]
    log: Annotated[list, append]


async def test_partial_failure_discards_whole_step():
    """Two siblings run in one step; one succeeds, one raises. The successful
    sibling's update must NOT be applied."""
    g = Graph(S)

    async def start(state, ctx):
        return {"log": "start"}

    async def good(state, ctx):
        return {"total": 100, "log": "good"}

    async def bad(state, ctx):
        raise RuntimeError("kaboom")

    g.add_node("start", start)
    g.add_node("good", good)
    g.add_node("bad", bad)
    g.add_node("join", lambda s, c: {})
    g.add_edge(START, "start")
    g.add_conditional_edges("start", lambda s: ["good", "bad"],
                            {"good": "good", "bad": "bad"})
    g.add_edge("good", "join")
    g.add_edge("bad", "join")
    g.add_edge("join", END)

    cp = MemoryCheckpointer()
    app = g.compile(checkpointer=cp)

    with pytest.raises(NodeError):
        await app.invoke({"total": 0, "log": []}, thread="t")

    # The last committed checkpoint is the one AFTER 'start', BEFORE the failed
    # fan-out step. 'good's +100 must not be there.
    last = await app.get_state("t")
    assert last is not None
    assert last.state["total"] == 0            # good's update discarded
    assert last.state["log"] == ["start"]      # only the start step committed
    assert "good" not in last.state["log"]


async def test_failed_step_is_not_checkpointed():
    """A super-step that raises writes no checkpoint for that step; history
    ends at the last successful step."""
    g = Graph(S)

    async def ok(state, ctx):
        return {"total": 1}

    async def boom(state, ctx):
        raise ValueError("x")

    g.add_node("ok", ok)
    g.add_node("boom", boom)
    g.add_edge(START, "ok")
    g.add_edge("ok", "boom")
    g.add_edge("boom", END)

    cp = MemoryCheckpointer()
    app = g.compile(checkpointer=cp)
    with pytest.raises(NodeError):
        await app.invoke({"total": 0}, thread="t2")

    steps = [c async for c in app.history("t2")]
    # Only step 1 ('ok') committed; the failing 'boom' step (step 2) did not.
    assert [c.step for c in steps] == [1]
    assert steps[0].state["total"] == 1
