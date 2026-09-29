# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""Graph engine tests: linear flow, conditional loop, fan-out with reducers,
checkpointing, HITL interrupt/resume, and the step-limit guard."""

from __future__ import annotations

from typing import Annotated

import pytest
from agentflow import (
    END,
    START,
    CompilationError,
    Graph,
    MemoryCheckpointer,
    NodeError,
    State,
    add,
    append,
    last,
)


class Counter(State):
    n: Annotated[int, add]
    log: Annotated[list, append]


async def test_linear_flow():
    g = Graph(Counter)

    async def a(state, ctx):
        return {"n": 1, "log": "a"}

    async def b(state, ctx):
        return {"n": 10, "log": "b"}

    g.add_node("a", a)
    g.add_node("b", b)
    g.add_edge(START, "a")
    g.add_edge("a", "b")
    g.add_edge("b", END)

    app = g.compile()
    out = await app.invoke({"n": 0, "log": []})
    assert out["n"] == 11
    assert out["log"] == ["a", "b"]


async def test_conditional_loop():
    g = Graph(Counter)

    async def inc(state, ctx):
        return {"n": 1, "log": f"inc->{state.get('n', 0) + 1}"}

    def route(state):
        return "loop" if state["n"] < 3 else "done"

    g.add_node("inc", inc)
    g.add_edge(START, "inc")
    g.add_conditional_edges("inc", route, {"loop": "inc", "done": END})

    app = g.compile()
    out = await app.invoke({"n": 0, "log": []})
    assert out["n"] == 3
    assert len(out["log"]) == 3


async def test_fanout_reducer_merge():
    """Two nodes run in one super-step; add + append fold both updates."""
    g = Graph(Counter)

    async def start(state, ctx):
        return {"log": "start"}

    async def x(state, ctx):
        return {"n": 2, "log": "x"}

    async def y(state, ctx):
        return {"n": 3, "log": "y"}

    async def join(state, ctx):
        return {"log": "join"}

    g.add_node("start", start)
    g.add_node("x", x)
    g.add_node("y", y)
    g.add_node("join", join)
    g.add_edge(START, "start")
    # fan-out: start routes to both x and y
    g.add_conditional_edges("start", lambda s: ["fx", "fy"], {"fx": "x", "fy": "y"})
    g.add_edge("x", "join")
    g.add_edge("y", "join")
    g.add_edge("join", END)

    app = g.compile()
    out = await app.invoke({"n": 0, "log": []})
    assert out["n"] == 5  # 2 + 3 folded via add
    # x and y both ran in the same super-step; order is registration order
    assert out["log"] == ["start", "x", "y", "join"]


async def test_checkpointing_records_every_step():
    g = Graph(Counter)

    async def inc(state, ctx):
        return {"n": 1}

    def route(state):
        return "loop" if state["n"] < 2 else "done"

    g.add_node("inc", inc)
    g.add_edge(START, "inc")
    g.add_conditional_edges("inc", route, {"loop": "inc", "done": END})

    cp = MemoryCheckpointer()
    app = g.compile(checkpointer=cp)
    await app.invoke({"n": 0}, thread="t1")

    steps = [c async for c in app.history("t1")]
    assert [c.step for c in steps] == [1, 2]
    assert steps[-1].state["n"] == 2
    assert steps[-1].next == ()  # terminal


async def test_interrupt_and_resume():
    class S(State):
        answer: Annotated[str, last]
        stage: Annotated[list, append]

    g = Graph(S)

    async def ask(state, ctx):
        human = await ctx.interrupt({"question": "approve?"})
        return {"answer": human, "stage": "asked"}

    async def finish(state, ctx):
        return {"stage": "finished"}

    g.add_node("ask", ask)
    g.add_node("finish", finish)
    g.add_edge(START, "ask")
    g.add_edge("ask", "finish")
    g.add_edge("finish", END)

    cp = MemoryCheckpointer()
    app = g.compile(checkpointer=cp)

    # First pass suspends at the interrupt.
    await app.invoke({"stage": []}, thread="hitl")
    state = await app.get_state("hitl")
    assert state.interrupted
    assert state.interrupt_node == "ask"
    assert state.interrupt_payload == {"question": "approve?"}
    assert not state.done

    # Resume with the human's answer.
    out = await app.resume("hitl", value="yes")
    assert out["answer"] == "yes"
    assert out["stage"] == ["asked", "finished"]


async def test_step_limit_guard():
    g = Graph(Counter)

    async def spin(state, ctx):
        return {"n": 1}

    g.add_node("spin", spin)
    g.add_edge(START, "spin")
    g.add_conditional_edges("spin", lambda s: "loop", {"loop": "spin"})

    app = g.compile(step_limit=5)
    with pytest.raises(NodeError):
        await app.invoke({"n": 0})


async def test_stream_events():
    g = Graph(Counter)

    async def a(state, ctx):
        return {"n": 1}

    g.add_node("a", a)
    g.add_edge(START, "a")
    g.add_edge("a", END)

    app = g.compile()
    kinds = [ev.kind async for ev in app.stream({"n": 0})]
    assert "node_start" in kinds
    assert "node_end" in kinds
    assert kinds[-1] == "done"


# ------------------- compilation validation -------------------


def test_compile_rejects_missing_start_edge():
    g = Graph(Counter)
    g.add_node("a", lambda s, c: {})
    g.add_edge("a", END)
    with pytest.raises(CompilationError):
        g.compile()


def test_compile_rejects_unknown_target():
    g = Graph(Counter)
    g.add_node("a", lambda s, c: {})
    g.add_edge(START, "a")
    g.add_edge("a", "ghost")
    with pytest.raises(CompilationError):
        g.compile()


def test_compile_rejects_unreachable_node():
    g = Graph(Counter)
    g.add_node("a", lambda s, c: {})
    g.add_node("orphan", lambda s, c: {})
    g.add_edge(START, "a")
    g.add_edge("a", END)
    g.add_edge("orphan", END)
    with pytest.raises(CompilationError):
        g.compile()


def test_compile_rejects_dead_end():
    g = Graph(Counter)
    g.add_node("a", lambda s, c: {})
    g.add_edge(START, "a")
    # a has no outgoing edge
    with pytest.raises(CompilationError):
        g.compile()
