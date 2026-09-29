"""Whole-run timeout: fires cleanly, preserves the last checkpoint, resumable."""

from __future__ import annotations

import asyncio
from typing import Annotated

import pytest

from agentflow import (
    END, START, Graph, State, add, MemoryCheckpointer, RunTimeout,
)


class Counter(State):
    n: Annotated[int, add]


def _slow_loop_graph(per_step_delay: float):
    g = Graph(Counter)

    async def tick(state, ctx):
        await asyncio.sleep(per_step_delay)
        return {"n": 1}

    g.add_node("tick", tick)
    g.add_edge(START, "tick")
    g.add_conditional_edges("tick", lambda s: "again", {"again": "tick"})  # loops forever
    return g


async def test_no_timeout_by_default_completes():
    # A finite graph with no timeout runs to completion.
    g = Graph(Counter)

    async def once(state, ctx):
        return {"n": 5}

    g.add_node("once", once)
    g.add_edge(START, "once")
    g.add_edge("once", END)
    out = await g.compile().invoke({"n": 0})
    assert out["n"] == 5


async def test_timeout_fires_on_runaway():
    app = _slow_loop_graph(0.05).compile(step_limit=10_000)
    with pytest.raises(RunTimeout) as ei:
        await app.invoke({"n": 0}, timeout=0.2)
    assert ei.value.thread == "default"
    assert ei.value.seconds == 0.2


async def test_timeout_preserves_last_checkpoint_and_resumes():
    cp = MemoryCheckpointer()
    # Loops a bounded number of times but slowly, so a timeout hits mid-run.
    g = Graph(Counter)

    async def tick(state, ctx):
        await asyncio.sleep(0.05)
        return {"n": 1}

    g.add_node("tick", tick)
    g.add_edge(START, "tick")
    g.add_conditional_edges("tick", lambda s: "again" if s["n"] < 100 else "done",
                            {"again": "tick", "done": END})
    app = g.compile(checkpointer=cp, step_limit=10_000)

    with pytest.raises(RunTimeout) as ei:
        await app.invoke({"n": 0}, thread="t", timeout=0.22)

    # Some steps completed and were checkpointed before the timeout.
    last = await app.get_state("t")
    assert last is not None
    n_at_timeout = last.state["n"]
    assert n_at_timeout >= 1
    assert ei.value.step == last.step

    # The run is resumable from the last checkpoint (give it enough time now).
    out = await app.resume("t")
    assert out["n"] == 100
