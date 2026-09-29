"""Synchronous facade: *_sync helpers wrap asyncio.run, guarded vs a live loop."""

from __future__ import annotations

from typing import Annotated

import pytest
from agentflow import END, START, Graph, MemoryCheckpointer, State, add, append, last


class S(State):
    n: Annotated[int, add]
    log: Annotated[list, append]


def _app(**kw):
    g = Graph(S)
    g.add_node("a", lambda s, c: {"n": 1, "log": "a"})
    g.add_node("b", lambda s, c: {"n": 2, "log": "b"})
    g.add_edge(START, "a")
    g.add_edge("a", "b")
    g.add_edge("b", END)
    return g.compile(**kw)


def test_invoke_sync_from_sync_code():
    # A plain (non-async) test function driving the graph synchronously.
    out = _app().invoke_sync({"n": 0, "log": []})
    assert out["n"] == 3
    assert out["log"] == ["a", "b"]


def test_stream_sync_returns_events():
    events = _app().stream_sync({"n": 0, "log": []})
    kinds = [e.kind for e in events]
    assert "node_end" in kinds
    assert kinds[-1] == "done"


def test_resume_sync_roundtrip():
    class H(State):
        answer: Annotated[str, last]

    g = Graph(H)

    async def ask(state, ctx):
        v = await ctx.interrupt({"q": "?"})
        return {"answer": v}

    g.add_node("ask", ask)
    g.add_edge(START, "ask")
    g.add_edge("ask", END)
    app = g.compile(checkpointer=MemoryCheckpointer())

    app.invoke_sync({}, thread="t")
    out = app.resume_sync("t", value="yes")
    assert out["answer"] == "yes"


async def test_sync_helper_refuses_inside_running_loop():
    # We're already inside an event loop (async test) — the sync helper must
    # refuse rather than deadlock.
    with pytest.raises(RuntimeError, match="running event loop"):
        _app().invoke_sync({"n": 0, "log": []})
