"""Observability hooks + metrics."""

from __future__ import annotations

from typing import Annotated

import pytest

from agentflow import END, START, Graph, Hooks, RunMetrics, State, add


class S(State):
    n: Annotated[int, add]


def _linear_graph(hooks=None):
    g = Graph(S)

    async def a(state, ctx):
        return {"n": 1}

    async def b(state, ctx):
        return {"n": 2}

    g.add_node("a", a)
    g.add_node("b", b)
    g.add_edge(START, "a")
    g.add_edge("a", "b")
    g.add_edge("b", END)
    return g.compile(hooks=hooks)


async def test_metrics_collect_node_and_step_counts():
    m = RunMetrics()
    out = await _linear_graph(hooks=m).invoke({"n": 0})
    assert out["n"] == 3
    s = m.summary()
    assert s["completed"] is True
    assert s["steps"] == 2                 # two super-steps (a, then b)
    assert s["nodes"]["a"]["calls"] == 1
    assert s["nodes"]["b"]["calls"] == 1
    assert s["nodes"]["a"]["errors"] == 0


async def test_custom_hooks_receive_lifecycle_callbacks():
    events = []

    class Recorder(Hooks):
        async def on_run_start(self, thread, step):
            events.append(("run_start", thread))

        async def on_node_start(self, thread, step, node):
            events.append(("node_start", node))

        async def on_node_end(self, thread, step, node, seconds):
            events.append(("node_end", node))

        async def on_run_end(self, thread, step, completed):
            events.append(("run_end", completed))

    await _linear_graph(hooks=Recorder()).invoke({"n": 0}, thread="tt")

    assert ("run_start", "tt") in events
    assert ("node_start", "a") in events
    assert ("node_end", "a") in events
    assert ("node_start", "b") in events
    assert ("run_end", True) in events
    # order: run_start before any node, run_end last
    assert events[0] == ("run_start", "tt")
    assert events[-1] == ("run_end", True)


async def test_hooks_error_never_breaks_run():
    class Boom(Hooks):
        async def on_node_start(self, thread, step, node):
            raise RuntimeError("hook blew up")

    # The run must complete despite the hook raising.
    out = await _linear_graph(hooks=Boom()).invoke({"n": 0})
    assert out["n"] == 3


async def test_metrics_count_errors():
    m = RunMetrics()
    g = Graph(S)

    async def boom(state, ctx):
        raise ValueError("x")

    g.add_node("boom", boom)
    g.add_edge(START, "boom")
    g.add_edge("boom", END)
    app = g.compile(hooks=m)
    with pytest.raises(Exception):
        await app.invoke({"n": 0})
    assert m.summary()["nodes"]["boom"]["errors"] == 1
    assert m.completed is False
