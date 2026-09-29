# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""PrometheusHooks: run/node/step counters and duration histogram."""

from __future__ import annotations

from typing import Annotated

import pytest
from agentflow import END, START, Graph, State, add

# Skip the whole module if the prometheus extra isn't installed.
pytest.importorskip("prometheus_client")

from agentflow.events import TextChunk  # noqa: E402
from agentflow.prometheus import PrometheusHooks  # noqa: E402


class S(State):
    n: Annotated[int, add]


def _value(hooks, name, labels=None):
    """Read a single metric sample value from the hooks' private registry."""
    return hooks.registry.get_sample_value(name, labels or {})


async def test_counters_and_histogram_on_success():
    hooks = PrometheusHooks()

    g = Graph(S)
    g.add_node("a", lambda s, c: {"n": 1})
    g.add_node("b", lambda s, c: {"n": 2})
    g.add_edge(START, "a")
    g.add_edge("a", "b")
    g.add_edge("b", END)
    await g.compile(hooks=hooks).invoke({"n": 0})

    assert _value(hooks, "agentflow_runs_started_total") == 1.0
    assert _value(hooks, "agentflow_runs_completed_total", {"outcome": "completed"}) == 1.0
    assert _value(hooks, "agentflow_runs_in_progress") == 0.0
    assert _value(hooks, "agentflow_node_runs_total", {"node": "a", "outcome": "ok"}) == 1.0
    assert _value(hooks, "agentflow_node_runs_total", {"node": "b", "outcome": "ok"}) == 1.0
    # histogram count for node a
    assert _value(hooks, "agentflow_node_duration_seconds_count", {"node": "a"}) == 1.0
    # at least one step ran
    assert _value(hooks, "agentflow_steps_total") >= 1.0


async def test_node_error_counter_and_incomplete_run():
    hooks = PrometheusHooks()

    g = Graph(S)

    async def boom(state, ctx):
        raise ValueError("kaboom")

    g.add_node("boom", boom)
    g.add_edge(START, "boom")
    g.add_edge("boom", END)

    from agentflow import NodeError

    with pytest.raises(NodeError):
        await g.compile(hooks=hooks).invoke({"n": 0})

    assert _value(hooks, "agentflow_node_runs_total", {"node": "boom", "outcome": "error"}) == 1.0
    assert _value(hooks, "agentflow_runs_completed_total", {"outcome": "incomplete"}) == 1.0
    assert _value(hooks, "agentflow_runs_in_progress") == 0.0


async def test_backend_event_counter():
    hooks = PrometheusHooks()

    g = Graph(S)

    async def talk(state, ctx):
        ctx.emit(TextChunk(text="hi"))
        return {"n": 1}

    g.add_node("talk", talk)
    g.add_edge(START, "talk")
    g.add_edge("talk", END)
    await g.compile(hooks=hooks).invoke({"n": 0})

    import asyncio

    await asyncio.sleep(0.05)  # let fire-and-forget on_event flush

    assert _value(hooks, "agentflow_backend_events_total", {"event_type": "TextChunk"}) == 1.0


def test_exposition_renders_text_format():
    hooks = PrometheusHooks()
    body, content_type = hooks.exposition()
    assert isinstance(body, bytes)
    assert content_type.startswith("text/plain")
    # metric names appear in the HELP/TYPE preamble even before any run
    text = body.decode("utf-8")
    assert "agentflow_runs_started_total" in text
    assert "agentflow_node_duration_seconds" in text


def test_private_registries_are_isolated():
    a = PrometheusHooks()
    b = PrometheusHooks()
    assert a.registry is not b.registry  # no shared/global collision


def test_shared_registry_accepted():
    from prometheus_client import CollectorRegistry

    reg = CollectorRegistry()
    hooks = PrometheusHooks(registry=reg)
    assert hooks.registry is reg
    body, _ = hooks.exposition()
    assert b"agentflow_steps_total" in body
