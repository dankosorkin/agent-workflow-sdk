# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""Persistent JSONL telemetry + MultiHooks composition."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Annotated

import pytest
from agentflow import (
    END,
    START,
    Graph,
    JsonlTelemetry,
    MultiHooks,
    NodeError,
    RunMetrics,
    State,
    add,
)
from agentflow.events import TextChunk


class S(State):
    n: Annotated[int, add]


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


async def test_jsonl_records_run_lifecycle(tmp_path):
    tel = JsonlTelemetry(tmp_path)  # directory -> per-thread files

    g = Graph(S)
    g.add_node("a", lambda s, c: {"n": 1})
    g.add_node("b", lambda s, c: {"n": 2})
    g.add_edge(START, "a")
    g.add_edge("a", "b")
    g.add_edge("b", END)
    await g.compile(hooks=tel).invoke({"n": 0}, thread="run1")

    events = _read_jsonl(tmp_path / "run1.jsonl")
    kinds = [e["event"] for e in events]
    assert kinds[0] == "run_start"
    assert kinds[-1] == "run_end"
    assert "node_start" in kinds and "node_end" in kinds and "step" in kinds
    assert events[-1]["completed"] is True
    # node_end carries a duration
    node_ends = [e for e in events if e["event"] == "node_end"]
    assert all("seconds" in e for e in node_ends)
    assert {e["node"] for e in node_ends} == {"a", "b"}


async def test_jsonl_single_file_mode(tmp_path):
    f = tmp_path / "trace.jsonl"
    tel = JsonlTelemetry(f)
    g = Graph(S)
    g.add_node("a", lambda s, c: {"n": 1})
    g.add_edge(START, "a")
    g.add_edge("a", END)
    await g.compile(hooks=tel).invoke({"n": 0}, thread="x")
    assert f.exists()
    events = _read_jsonl(f)
    assert events[0]["event"] == "run_start"


async def test_jsonl_records_node_error(tmp_path):
    tel = JsonlTelemetry(tmp_path)
    g = Graph(S)

    async def boom(state, ctx):
        raise ValueError("nope")

    g.add_node("boom", boom)
    g.add_edge(START, "boom")
    g.add_edge("boom", END)
    with pytest.raises(NodeError):
        await g.compile(hooks=tel).invoke({"n": 0}, thread="err")

    events = _read_jsonl(tmp_path / "err.jsonl")
    kinds = [e["event"] for e in events]
    assert "node_error" in kinds
    err = [e for e in events if e["event"] == "node_error"][0]
    assert "ValueError" in err["error"]
    assert events[-1]["event"] == "run_end" and events[-1]["completed"] is False


async def test_backend_events_recorded_via_emit(tmp_path):
    tel = JsonlTelemetry(tmp_path)
    g = Graph(S)

    async def talk(state, ctx):
        ctx.emit(TextChunk(text="hello"))
        ctx.emit(TextChunk(text=" world"))
        return {"n": 1}

    g.add_node("talk", talk)
    g.add_edge(START, "talk")
    g.add_edge("talk", END)
    await g.compile(hooks=tel).invoke({"n": 0}, thread="evt")

    # on_event is scheduled as a fire-and-forget task; let those tasks run.
    await asyncio.sleep(0.05)

    events = _read_jsonl(tmp_path / "evt.jsonl")
    emit_events = [e for e in events if e["event"] == "event"]
    assert len(emit_events) == 2
    assert all(e["eventType"] == "TextChunk" for e in emit_events)
    assert emit_events[0]["payload"]["text"] == "hello"


async def test_multihooks_fans_out(tmp_path):
    tel = JsonlTelemetry(tmp_path)
    metrics = RunMetrics()
    hooks = MultiHooks(metrics, tel)

    g = Graph(S)
    g.add_node("a", lambda s, c: {"n": 1})
    g.add_edge(START, "a")
    g.add_edge("a", END)
    await g.compile(hooks=hooks).invoke({"n": 0}, thread="multi")

    # metrics collected
    assert metrics.completed is True
    assert metrics.nodes["a"].calls == 1
    # telemetry written
    events = _read_jsonl(tmp_path / "multi.jsonl")
    assert events[0]["event"] == "run_start"


async def test_multihooks_isolates_a_failing_child(tmp_path):
    tel = JsonlTelemetry(tmp_path)

    class Bad:
        async def on_run_start(self, *a):
            raise RuntimeError("boom")

        def __getattr__(self, _):  # any other hook -> no-op coroutine
            async def noop(*a, **k):
                return None

            return noop

    hooks = MultiHooks(Bad(), tel)
    g = Graph(S)
    g.add_node("a", lambda s, c: {"n": 1})
    g.add_edge(START, "a")
    g.add_edge("a", END)
    # The bad child raising must not break the run or the good telemetry child.
    out = await g.compile(hooks=hooks).invoke({"n": 0}, thread="iso")
    assert out["n"] == 1
    events = _read_jsonl(tmp_path / "iso.jsonl")
    assert events[0]["event"] == "run_start"
