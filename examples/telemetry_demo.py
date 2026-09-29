# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""Telemetry demo: record a run to JSONL while collecting in-memory metrics.

Runs a tiny graph whose node streams a couple of backend events, with both
RunMetrics and JsonlTelemetry attached via MultiHooks. Prints the metrics
summary and the path to the written JSONL log.

    python examples/telemetry_demo.py
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Annotated

from agentflow import (
    END,
    START,
    Graph,
    JsonlTelemetry,
    MultiHooks,
    RunMetrics,
    State,
    add,
)
from agentflow.events import TextChunk, ToolCall


class S(State):
    n: Annotated[int, add]


async def main() -> None:
    out_dir = Path(".runs/telemetry")

    async def plan(state, ctx):
        ctx.emit(TextChunk(text="thinking..."))
        return {"n": 1}

    async def act(state, ctx):
        ctx.emit(ToolCall(id="1", name="search", args={"q": "agentflow"}))
        ctx.emit(TextChunk(text="done"))
        return {"n": 1}

    g = Graph(S)
    g.add_node("plan", plan)
    g.add_node("act", act)
    g.add_edge(START, "plan")
    g.add_edge("plan", "act")
    g.add_edge("act", END)

    metrics = RunMetrics()
    telemetry = JsonlTelemetry(out_dir)
    app = g.compile(hooks=MultiHooks(metrics, telemetry))

    await app.invoke({"n": 0}, thread="demo")
    await asyncio.sleep(0.05)  # let fire-and-forget event hooks flush

    print("metrics:", json.dumps(metrics.summary(), indent=2))
    log = out_dir / "demo.jsonl"
    print(f"\ntelemetry log: {log}")
    print("--- events ---")
    for line in log.read_text().splitlines():
        e = json.loads(line)
        extra = e.get("node") or e.get("eventType") or ""
        print(f"  {e['event']:<12} step={e.get('step')} {extra}")


if __name__ == "__main__":
    asyncio.run(main())
