# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""Backend-agnostic smoke example: a tiny graph with a conditional loop.

Run: python examples/hello_graph.py

No backend, no network — just the engine. It counts to a target, appending a
log line each step, and stops via a conditional edge.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from agentflow import END, START, Graph, State, add, append


class CountState(State):
    n: Annotated[int, add]
    log: Annotated[list, append]


async def main() -> None:
    target = 5

    async def tick(state, ctx):
        nxt = state.get("n", 0) + 1
        return {"n": 1, "log": f"tick {nxt}"}

    def route(state):
        return "again" if state["n"] < target else "done"

    g = Graph(CountState)
    g.add_node("tick", tick)
    g.add_edge(START, "tick")
    g.add_conditional_edges("tick", route, {"again": "tick", "done": END})

    app = g.compile()

    # Stream the run so you can watch each super-step.
    async for ev in app.stream({"n": 0, "log": []}):
        if ev.kind in ("node_end", "done"):
            print(f"[{ev.kind}] step={ev.step} node={ev.node}")

    final = await app.invoke({"n": 0, "log": []}, thread="hello")
    print("final:", final)


if __name__ == "__main__":
    asyncio.run(main())
