# Copyright 2026 Daniel Sorkin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

    # Stream one run: its `done` event carries the final state.
    final = None
    async for ev in app.stream({"n": 0, "log": []}):
        if ev.kind in ("node_end", "done"):
            print(f"[{ev.kind}] step={ev.step} node={ev.node}")
        if ev.kind == "done":
            final = ev.data

    print("final:", final)


if __name__ == "__main__":
    asyncio.run(main())
