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

"""Optimizer tutorial, chapter 2: branching and loops.

Run: python examples/tutorial/02_branching.py

The optimizer now decides for itself. A greedy node tries adding one strategy
at a time, keeping it only if it lowers the cost, and a conditional edge loops
until no remaining strategy helps. Control flow is a router over state, not a
callback.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from agentflow import END, START, Graph, State, add, last

BASE_COST = 100.0
STRATEGY_FACTORS = {
    "add_index": 0.4,
    "push_filter": 0.6,
    "reorder_stages": 0.9,
    "cache_result": 1.1,
}


def cost_of(plan: frozenset[str]) -> float:
    cost = BASE_COST
    for strategy in plan:
        cost *= STRATEGY_FACTORS.get(strategy, 1.0)
    return round(cost, 2)


class OptState(State):
    plan: Annotated[frozenset, last]
    cost: Annotated[float, last]
    steps: Annotated[int, add]  # how many improvement rounds we ran


async def improve(state: OptState, ctx) -> dict:
    """Greedily add the single strategy that lowers cost the most this round."""
    plan = state["plan"]
    current = cost_of(plan)

    best_strategy = None
    best_cost = current
    for strategy in STRATEGY_FACTORS:
        if strategy in plan:
            continue
        trial = cost_of(plan | {strategy})
        if trial < best_cost:
            best_cost, best_strategy = trial, strategy

    if best_strategy is None:
        return {"steps": 1}  # nothing improved; cost/plan unchanged

    new_plan = plan | {best_strategy}
    print(f"  + {best_strategy}: {current} -> {best_cost}")
    return {"plan": new_plan, "cost": best_cost, "steps": 1}


def route(state: OptState) -> str:
    """Loop while the last round still has an improving move available."""
    plan = state["plan"]
    current = cost_of(plan)
    improvable = any(s not in plan and cost_of(plan | {s}) < current for s in STRATEGY_FACTORS)
    return "again" if improvable else "done"


def build() -> Graph:
    g = Graph(OptState)
    g.add_node("improve", improve)
    g.add_edge(START, "improve")
    g.add_conditional_edges("improve", route, {"again": "improve", "done": END})
    return g


async def main() -> None:
    app = build().compile()
    print("optimizing from the empty plan:")
    out = await app.invoke({"plan": frozenset(), "cost": BASE_COST})
    print(f"final plan {set(out['plan'])} -> cost {out['cost']} in {out['steps']} steps")


if __name__ == "__main__":
    asyncio.run(main())
