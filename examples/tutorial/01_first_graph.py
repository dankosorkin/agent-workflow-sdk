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

"""Optimizer tutorial, chapter 1: your first graph.

Run: python examples/tutorial/01_first_graph.py

One node, one state channel, one run. The node scores a single query plan
against a deterministic cost model and writes the cost into state. No branching,
no loop, no backend yet — just enough to see state, a reducer, and invoke.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from agentflow import END, START, Graph, State, last

# --- the toy cost model (shared by every chapter) --------------------------
# A plan is a set of applied strategy names. Each strategy multiplies the base
# cost by a factor < 1 (it helps) or > 1 (it hurts). Deterministic: the same
# plan always scores the same, so runs are perfectly reproducible.

BASE_COST = 100.0
STRATEGY_FACTORS = {
    "add_index": 0.4,  # a big win
    "push_filter": 0.6,  # a solid win
    "reorder_stages": 0.9,  # a small win
    "cache_result": 1.1,  # actually a slight loss here
}


def cost_of(plan: frozenset[str]) -> float:
    """Modeled cost of a plan (lower is better). Pure and deterministic."""
    cost = BASE_COST
    for strategy in plan:
        cost *= STRATEGY_FACTORS.get(strategy, 1.0)
    return round(cost, 2)


# --- the graph -------------------------------------------------------------


class OptState(State):
    plan: Annotated[frozenset, last]  # the applied strategies
    cost: Annotated[float, last]  # the modeled cost of that plan


async def score(state: OptState, ctx) -> dict:
    plan = state["plan"]
    return {"cost": cost_of(plan)}


def build() -> Graph:
    g = Graph(OptState)
    g.add_node("score", score)
    g.add_edge(START, "score")
    g.add_edge("score", END)
    return g


async def main() -> None:
    app = build().compile()
    # score a plan that applies one strategy
    out = await app.invoke({"plan": frozenset({"add_index"})})
    print(f"plan {set(out['plan'])} -> cost {out['cost']}")
    # the empty plan is the baseline
    base = await app.invoke({"plan": frozenset()})
    print(f"plan {set(base['plan'])} -> cost {base['cost']}")


if __name__ == "__main__":
    asyncio.run(main())
