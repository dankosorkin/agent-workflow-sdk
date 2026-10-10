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


def cost_of(plan: tuple[str, ...]) -> float:
    """Modeled cost of a plan (lower is better). Pure and deterministic."""
    cost = BASE_COST
    for strategy in plan:
        cost *= STRATEGY_FACTORS.get(strategy, 1.0)
    return round(cost, 2)


# A plan is a sorted tuple of strategy names. We use a tuple, not a set,
# because later chapters checkpoint the state to disk and JSON has no set type
# — a tuple round-trips as a list. `with_strategy` keeps it sorted so the same
# set of strategies always has the same representation.


def with_strategy(plan: tuple[str, ...], strategy: str) -> tuple[str, ...]:
    return tuple(sorted(set(plan) | {strategy}))


# --- the graph -------------------------------------------------------------


class OptState(State):
    plan: Annotated[tuple, last]  # the applied strategies, sorted
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
    out = await app.invoke({"plan": ("add_index",)})
    print(f"plan {list(out['plan'])} -> cost {out['cost']}")
    # the empty plan is the baseline
    base = await app.invoke({"plan": ()})
    print(f"plan {list(base['plan'])} -> cost {base['cost']}")


if __name__ == "__main__":
    asyncio.run(main())
