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

"""Optimizer tutorial, chapter 4: dual-state with by_key.

Run: python examples/tutorial/04_dual_state.py

The flat state of chapter 3 is split into two channels with different jobs:
`problem` holds the fixed facts (the cost model's constraints) and barely
changes; `search` holds everything the optimizer discovers — the candidates it
tried and the incumbent (best plan so far). The `search` channel is a by_key
channel so its fields each fold with their own reducer: candidates accumulate,
the incumbent is replaced whole.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from agentflow import END, START, Graph, State, append, by_key, last, merge

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
    # fixed facts about the problem; a plain merge is enough (last-wins per key)
    problem: Annotated[dict, merge]
    # the moving search: each field folds with its own reducer
    search: Annotated[
        dict,
        by_key(
            {
                "candidates": append,  # every trial result accumulates
                "incumbent": last,  # the best plan so far, replaced whole
            }
        ),
    ]


def _trial_node(strategy: str):
    async def trial(state: OptState, ctx) -> dict:
        incumbent = state["search"].get("incumbent") or {"plan": frozenset(), "cost": BASE_COST}
        plan = incumbent["plan"]
        if strategy in plan:
            return {}
        cand = {"plan": plan | {strategy}, "cost": cost_of(plan | {strategy})}
        return {"search": {"candidates": [cand]}}

    trial.__name__ = f"trial_{strategy}"
    return trial


async def promote(state: OptState, ctx) -> dict:
    """Promotion boundary: pick the best candidate that beats the incumbent."""
    incumbent = state["search"].get("incumbent") or {"plan": frozenset(), "cost": BASE_COST}
    candidates = state["search"].get("candidates", [])
    plan = incumbent["plan"]
    # only this round's candidates: the incumbent plan plus exactly one strategy
    this_round = [
        c for c in candidates if plan.issubset(c["plan"]) and len(c["plan"]) == len(plan) + 1
    ]
    better = [c for c in this_round if c["cost"] < incumbent["cost"]]
    if not better:
        return {}
    best = min(better, key=lambda c: c["cost"])
    print(f"  promote {set(best['plan'])} cost {best['cost']}")
    return {"search": {"incumbent": best}}


def fan_out(state: OptState) -> list[str]:
    incumbent = state["search"].get("incumbent") or {"plan": frozenset()}
    plan = incumbent["plan"]
    remaining = [s for s in STRATEGY_FACTORS if s not in plan]
    return remaining or ["none"]


def after_promote(state: OptState) -> str:
    incumbent = state["search"].get("incumbent") or {"plan": frozenset(), "cost": BASE_COST}
    plan = incumbent["plan"]
    improvable = any(
        s not in plan and cost_of(plan | {s}) < incumbent["cost"] for s in STRATEGY_FACTORS
    )
    return "again" if improvable else "done"


async def noop(state: OptState, ctx) -> dict:
    return {}


def build() -> Graph:
    g = Graph(OptState)
    g.add_node("dispatch", noop)
    for strategy in STRATEGY_FACTORS:
        g.add_node(f"trial_{strategy}", _trial_node(strategy))
    g.add_node("none", noop)
    g.add_node("promote", promote)

    g.add_edge(START, "dispatch")
    targets = {s: f"trial_{s}" for s in STRATEGY_FACTORS}
    targets["none"] = "none"
    g.add_conditional_edges("dispatch", fan_out, targets)
    for strategy in STRATEGY_FACTORS:
        g.add_edge(f"trial_{strategy}", "promote")
    g.add_edge("none", "promote")
    g.add_conditional_edges("promote", after_promote, {"again": "dispatch", "done": END})
    return g


async def main() -> None:
    app = build().compile()
    print("optimizing with dual-state (problem vs search):")
    out = await app.invoke(
        {
            "problem": {"base_cost": BASE_COST, "strategies": list(STRATEGY_FACTORS)},
            "search": {"incumbent": {"plan": frozenset(), "cost": BASE_COST}},
        }
    )
    incumbent = out["search"]["incumbent"]
    print(f"final plan {set(incumbent['plan'])} -> cost {incumbent['cost']}")
    print(f"candidates evaluated across the run: {len(out['search']['candidates'])}")


if __name__ == "__main__":
    asyncio.run(main())
