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

"""Optimizer tutorial, chapter 6: the promotion gate.

Run: python examples/tutorial/06_gate.py

The silent `verify` node of chapter 5 becomes an explicit quality gate. The
gate asks "did this round produce a candidate worth promoting?" and routes
three ways: pass (promote and keep going), fail (let the proposer try again),
or escalate (the proposer could not improve within the attempt budget — stop).
The attempt counter and three-way routing come from add_quality_gate.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from agentflow import END, START, Graph, append, by_key, last, merge
from agentflow.prebuilt import GateResult, GateState, add_quality_gate

BASE_COST = 100.0
STRATEGY_FACTORS = {
    "add_index": 0.4,
    "push_filter": 0.6,
    "reorder_stages": 0.9,
    "cache_result": 1.1,
}


def cost_of(plan) -> float:
    cost = BASE_COST
    for strategy in plan:
        cost *= STRATEGY_FACTORS.get(strategy, 1.0)
    return round(cost, 2)


def with_strategy(plan, strategy: str) -> tuple[str, ...]:
    """A plan is a sorted tuple (JSON-serializable, so it survives chapter 8)."""
    return tuple(sorted(set(plan) | {strategy}))


# GateState mixes in gate_attempts / gate_results; we add our own channels.
class OptState(GateState):
    problem: Annotated[dict, merge]
    search: Annotated[dict, by_key({"candidates": append, "incumbent": last})]


async def propose(state: OptState, ctx) -> dict:
    """Propose every untried strategy as a candidate this round."""
    incumbent = state["search"]["incumbent"]
    plan = incumbent["plan"]
    cands = [
        {"plan": with_strategy(plan, s), "cost": cost_of(with_strategy(plan, s))}
        for s in STRATEGY_FACTORS
        if s not in plan
    ]
    return {"search": {"candidates": cands}}


def _best_this_round(state: OptState):
    incumbent = state["search"]["incumbent"]
    plan = incumbent["plan"]
    this_round = [
        c
        for c in state["search"].get("candidates", [])
        if set(plan).issubset(c["plan"]) and len(c["plan"]) == len(plan) + 1
    ]
    better = [c for c in this_round if c["cost"] < incumbent["cost"]]
    return min(better, key=lambda c: c["cost"]) if better else None


async def evaluate(state: OptState, ctx) -> GateResult:
    """The gate's decision: is there a candidate worth promoting this round?"""
    best = _best_this_round(state)
    if best is None:
        return GateResult(passed=False, feedback="no candidate beat the incumbent")
    improvement = state["search"]["incumbent"]["cost"] - best["cost"]
    return GateResult(passed=True, score=improvement, reasons=[f"best={best['cost']}"])


async def promote(state: OptState, ctx) -> dict:
    best = _best_this_round(state)
    if best is None:
        return {}
    print(f"  promote {list(best['plan'])} cost {best['cost']}")
    return {"search": {"incumbent": best}}


async def report(state: OptState, ctx) -> dict:
    incumbent = state["search"]["incumbent"]
    print(f"  stop: best plan {list(incumbent['plan'])} cost {incumbent['cost']}")
    return {}


def after_promote(state: OptState) -> str:
    incumbent = state["search"]["incumbent"]
    plan = incumbent["plan"]
    improvable = any(
        s not in plan and cost_of(with_strategy(plan, s)) < incumbent["cost"]
        for s in STRATEGY_FACTORS
    )
    return "again" if improvable else "stop"


def build() -> Graph:
    g = Graph(OptState)
    g.add_node("propose", propose)
    g.add_node("promote", promote)
    g.add_node("report", report)

    # the gate sits between propose and the promote/report decision
    add_quality_gate(
        g,
        "gate",
        evaluate,
        on_pass="promote",  # a worthy candidate -> promote it
        on_fail="propose",  # nothing good this round -> propose again
        on_escalate="report",  # repeatedly no improvement -> give up
        max_attempts=3,
    )

    g.add_edge(START, "propose")
    g.add_edge("propose", "gate")
    g.add_conditional_edges("promote", after_promote, {"again": "propose", "stop": "report"})
    g.add_edge("report", END)
    return g


async def main() -> None:
    app = build().compile(step_limit=100)
    problem = {"base_cost": BASE_COST, "strategies": list(STRATEGY_FACTORS)}
    print("optimizing with a promotion gate:")
    out = await app.invoke(
        {"problem": problem, "search": {"incumbent": {"plan": (), "cost": BASE_COST}}}
    )
    incumbent = out["search"]["incumbent"]
    print(f"final plan {list(incumbent['plan'])} -> cost {incumbent['cost']}")


if __name__ == "__main__":
    asyncio.run(main())
