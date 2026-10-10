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

"""Optimizer tutorial, chapter 7: human approval.

Run: python examples/tutorial/07_human.py

Finding the best plan is automatic; *applying* it may be expensive or risky, so
we pause for a human to approve before committing. The approve node calls
ctx.interrupt, which suspends the run and checkpoints it; a separate resume
feeds the human's answer back and the run continues to apply or abandon.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from agentflow import END, START, Graph, MemoryCheckpointer, append, by_key, last, merge
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


class OptState(GateState):
    problem: Annotated[dict, merge]
    search: Annotated[dict, by_key({"candidates": append, "incumbent": last})]
    applied: Annotated[bool, last]
    decision: Annotated[str, last]


async def propose(state: OptState, ctx) -> dict:
    plan = state["search"]["incumbent"]["plan"]
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
    best = _best_this_round(state)
    return GateResult(passed=best is not None)


async def promote(state: OptState, ctx) -> dict:
    best = _best_this_round(state)
    return {"search": {"incumbent": best}} if best else {}


def after_promote(state: OptState) -> str:
    incumbent = state["search"]["incumbent"]
    plan = incumbent["plan"]
    improvable = any(
        s not in plan and cost_of(with_strategy(plan, s)) < incumbent["cost"]
        for s in STRATEGY_FACTORS
    )
    return "again" if improvable else "approve"


async def approve(state: OptState, ctx) -> dict:
    """Pause for a human to approve applying the final plan."""
    incumbent = state["search"]["incumbent"]
    answer = await ctx.interrupt(
        {
            "question": "apply this plan?",
            "plan": sorted(incumbent["plan"]),
            "cost": incumbent["cost"],
        }
    )
    # answer comes back from app.resume(thread, value=...)
    return {
        "decision": "apply" if str(answer).lower() in ("y", "yes", "apply", "allow") else "skip"
    }


async def apply_plan(state: OptState, ctx) -> dict:
    if state["decision"] == "apply":
        print(f"  applied plan {list(state['search']['incumbent']['plan'])}")
        return {"applied": True}
    print("  plan not applied (human declined)")
    return {"applied": False}


def after_approve(state: OptState) -> str:
    return "apply"  # always proceed to apply_plan, which honors the decision


def build(checkpointer=None) -> Graph:
    g = Graph(OptState)
    g.add_node("propose", propose)
    g.add_node("promote", promote)
    g.add_node("approve", approve)
    g.add_node("apply", apply_plan)
    add_quality_gate(
        g, "gate", evaluate, on_pass="promote", on_fail="propose", on_escalate="approve"
    )
    g.add_edge(START, "propose")
    g.add_edge("propose", "gate")
    g.add_conditional_edges("promote", after_promote, {"again": "propose", "approve": "approve"})
    g.add_conditional_edges("approve", after_approve, {"apply": "apply"})
    g.add_edge("apply", END)
    return g


async def main() -> None:
    cp = MemoryCheckpointer()
    app = build().compile(checkpointer=cp, step_limit=100)
    problem = {"base_cost": BASE_COST}
    init = {"problem": problem, "search": {"incumbent": {"plan": (), "cost": BASE_COST}}}

    print("optimizing until the approval gate:")
    await app.invoke(init, thread="run-1")

    # the run is now parked at the human interrupt
    state = await app.get_state("run-1")
    assert state.interrupted
    req = state.interrupt_payload
    print(f"  PAUSED for approval: apply {req['plan']} (cost {req['cost']})?")

    # a human (here: us) approves; resume feeds the answer back
    out = await app.resume("run-1", value="yes")
    print(f"applied={out['applied']} final plan {list(out['search']['incumbent']['plan'])}")


if __name__ == "__main__":
    asyncio.run(main())
