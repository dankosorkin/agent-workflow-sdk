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

"""Query-optimizer tutorial, chapter 7: human approval.

Run: python -m examples.tutorial.queryopt.ch07_human

Creating indexes on a production table is not free — it takes space and slows
writes. So before the optimizer applies its chosen index set, it pauses for a
human to approve, with ctx.interrupt. The search is automatic; committing the
result is gated on a person.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from agentflow import END, START, Graph, MemoryCheckpointer, append, by_key, last, merge
from agentflow.prebuilt import GateResult, GateState, add_quality_gate, best_of, score_candidates
from examples.tutorial.queryopt.proposer import get_proposer
from examples.tutorial.queryopt.scorer import Problem, make_scorer


class OptState(GateState):
    problem: Annotated[dict, merge]
    search: Annotated[dict, by_key({"candidates": append, "incumbent": last})]
    round_best: Annotated[dict, last]
    applied: Annotated[bool, last]


def build(problem: Problem) -> Graph:
    scorer = make_scorer(problem)
    proposer = get_proposer()

    async def propose(state: OptState, ctx) -> dict:
        incumbent = state["search"].get("incumbent") or {"indexes": (), "value": float("-inf")}
        candidates = await proposer.propose(problem, incumbent["indexes"])
        scored = await score_candidates(candidates, problem, scorer, max_concurrency=8)
        best = best_of(scored)
        if best is None or best[1].value <= incumbent["value"]:
            return {"round_best": {}}
        return {"round_best": {"indexes": best[0].indexes, "value": best[1].value}}

    async def evaluate(state: OptState, ctx) -> GateResult:
        return GateResult(passed=bool(state.get("round_best")))

    async def promote(state: OptState, ctx) -> dict:
        return {"search": {"incumbent": state["round_best"]}}

    async def approve(state: OptState, ctx) -> dict:
        inc = state["search"].get("incumbent") or {"indexes": (), "value": None}
        answer = await ctx.interrupt(
            {
                "question": "apply these indexes?",
                "indexes": list(inc["indexes"]),
                "value": inc["value"],
            }
        )
        return {"applied": str(answer).lower() in ("y", "yes", "apply")}

    g = Graph(OptState)
    g.add_node("propose", propose)
    g.add_node("promote", promote)
    g.add_node("approve", approve)
    add_quality_gate(
        g, "gate", evaluate, on_pass="promote", on_fail="propose", on_escalate="approve"
    )
    g.add_edge(START, "propose")
    g.add_edge("propose", "gate")
    g.add_edge("promote", "propose")
    g.add_edge("approve", END)
    return g


async def main() -> None:
    problem = Problem()
    app = build(problem).compile(checkpointer=MemoryCheckpointer(), step_limit=50)
    print("optimizing until the approval pause:")
    await app.invoke(
        {
            "problem": {"seed": problem.seed},
            "search": {"incumbent": {"indexes": (), "value": float("-inf")}},
        },
        thread="run-1",
    )
    state = await app.get_state("run-1")
    assert state.interrupted
    req = state.interrupt_payload
    print(f"  PAUSED: apply {len(req['indexes'])} indexes (value {req['value']})?")
    out = await app.resume("run-1", value="yes")
    print(f"applied={out['applied']}")


if __name__ == "__main__":
    asyncio.run(main())
