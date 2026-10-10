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

"""Query-optimizer tutorial, chapter 6: the promotion gate.

Run: python -m examples.tutorial.queryopt.ch06_gate

The promotion decision becomes an explicit quality gate. The evaluator asks
"did this round produce an index set worth promoting?" and routes three ways:
pass (promote, keep searching), fail (let the proposer try again), escalate
(the proposer stopped finding improvements within the attempt budget — stop).
The rewrite-equivalence check lives in the scorer, so an infeasible candidate
can never be promoted.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from agentflow import END, START, Graph, append, by_key, last, merge
from agentflow.prebuilt import GateResult, GateState, add_quality_gate, best_of, score_candidates
from examples.tutorial.queryopt.proposer import get_proposer
from examples.tutorial.queryopt.scorer import Problem, make_scorer


class OptState(GateState):
    problem: Annotated[dict, merge]
    search: Annotated[dict, by_key({"candidates": append, "incumbent": last})]
    round_best: Annotated[dict, last]  # the best candidate found this round


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
        rb = state.get("round_best") or {}
        if not rb:
            return GateResult(passed=False, feedback="no candidate beat the incumbent")
        return GateResult(passed=True, score=rb["value"])

    async def promote(state: OptState, ctx) -> dict:
        rb = state["round_best"]
        print(f"  promote value {rb['value']} ({len(rb['indexes'])} indexes)")
        return {"search": {"incumbent": rb}}

    async def report(state: OptState, ctx) -> dict:
        inc = state["search"].get("incumbent") or {"indexes": (), "value": None}
        print(f"  stop: best value {inc['value']} with {len(inc['indexes'])} indexes")
        return {}

    g = Graph(OptState)
    g.add_node("propose", propose)
    g.add_node("promote", promote)
    g.add_node("report", report)
    add_quality_gate(
        g, "gate", evaluate, on_pass="promote", on_fail="propose", on_escalate="report"
    )
    g.add_edge(START, "propose")
    g.add_edge("propose", "gate")
    g.add_edge("promote", "propose")  # after promoting, try to improve again
    g.add_edge("report", END)
    return g


async def main() -> None:
    problem = Problem()
    app = build(problem).compile(step_limit=50)
    print("optimizing with a promotion gate:")
    out = await app.invoke(
        {
            "problem": {"seed": problem.seed},
            "search": {"incumbent": {"indexes": (), "value": float("-inf")}},
        }
    )
    inc = out["search"]["incumbent"]
    print(f"\nfinal value {inc['value']} with {len(inc['indexes'])} indexes")


if __name__ == "__main__":
    asyncio.run(main())
