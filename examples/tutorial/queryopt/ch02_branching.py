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

"""Query-optimizer tutorial, chapter 2: branching and loops.

Run: python -m examples.tutorial.queryopt.ch02_branching

A greedy loop: each round, try adding one more index, keep it if it lowers the
cost, and loop until nothing helps. Control flow is a router over state — the
same shape as the mechanics tutorial, now driving a real EXPLAIN-based measure.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from agentflow import END, START, Graph, State, add, last
from agentflow.prebuilt import best_of, score_candidates
from examples.tutorial.queryopt.proposer import HeuristicProposer
from examples.tutorial.queryopt.scorer import Problem, make_scorer


class OptState(State):
    applied: Annotated[tuple, last]  # the index statements chosen so far
    value: Annotated[float, last]
    improved: Annotated[bool, last]  # did the last round lower the cost?
    rounds: Annotated[int, add]


def build(problem: Problem) -> Graph:
    scorer = make_scorer(problem)
    proposer = HeuristicProposer()

    async def improve(state: OptState, ctx) -> dict:
        applied = state.get("applied", ())
        current_value = state.get("value", float("-inf"))
        candidates = await proposer.propose(problem, applied)
        scored = await score_candidates(candidates, problem, scorer, max_concurrency=4)
        best = best_of(scored)
        if best is None or best[1].value <= current_value:
            return {"rounds": 1, "improved": False}  # nothing beat the current plan
        best_cand, best_score = best
        print(
            f"  round {state.get('rounds', 0) + 1}: "
            f"+{len(best_cand.indexes)} indexes -> value {best_score.value} "
            f"(plan_cost {best_score.metrics['plan_cost']})"
        )
        return {
            "applied": best_cand.indexes,
            "value": best_score.value,
            "improved": True,
            "rounds": 1,
        }

    def route(state: OptState) -> str:
        return "again" if state.get("improved") else "done"

    g = Graph(OptState)
    g.add_node("improve", improve)
    g.add_edge(START, "improve")
    g.add_conditional_edges("improve", route, {"again": "improve", "done": END})
    return g


async def main() -> None:
    problem = Problem()
    app = build(problem).compile(step_limit=20)
    print("greedily adding indexes until nothing helps:")
    out = await app.invoke({"applied": (), "value": float("-inf")})
    print(
        f"\nfinal value {out['value']} with {len(out['applied'])} indexes in {out['rounds']} rounds"
    )
    for stmt in out["applied"]:
        print(f"    {stmt}")


if __name__ == "__main__":
    asyncio.run(main())
