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

"""Query-optimizer tutorial, chapter 3: parallel experiments.

Run: python -m examples.tutorial.queryopt.ch03_parallel

Each round proposes several index candidates and scores them concurrently —
each candidate runs its own EXPLAIN against its own scratch database, so they
are independent and parallelizable. score_candidates fans them out (bounded by
max_concurrency); best_of picks the winner. On a real database, where scoring a
candidate is the expensive part, this concurrency is the whole game.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from agentflow import END, START, Graph, State, add, last
from agentflow.prebuilt import best_of, score_candidates
from examples.tutorial.queryopt.proposer import HeuristicProposer
from examples.tutorial.queryopt.scorer import Problem, make_scorer


class OptState(State):
    applied: Annotated[tuple, last]
    value: Annotated[float, last]
    improved: Annotated[bool, last]
    rounds: Annotated[int, add]
    scored_count: Annotated[int, add]  # how many candidates we evaluated in total


def build(problem: Problem) -> Graph:
    scorer = make_scorer(problem)
    proposer = HeuristicProposer()

    async def round_node(state: OptState, ctx) -> dict:
        applied = state.get("applied", ())
        current_value = state.get("value", float("-inf"))
        candidates = await proposer.propose(problem, applied)
        # score every candidate this round concurrently
        scored = await score_candidates(candidates, problem, scorer, max_concurrency=8)
        best = best_of(scored)
        if best is None or best[1].value <= current_value:
            return {"improved": False, "rounds": 1, "scored_count": len(scored)}
        best_cand, best_score = best
        print(
            f"  round {state.get('rounds', 0) + 1}: scored {len(scored)} candidates, "
            f"best value {best_score.value} (plan_cost {best_score.metrics['plan_cost']})"
        )
        return {
            "applied": best_cand.indexes,
            "value": best_score.value,
            "improved": True,
            "rounds": 1,
            "scored_count": len(scored),
        }

    def route(state: OptState) -> str:
        return "again" if state.get("improved") else "done"

    g = Graph(OptState)
    g.add_node("round", round_node)
    g.add_edge(START, "round")
    g.add_conditional_edges("round", route, {"again": "round", "done": END})
    return g


async def main() -> None:
    problem = Problem()
    app = build(problem).compile(step_limit=20)
    print("each round scores all candidates concurrently:")
    out = await app.invoke({"applied": (), "value": float("-inf")})
    print(
        f"\nfinal value {out['value']} with {len(out['applied'])} indexes; "
        f"{out['scored_count']} candidate evaluations across {out['rounds']} rounds"
    )


if __name__ == "__main__":
    asyncio.run(main())
