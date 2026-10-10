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

"""Query-optimizer tutorial, chapter 4: dual-state with by_key.

Run: python -m examples.tutorial.queryopt.ch04_dual_state

The flat state splits in two. `problem` holds the fixed facts (the query, the
data seed, the index budget) and never changes during the search. `search`
holds everything discovered — the candidates tried and the incumbent (best set
of indexes so far) — in a by_key channel so its fields fold differently:
candidates accumulate, the incumbent is replaced whole.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from agentflow import END, START, Graph, State, append, by_key, last, merge
from agentflow.prebuilt import best_of, score_candidates
from examples.tutorial.queryopt.proposer import HeuristicProposer
from examples.tutorial.queryopt.scorer import Problem, make_scorer


class OptState(State):
    problem: Annotated[dict, merge]  # fixed facts (seed, penalty, query)
    search: Annotated[
        dict,
        by_key(
            {
                "candidates": append,  # every scored candidate accumulates
                "incumbent": last,  # the best (indexes, value) so far, whole
            }
        ),
    ]
    improved: Annotated[bool, last]
    rounds: Annotated[int, last]


def build(problem: Problem) -> Graph:
    scorer = make_scorer(problem)
    proposer = HeuristicProposer()

    async def round_node(state: OptState, ctx) -> dict:
        incumbent = state["search"].get("incumbent") or {"indexes": (), "value": float("-inf")}
        applied = incumbent["indexes"]
        candidates = await proposer.propose(problem, applied)
        scored = await score_candidates(candidates, problem, scorer, max_concurrency=8)
        # record a compact summary of each candidate into search.candidates
        summaries = [
            {"indexes": c.indexes, "value": s.value, "plan_cost": s.metrics.get("plan_cost")}
            for c, s in scored
        ]
        best = best_of(scored)
        updates: dict = {
            "search": {"candidates": summaries},
            "rounds": state.get("rounds", 0) + 1,
        }
        if best is None or best[1].value <= incumbent["value"]:
            updates["improved"] = False
            return updates
        best_cand, best_score = best
        print(f"  round {state.get('rounds', 0) + 1}: promote value {best_score.value}")
        updates["search"]["incumbent"] = {
            "indexes": best_cand.indexes,
            "value": best_score.value,
        }
        updates["improved"] = True
        return updates

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
    print("dual-state: fixed problem vs moving search")
    out = await app.invoke(
        {
            "problem": {"seed": problem.seed, "index_penalty": problem.index_penalty},
            "search": {"incumbent": {"indexes": (), "value": float("-inf")}},
            "rounds": 0,
        }
    )
    inc = out["search"]["incumbent"]
    print(f"\nbest value {inc['value']} with {len(inc['indexes'])} indexes")
    print(f"candidates evaluated across the run: {len(out['search']['candidates'])}")


if __name__ == "__main__":
    asyncio.run(main())
