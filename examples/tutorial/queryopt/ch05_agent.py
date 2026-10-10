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

"""Query-optimizer tutorial, chapter 5: a real agent proposer.

Run (heuristic, offline):   python -m examples.tutorial.queryopt.ch05_agent
Run (local Codex agent):    AGENTFLOW_TUTORIAL_BACKEND=codex python -m \
                                examples.tutorial.queryopt.ch05_agent

The proposer stops enumerating and starts *reasoning*: a local Codex agent
reads the schema and query and suggests indexes. The graph does not change —
``get_proposer()`` returns a HeuristicProposer by default (deterministic,
offline, for CI) or a CodexProposer when AGENTFLOW_TUTORIAL_BACKEND=codex and
``codex`` is on PATH. The scorer stays deterministic SQLite either way, so the
measure is identical no matter who proposed the candidate — swap the proposer
and the score is still comparable. That is the propose/verify split: the agent
explores, the database judges.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from agentflow import END, START, Graph, State, append, by_key, last, merge
from agentflow.prebuilt import best_of, score_candidates
from examples.tutorial.queryopt.proposer import get_proposer
from examples.tutorial.queryopt.scorer import Problem, make_scorer


class OptState(State):
    problem: Annotated[dict, merge]
    search: Annotated[dict, by_key({"candidates": append, "incumbent": last})]
    improved: Annotated[bool, last]
    rounds: Annotated[int, last]


def build(problem: Problem) -> Graph:
    scorer = make_scorer(problem)
    proposer = get_proposer()  # heuristic by default; Codex behind the env flag

    async def round_node(state: OptState, ctx) -> dict:
        incumbent = state["search"].get("incumbent") or {"indexes": (), "value": float("-inf")}
        candidates = await proposer.propose(problem, incumbent["indexes"])
        scored = await score_candidates(candidates, problem, scorer, max_concurrency=8)
        best = best_of(scored)
        updates: dict = {"rounds": state.get("rounds", 0) + 1}
        if best is None or best[1].value <= incumbent["value"]:
            updates["improved"] = False
            return updates
        best_cand, best_score = best
        print(
            f"  round {state.get('rounds', 0) + 1}: proposer {type(proposer).__name__} "
            f"-> value {best_score.value} (plan_cost {best_score.metrics['plan_cost']})"
        )
        updates["search"] = {"incumbent": {"indexes": best_cand.indexes, "value": best_score.value}}
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
    proposer = get_proposer()
    print(f"optimizing with {type(proposer).__name__} (the scorer is always real SQLite):")
    app = build(problem).compile(step_limit=20)
    out = await app.invoke(
        {
            "problem": {"seed": problem.seed},
            "search": {"incumbent": {"indexes": (), "value": float("-inf")}},
            "rounds": 0,
        }
    )
    inc = out["search"]["incumbent"]
    print(f"\nbest value {inc['value']} with {len(inc['indexes'])} indexes:")
    for stmt in inc["indexes"]:
        print(f"    {stmt}")


if __name__ == "__main__":
    asyncio.run(main())
