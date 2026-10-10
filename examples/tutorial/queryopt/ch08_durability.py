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

"""Query-optimizer tutorial, chapter 8: surviving restarts, and a token budget.

Run: python -m examples.tutorial.queryopt.ch08_durability

Two durability concerns, plus the guard that matters once a real LLM proposes.

- The search is checkpointed, so a run that stops mid-search resumes in a fresh
  process. State is all JSON-serializable (index statements are strings,
  candidate summaries are dicts), so a durable checkpointer can persist it.
- A token budget: when the proposer is an LLM, each round costs tokens. The
  budget rides the checkpoint (BudgetState), a node records each round's usage,
  and the router stops when the cap is hit. With the heuristic proposer the
  usage is zero, so the cap is never tripped and the run is unaffected.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from typing import Annotated

from agentflow import END, START, Graph, SqliteCheckpointer, Usage, append, by_key, last, merge
from agentflow.prebuilt import (
    BudgetState,
    best_of,
    over_budget,
    record_usage,
    score_candidates,
)
from examples.tutorial.queryopt.proposer import get_proposer
from examples.tutorial.queryopt.scorer import Problem, make_scorer


class OptState(BudgetState):
    problem: Annotated[dict, merge]
    search: Annotated[dict, by_key({"candidates": append, "incumbent": last})]
    improved: Annotated[bool, last]
    stopped_for_budget: Annotated[bool, last]


def build(problem: Problem, *, max_tokens: int | None = None) -> Graph:
    scorer = make_scorer(problem)
    proposer = get_proposer()

    async def round_node(state: OptState, ctx) -> dict:
        incumbent = state["search"].get("incumbent") or {"indexes": (), "value": float("-inf")}
        candidates = await proposer.propose(problem, incumbent["indexes"])
        scored = await score_candidates(candidates, problem, scorer, max_concurrency=8)
        best = best_of(scored)
        # a heuristic proposer has no token usage; an LLM one would. We model a
        # per-round usage so the budget wiring is visible even offline.
        usage = Usage(total_tokens=0)
        updates: dict = {**record_usage(usage)}
        if best is None or best[1].value <= incumbent["value"]:
            updates["improved"] = False
            return updates
        updates["search"] = {"incumbent": {"indexes": best[0].indexes, "value": best[1].value}}
        updates["improved"] = True
        return updates

    def route(state: OptState) -> str:
        if max_tokens is not None and over_budget(state, max_tokens=max_tokens):
            return "stop"
        return "again" if state.get("improved") else "stop"

    async def stop_node(state: OptState, ctx) -> dict:
        return {"stopped_for_budget": over_budget(state, max_tokens=max_tokens or 10**18)}

    g = Graph(OptState)
    g.add_node("round", round_node)
    g.add_node("stop", stop_node)
    g.add_edge(START, "round")
    g.add_conditional_edges("round", route, {"again": "round", "stop": "stop"})
    g.add_edge("stop", END)
    return g


async def main() -> None:
    db = os.path.join(tempfile.mkdtemp(), "opt.db")
    problem = Problem()
    init = {
        "problem": {"seed": problem.seed},
        "search": {"incumbent": {"indexes": (), "value": float("-inf")}},
    }

    # process A: optimize to completion, durably checkpointed to sqlite
    app_a = build(problem).compile(checkpointer=SqliteCheckpointer(db), step_limit=50)
    print("process A: optimizing (durable to sqlite):")
    out = await app_a.invoke(init, thread="run-1")
    inc = out["search"]["incumbent"]
    print(f"  done: value {inc['value']} with {len(inc['indexes'])} indexes")

    # process B: a fresh graph over the same db re-reads the final checkpoint
    app_b = build(problem).compile(checkpointer=SqliteCheckpointer(db))
    reloaded = await app_b.get_state("run-1")
    print(
        f"process B: read the checkpoint -> value {reloaded.state['search']['incumbent']['value']}"
    )
    print(
        f"  budget total_tokens recorded: {reloaded.state.get('budget', {}).get('total_tokens', 0)}"
    )


if __name__ == "__main__":
    asyncio.run(main())
