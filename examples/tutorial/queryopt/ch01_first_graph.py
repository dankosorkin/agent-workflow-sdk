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

"""Query-optimizer tutorial, chapter 1: your first graph, a real measure.

Run: python -m examples.tutorial.queryopt.ch01_first_graph

One node that scores one candidate against a real SQLite database. No search
yet — just enough to see that the measure is genuine: the empty plan scans a
table, a candidate with the right indexes turns scans into index searches, and
the plan cost drops. The engine pieces (State, a node, invoke) are the same as
the mechanics tutorial; what is new is that `cost` comes from EXPLAIN, not a toy
formula.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from agentflow import END, START, Graph, State, last
from examples.tutorial.queryopt.scorer import Candidate, Problem, make_scorer


class OptState(State):
    candidate: Annotated[Candidate, last]
    value: Annotated[float, last]
    metrics: Annotated[dict, last]


def build(problem: Problem) -> Graph:
    scorer = make_scorer(problem)

    async def score(state: OptState, ctx) -> dict:
        result = await scorer(state["candidate"], problem)
        return {"value": result.value, "metrics": result.metrics}

    g = Graph(OptState)
    g.add_node("score", score)
    g.add_edge(START, "score")
    g.add_edge("score", END)
    return g


async def main() -> None:
    problem = Problem()
    app = build(problem).compile()

    # the empty plan: no indexes, the query scans
    base = await app.invoke({"candidate": Candidate()})
    print(f"empty plan:   value {base['value']:>7}  plan_cost {base['metrics']['plan_cost']}")
    for line in base["metrics"]["plan"]:
        print(f"    {line}")

    # a candidate with indexes on the join/filter columns
    indexed = Candidate(
        indexes=(
            "CREATE INDEX i_oi_order ON order_items(order_id)",
            "CREATE INDEX i_o_customer ON orders(customer_id)",
            "CREATE INDEX i_c_segment ON customers(segment)",
        )
    )
    out = await app.invoke({"candidate": indexed})
    print(f"\nindexed plan: value {out['value']:>7}  plan_cost {out['metrics']['plan_cost']}")
    for line in out["metrics"]["plan"]:
        print(f"    {line}")


if __name__ == "__main__":
    asyncio.run(main())
