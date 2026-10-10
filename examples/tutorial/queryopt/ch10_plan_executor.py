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

"""Query-optimizer tutorial, chapter 10: plan-as-data at scale.

Run: python -m examples.tutorial.queryopt.ch10_plan_executor

The finale. The planner emits a dependency graph of index experiments as data:
measure each single index first, then — building on the best single — measure
the pairs that extend it. add_plan_executor runs them by readiness (singles
concurrently, each pair after its singles), scoring each with the real SQLite
scorer. The whole optimizer runs as a background job on the control plane: a
registry, a queue, a worker — the shape it would take in production, where the
queue is Postgres and an HTTP server accepts the submission.
"""

from __future__ import annotations

import asyncio
import tempfile
from typing import Annotated

from agentflow import (
    END,
    START,
    FileCheckpointer,
    Graph,
    GraphRegistry,
    MemoryRunQueue,
    Worker,
    last,
)
from agentflow.prebuilt import (
    PlanState,
    Task,
    TaskResult,
    add_plan_executor,
    plan_complete,
)
from examples.tutorial.queryopt.proposer import _MOVES
from examples.tutorial.queryopt.scorer import Candidate, Problem, make_scorer


class OptState(PlanState):
    waves: Annotated[int, last]
    best: Annotated[dict, last]


def build(problem: Problem, checkpointer):
    scorer = make_scorer(problem)

    async def measure(task: Task, state, ctx) -> TaskResult:
        indexes = tuple(task.input["indexes"])
        score = await scorer(Candidate(indexes=indexes), problem)
        return TaskResult(task.id, output={"indexes": list(indexes), "value": score.value})

    async def planner(state: OptState, ctx) -> dict:
        results = state.get("plan", {}).get("results", [])
        waves = state.get("waves", 0)
        moves = list(_MOVES.values())

        if waves == 0:
            # wave 1: each single index as its own experiment (independent)
            tasks = [
                Task(f"single:{i}", "measure", input={"indexes": [m]}) for i, m in enumerate(moves)
            ]
            print(f"planner wave 1: {len(tasks)} single-index experiments")
            return {"plan": {"tasks": tasks}, "waves": 1}

        if waves == 1:
            # pick the best single, then measure pairs that extend it
            singles = {
                tuple(r["output"]["indexes"]): r["output"]["value"]
                for r in results
                if len(r["output"]["indexes"]) == 1
            }
            best_single = max(singles, key=singles.get)
            best_idx = moves.index(best_single[0])
            pairs = [
                Task(
                    f"pair:{best_idx}+{i}",
                    "measure",
                    input={"indexes": sorted([best_single[0], m])},
                    prerequisites=(f"single:{best_idx}", f"single:{i}"),
                )
                for i, m in enumerate(moves)
                if m != best_single[0]
            ]
            print(f"planner wave 2: {len(pairs)} pair experiments extending the best single")
            return {"plan": {"tasks": pairs}, "waves": 2}

        return {"waves": waves + 1}

    def planner_route(state: OptState) -> str:
        return "stop" if (plan_complete(state) and state.get("waves", 0) >= 2) else "run"

    async def summarize(state: OptState, ctx) -> dict:
        results = state["plan"]["results"]
        best = max(results, key=lambda r: r["output"]["value"])["output"]
        print(
            f"best measured: {len(best['indexes'])} indexes, value {best['value']} "
            f"(from {len(results)} experiments)"
        )
        return {"best": best}

    g = Graph(OptState)
    g.add_node("planner", planner)
    add_plan_executor(g, handlers={"measure": measure}, on_idle="planner", max_fanout=6)
    g.add_node("summarize", summarize)
    g.add_conditional_edges("planner", planner_route, {"run": "dispatch", "stop": "summarize"})
    g.add_edge("summarize", END)
    g.add_edge(START, "planner")
    return g.compile(checkpointer=checkpointer, step_limit=200)


async def main() -> None:
    problem = Problem()
    cp = FileCheckpointer(tempfile.mkdtemp())

    registry = GraphRegistry()
    registry.register("optimize", lambda: build(problem, cp))

    queue = MemoryRunQueue()
    run = await queue.enqueue("optimize", {"waves": 0})
    print(f"enqueued run {run.run_id[:8]} on the control plane\n")

    worker = Worker(queue, registry)
    while await worker.run_once():
        pass

    rec = await queue.get(run.run_id)
    print(f"\nrun status: {rec.status}")


if __name__ == "__main__":
    asyncio.run(main())
