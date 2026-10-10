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

"""Optimizer tutorial, chapter 10: plan-as-data at scale.

Run: python examples/tutorial/10_plan_executor.py

The finale. The planner stops proposing one move at a time and instead emits a
*dependency graph of experiments* as data: measure each single strategy first,
then measure the promising pairs that build on them. add_plan_executor runs the
graph by readiness — independent experiments fan out concurrently, dependents
wait for their prerequisites — and the planner re-plans from the results. The
whole optimizer then runs as a background job through the control plane: a
registry, a queue, and a worker, exactly as it would in production (where the
queue is Postgres and an HTTP server, chapter's end, accepts the submission).
"""

from __future__ import annotations

import asyncio
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

BASE_COST = 100.0
STRATEGY_FACTORS = {"add_index": 0.4, "push_filter": 0.6, "reorder_stages": 0.9}


def cost_of(plan) -> float:
    cost = BASE_COST
    for s in plan:
        cost *= STRATEGY_FACTORS.get(s, 1.0)
    return round(cost, 2)


class OptState(PlanState):
    waves: Annotated[int, last]
    best: Annotated[dict, last]


# --- handlers: each task measures one plan against the cost model ----------


async def measure(task: Task, state, ctx) -> TaskResult:
    plan = tuple(task.input["plan"])
    return TaskResult(task.id, output={"plan": list(plan), "cost": cost_of(plan)})


# --- the planner: emits a dependency DAG, then re-plans from results --------


async def planner(state: OptState, ctx) -> dict:
    plan_state = state.get("plan", {})
    results = plan_state.get("results", [])
    waves = state.get("waves", 0)

    if waves == 0:
        # wave 1: measure each single strategy (independent, fan out together)
        tasks = [Task(f"single:{s}", "measure", input={"plan": [s]}) for s in STRATEGY_FACTORS]
        print(f"planner wave 1: {len(tasks)} single-strategy experiments")
        return {"plan": {"tasks": tasks}, "waves": 1}

    if waves == 1:
        # wave 2: for the best single, measure pairs that extend it. Each pair
        # depends on BOTH its singles already being measured.
        singles = {r["output"]["plan"][0]: r["output"]["cost"] for r in results}
        best_single = min(singles, key=singles.get)
        pairs = [
            Task(
                f"pair:{best_single}+{s}",
                "measure",
                input={"plan": sorted([best_single, s])},
                prerequisites=(f"single:{best_single}", f"single:{s}"),
            )
            for s in STRATEGY_FACTORS
            if s != best_single
        ]
        print(f"planner wave 2: {len(pairs)} pair experiments extending {best_single!r}")
        return {"plan": {"tasks": pairs}, "waves": 2}

    return {"waves": waves + 1}  # nothing more to propose


def planner_route(state: OptState) -> str:
    # stop once the plan is complete and we have run both waves
    if plan_complete(state) and state.get("waves", 0) >= 2:
        return "stop"
    return "run"


async def summarize(state: OptState, ctx) -> dict:
    results = state["plan"]["results"]
    best = min(results, key=lambda r: r["output"]["cost"])["output"]
    print(
        f"best measured plan: {best['plan']} cost {best['cost']} (from {len(results)} experiments)"
    )
    return {"best": best}


def make_graph(checkpointer) -> Graph:
    g = Graph(OptState)
    g.add_node("planner", planner)
    add_plan_executor(g, handlers={"measure": measure}, on_idle="planner", max_fanout=4)
    g.add_node("summarize", summarize)
    g.add_conditional_edges("planner", planner_route, {"run": "dispatch", "stop": "summarize"})
    g.add_edge("summarize", END)
    g.add_edge(START, "planner")
    return g.compile(checkpointer=checkpointer)


async def main() -> None:
    import tempfile

    cp = FileCheckpointer(tempfile.mkdtemp())

    # Register the optimizer graph by name, enqueue a run, drain with a worker —
    # the control plane. In production the queue is PostgresRunQueue shared by a
    # pool of workers, and an HTTP server (agentflow.server.create_app) accepts
    # the enqueue over the network. Here it is all in-process and offline.
    registry = GraphRegistry()
    registry.register("optimize", lambda: make_graph(cp))

    queue = MemoryRunQueue()
    run = await queue.enqueue("optimize", {"waves": 0})
    print(f"enqueued run {run.run_id[:8]} on the control plane\n")

    worker = Worker(queue, registry)
    # drain the queue (this run plus the re-plan waves all live under one run)
    while await worker.run_once():
        pass

    rec = await queue.get(run.run_id)
    print(f"\nrun status: {rec.status}")


if __name__ == "__main__":
    asyncio.run(main())
