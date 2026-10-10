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

"""Plan-as-data execution: a planner emits a task DAG, the engine runs it.

Run: python examples/plan_executor_demo.py

A tiny build pipeline expressed as a dependency graph of tasks:

    fetch ──> compile ──> test ──┐
                 └────> lint ────┴──> package

The planner proposes all of it up front as Task objects with prerequisites.
`add_plan_executor` runs each task once its prerequisites are done, fanning the
independent ones (test, lint) out concurrently in one super-step, and loops
until nothing is runnable. No graph node is added at runtime — a task is data.

Everything is offline: each handler just records that it ran and returns a
TaskResult. Swap a handler body for real work (a subprocess, an API call) and
the plan machinery is unchanged.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from agentflow import END, START, Graph, last
from agentflow.prebuilt import (
    PlanState,
    Task,
    TaskResult,
    add_plan_executor,
    plan_complete,
    plan_stalled,
)


class BuildState(PlanState):
    seeded: Annotated[bool, last]


async def run_step(task: Task, state, ctx) -> TaskResult:
    # a real handler would do the work for task.kind here
    print(f"  run {task.id}")
    return TaskResult(task.id, output=f"{task.id}-ok")


async def planner(state, ctx) -> dict:
    if state.get("seeded"):
        return {}
    tasks = [
        Task("fetch", "step"),
        Task("compile", "step", prerequisites=("fetch",)),
        Task("test", "step", prerequisites=("compile",)),
        Task("lint", "step", prerequisites=("compile",)),
        Task("package", "step", prerequisites=("test", "lint")),
    ]
    print("planner: proposed a 5-task build DAG")
    return {"plan": {"tasks": tasks}, "seeded": True}


async def report(state, ctx) -> dict:
    return {}


def report_route(state) -> str:
    if plan_stalled(state):
        print("report: stalled — some tasks are blocked")
    elif plan_complete(state):
        print("report: all tasks done")
    return "stop"


def build() -> Graph:
    g = Graph(BuildState)
    g.add_node("planner", planner)
    # one handler kind, "step"; max_fanout caps concurrent slots
    add_plan_executor(g, handlers={"step": run_step}, on_idle="report", max_fanout=4)
    g.add_node("report", report)
    g.add_conditional_edges("report", report_route, {"stop": END})
    g.add_edge(START, "planner")
    g.add_edge("planner", "dispatch")
    return g


async def main() -> None:
    out = await build().compile().invoke({})
    plan = out["plan"]
    print()
    print(f"done:    {sorted(plan['done_ids'])}")
    outs = ", ".join(f"{r['task_id']}={r['output']}" for r in plan["results"])
    print(f"outputs: {{{outs}}}")


if __name__ == "__main__":
    asyncio.run(main())
