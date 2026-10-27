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

"""Spike: the failure path of plan-as-data — an honest stop, not a silent pass.

Design notes §3 p.3/p.11 promise: a step that cannot satisfy its checks within
``maxIterations`` ends the plan with an explicit reason, and nothing downstream
of it pretends to have run. This spike measures that against the real engine.

The plan (a diamond with a tail and an independent side branch)::

    collect ──► good ──► audit            (audit needs only `good`)
        │         └────► report ──► publish
        └──► bad ───────┘                 (bad never passes its checks)

Expected, and asserted:
  * ``bad`` exhausts its attempts and ends FAILED, carrying the check reasons.
  * ``report`` (needs good AND bad) and ``publish`` (needs report) are blocked
    transitively and the backend is NEVER called for them.
  * ``audit`` and ``good`` are unaffected: independent work still finishes.
  * the run does not raise; ``plan_stalled`` is true and ``plan_complete`` false,
    and a terminal node records a structured stop record listing what failed
    and what was blocked (the "honest stop" artifact a UI would show).

HONEST SCOPE:
  * The retry loop lives in the handler here (as in ``plan_as_data_spike.py``);
    whether its counter survives a crash is measured in ``gate_resume_spike.py``.
    This spike is about what happens AFTER the attempts are used up.
  * "Blocked" is derived from the plan data (tasks not in ``done_ids``). That is
    exactly what ``plan_stalled`` means; the spike does not distinguish "blocked
    by a failed prerequisite" from "blocked by a dependency cycle" — a plan
    validator must reject cycles before execution (design notes, DAG-only v1).
  * A step failure here is a ``TaskResult(failed=True)``. A handler that RAISES
    is a different path (the whole round is discarded, see ``idempotency_spike``).

Run it directly:

    python spikes/failure_path_spike.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Annotated, Any

from _checks import checkpointer_factories, run_checks
from agentflow import END, START, Graph, last
from agentflow.prebuilt import (
    PlanState,
    Task,
    TaskResult,
    add_plan_executor,
    plan_complete,
    plan_stalled,
)

MAX_ATTEMPTS = 3


@dataclass
class ScriptedBackend:
    """Per-step outputs; the last scripted output repeats. Records every call."""

    scripts: dict[str, list[dict[str, Any]]]
    calls: list[str] = field(default_factory=list)

    async def run(self, step_id: str, attempt: int, feedback: str | None) -> dict[str, Any]:
        self.calls.append(f"{step_id}#{attempt}")
        await asyncio.sleep(0)
        outputs = self.scripts[step_id]
        return outputs[min(attempt - 1, len(outputs) - 1)]


def has_rows(target: str = "rows") -> list[dict[str, Any]]:
    return [{"kind": "not_empty", "target": target}]


PLAN: list[Task] = [
    Task("collect", "agent_step", input={"checks": has_rows()}),
    Task("good", "agent_step", prerequisites=("collect",), input={"checks": has_rows()}),
    Task("bad", "agent_step", prerequisites=("collect",), input={"checks": has_rows()}),
    Task("audit", "agent_step", prerequisites=("good",), input={"checks": has_rows()}),
    Task("report", "agent_step", prerequisites=("good", "bad"), input={"checks": has_rows()}),
    Task("publish", "agent_step", prerequisites=("report",), input={"checks": has_rows()}),
]

SCRIPTS: dict[str, list[dict[str, Any]]] = {
    "collect": [{"rows": [1]}],
    "good": [{"rows": [2]}],
    "bad": [{"rows": []}],  # always empty -> not_empty fails on every attempt
    "audit": [{"rows": [3]}],
    "report": [{"rows": [4]}],  # would pass — but must never be reached
    "publish": [{"rows": [5]}],
}


class FailureState(PlanState):
    outcome: Annotated[dict, last]


def build(backend: ScriptedBackend, checkpointer):
    async def handler(task: Task, state: dict, ctx: Any) -> TaskResult:
        feedback: str | None = None
        reasons: list[str] = []
        for attempt in range(1, MAX_ATTEMPTS + 1):
            output = await backend.run(task.id, attempt, feedback)
            passed, reasons = run_checks(output, task.input["checks"])
            if passed:
                return TaskResult(task.id, output={"value": output, "attempts": attempt})
            feedback = "; ".join(reasons)
        return TaskResult(
            task.id, failed=True, error=f"checks failed after {MAX_ATTEMPTS} attempts: {reasons}"
        )

    async def planner(state, ctx):
        if state.get("plan", {}).get("tasks"):
            return {}
        return {"plan": {"tasks": list(PLAN)}}

    async def stop_record(state, ctx):
        """The terminal node: turn plan data into an explicit, structured verdict."""
        plan = state["plan"]
        done = set(plan["done_ids"])
        failed = {r["task_id"]: r["error"] for r in plan["results"] if r["failed"]}
        blocked = [t["id"] for t in plan["tasks"] if t["id"] not in done]
        status = "complete" if plan_complete(state) else ("stopped" if plan_stalled(state) else "?")
        return {"outcome": {"status": status, "failed": failed, "blocked": blocked}}

    g = Graph(FailureState)
    g.add_node("planner", planner)
    add_plan_executor(g, handlers={"agent_step": handler}, on_idle="stop_record", max_fanout=4)
    g.add_node("stop_record", stop_record)
    g.add_edge(START, "planner")
    g.add_edge("planner", "dispatch")
    g.add_edge("stop_record", END)
    return g.compile(checkpointer=checkpointer, step_limit=4 * len(PLAN) + 10)


async def run_once(label: str, make_cp) -> None:
    backend = ScriptedBackend(SCRIPTS)
    final = await build(backend, make_cp()).invoke({}, thread="failure-path")  # must NOT raise

    plan = final["plan"]
    results = {r["task_id"]: r for r in plan["results"]}
    outcome = final["outcome"]

    print(f"=== [{label}] ===")
    print(f"  backend calls : {', '.join(backend.calls)}")
    print(f"  finished OK   : {sorted(t for t, r in results.items() if not r['failed'])}")
    print(f"  FAILED        : {outcome['failed']}")
    print(f"  blocked       : {outcome['blocked']}")
    print(
        f"  status        : {outcome['status']}  "
        f"(plan_complete={plan_complete(final)}, plan_stalled={plan_stalled(final)})"
    )

    # The failing step used exactly its budget and kept the reason.
    bad_calls = [c for c in backend.calls if c.startswith("bad#")]
    assert bad_calls == ["bad#1", "bad#2", "bad#3"], bad_calls
    assert results["bad"]["failed"] is True
    assert "not_empty(rows)" in results["bad"]["error"] and "empty" in results["bad"]["error"]

    # Everything downstream of it is blocked, transitively, and never invoked.
    assert outcome["blocked"] == ["report", "publish"], outcome["blocked"]
    assert not any(c.startswith(("report#", "publish#")) for c in backend.calls), backend.calls
    assert "report" not in results and "publish" not in results

    # Independent work is untouched.
    for step in ("collect", "good", "audit"):
        assert results[step]["failed"] is False, step
    assert set(plan["done_ids"]) == {"collect", "good", "bad", "audit"}, plan["done_ids"]

    # The stop is explicit, not a silent pass.
    assert plan_stalled(final) and not plan_complete(final)
    assert outcome["status"] == "stopped" and list(outcome["failed"]) == ["bad"]
    print("  OK\n")


async def main() -> None:
    for label, make_cp in checkpointer_factories():
        await run_once(label, make_cp)
    print("All spike assertions passed on both checkpointers.")


if __name__ == "__main__":
    asyncio.run(main())
