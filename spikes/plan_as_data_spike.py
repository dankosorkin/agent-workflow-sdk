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

"""Spike: "workflow from request" executed as plan-as-data, NOT codegen.

What this spike proves against the real engine (hermetic — no CLI/LLM):

  1. A plan is DATA (Task(id, kind, input, prerequisites)) that
     `add_plan_executor` runs by dependency readiness. No `workflow.py` is
     generated; the engine graph is fixed.
  2. Dependency data flow: a step reads its predecessors' outputs from
     `state["plan"]["results"]` and the final `report` step actually receives
     both branch outputs (review point 8 — inputs <- dependency outputs).
  3. Retry WITH feedback (not just retry): the failure reason from attempt 1's
     checks is handed to attempt 2, and the spike asserts the reason text
     arrived — the backend records every feedback it was given.
  4. Real parallelism: a concurrency counter shows the two independent `enrich`
     steps run at the same time (peak == 2), not merely in sequence.
  5. Deterministic `checks` run in plain code (no model), returning structured
     reasons.

HONEST SCOPE — read this before citing the spike:
  * The per-step retry loop here lives INSIDE the handler, not in a real
    `add_quality_gate` sub-graph. So this spike does NOT prove that a step's
    attempt count survives a crash/resume (the main argument for expanding a
    step into a gate sub-graph — design notes §3 p.11). That claim is proven
    separately in `spikes/gate_resume_spike.py`.
  * The engine's own gate routing (pass / fail-and-revise / escalate, attempts
    in state, GateEvents) is covered by `tests/test_gate.py`.

Run it directly:

    python spikes/plan_as_data_spike.py

It prints a trace and asserts the outcome on BOTH an in-memory and a SQLite
checkpointer. It is a spike, not production code or a test — it lives outside
the test suite on purpose.
"""

from __future__ import annotations

import asyncio
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from agentflow import END, START, Graph, MemoryCheckpointer
from agentflow.checkpoint.sqlite import SqliteCheckpointer
from agentflow.prebuilt import (
    PlanState,
    Task,
    TaskResult,
    add_plan_executor,
    plan_complete,
    plan_stalled,
)

# ---------------------------------------------------------------------------
# Fake backend double: a scripted "agent call". Replace with a real backend
# (KiroBackend / ClaudeCodeBackend / an LLMBackend) to go live.
# ---------------------------------------------------------------------------


@dataclass
class FakeBackend:
    """Scripted agent double that records calls, the feedback it was given, and
    concurrency.

    ``scripts[step_id]`` is a list of outputs, one per attempt, so a step can
    fail its checks on attempt 1 and pass on attempt 2. ``feedbacks[step_id]``
    records the feedback string handed to each attempt, so the spike can PROVE
    the failure reason flowed into the retry (not just that a retry happened).
    ``live``/``peak`` track simultaneous calls to prove real parallelism.
    """

    scripts: dict[str, list[dict[str, Any]]]
    calls: list[str] = field(default_factory=list)
    feedbacks: dict[str, list[str | None]] = field(default_factory=dict)
    live: int = 0
    peak: int = 0

    async def run(
        self,
        step_id: str,
        attempt: int,
        prompt: str,
        feedback: str | None,
        deps: dict[str, Any],
    ) -> dict:
        self.live += 1
        self.peak = max(self.peak, self.live)
        try:
            self.calls.append(f"{step_id}#{attempt}")
            self.feedbacks.setdefault(step_id, []).append(feedback)
            # Yield so genuinely-concurrent calls overlap (peak can reach 2).
            await asyncio.sleep(0.01)
            outputs = self.scripts.get(step_id, [{}])
            idx = min(attempt - 1, len(outputs) - 1)  # clamp past end of script
            out = dict(outputs[idx])
            # A real agent would consume `deps`; the report step echoes which
            # predecessor outputs it actually received so the spike can assert it.
            if step_id == "report":
                out = {"summary": f"branches seen: {sorted(deps)}", "deps_received": deps}
            return out
        finally:
            self.live -= 1


# ---------------------------------------------------------------------------
# Deterministic checks (the declarative primitives, in miniature). Strict:
# a missing target is a FAILURE, never a silently-passing empty.
# ---------------------------------------------------------------------------

_MISSING = object()


def check_not_empty(output: dict, target: str) -> tuple[bool, str | None]:
    if target not in output:
        return False, f"target {target!r} missing"
    value = output[target]
    # "present and non-empty": empty string/list/dict/None fail; 0/False do NOT.
    empty = value is None or (hasattr(value, "__len__") and len(value) == 0)
    return (not empty), None if not empty else f"target {target!r} is empty"


def check_has_fields(output: dict, target: str, fields: list[str]) -> tuple[bool, str | None]:
    if target not in output:
        return False, f"target {target!r} missing"
    items = output[target]
    if not isinstance(items, list):
        return False, f"target {target!r} is not a list"
    for i, it in enumerate(items):
        missing = [f for f in fields if f not in it]
        if missing:
            return False, f"item {i} missing fields {missing}"
    return True, None


def run_checks(output: dict, checks: list[dict]) -> tuple[bool, list[str]]:
    """Run a step's declarative checks; collect structured failure reasons."""
    reasons: list[str] = []
    for c in checks:
        kind = c["kind"]
        if kind == "not_empty":
            ok, why = check_not_empty(output, c["target"])
        elif kind == "has_fields":
            ok, why = check_has_fields(output, c["target"], c["fields"])
        else:  # a real impl has ~13 primitives; the spike needs two
            ok, why = False, f"unknown check kind {kind!r}"
        if not ok and why:
            reasons.append(f"{kind}({c['target']}): {why}")
    return (not reasons), reasons


# ---------------------------------------------------------------------------
# The plan: data, not code. What wf-creator would emit as plan.json.
# A diamond so the spike proves parallel fan-out + join by dependency.
# ---------------------------------------------------------------------------

PLAN: list[Task] = [
    Task(
        id="collect",
        kind="agent_step",
        input={
            "prompt": "collect raw records",
            "checks": [
                {"kind": "not_empty", "target": "records"},
                {"kind": "has_fields", "target": "records", "fields": ["id", "ts"]},
            ],
        },
    ),
    Task(
        id="enrich_a",
        kind="agent_step",
        prerequisites=("collect",),
        input={
            "prompt": "enrich branch A",
            "checks": [{"kind": "not_empty", "target": "rows"}],
        },
    ),
    Task(
        id="enrich_b",
        kind="agent_step",
        prerequisites=("collect",),
        input={
            "prompt": "enrich branch B",
            "checks": [{"kind": "not_empty", "target": "rows"}],
        },
    ),
    Task(
        id="report",
        kind="agent_step",
        prerequisites=("enrich_a", "enrich_b"),
        input={
            "prompt": "write the final report",
            "checks": [{"kind": "not_empty", "target": "summary"}],
        },
    ),
]

# "collect" fails has_fields on attempt 1 (missing "ts"), passes on attempt 2.
SCRIPTS: dict[str, list[dict[str, Any]]] = {
    "collect": [
        {"records": [{"id": 1}, {"id": 2}]},  # attempt 1: no "ts"
        {"records": [{"id": 1, "ts": 10}, {"id": 2, "ts": 20}]},  # attempt 2: good
    ],
    "enrich_a": [{"rows": [1, 2, 3]}],
    "enrich_b": [{"rows": [4, 5]}],
    "report": [{}],  # overwritten by the backend to echo deps it received
}


def build(backend: FakeBackend, checkpointer):
    """One fixed engine graph: planner -> plan_executor(agent_step) -> idle gate."""

    async def handler(task: Task, state: dict, ctx: Any) -> TaskResult:
        checks = task.input["checks"]
        prompt = task.input["prompt"]
        # Dependency data flow: gather predecessors' outputs by task id.
        results = {r["task_id"]: r for r in state.get("plan", {}).get("results") or ()}
        deps = {
            dep: results[dep]["output"]["value"]
            for dep in task.prerequisites
            if dep in results and not results[dep]["failed"]
        }
        feedback: str | None = None
        max_attempts = 3
        reasons: list[str] = []
        for attempt in range(1, max_attempts + 1):
            output = await backend.run(task.id, attempt, prompt, feedback, deps)
            passed, reasons = run_checks(output, checks)
            if passed:
                return TaskResult(task.id, output={"value": output, "attempts": attempt})
            feedback = "; ".join(reasons)  # structured reason -> next attempt
        return TaskResult(
            task.id, failed=True, error=f"checks failed after {max_attempts}: {reasons}"
        )

    async def planner(state, ctx):
        if state.get("plan", {}).get("tasks"):
            return {}
        return {"plan": {"tasks": list(PLAN)}}

    async def idle(state, ctx):
        return {}

    def idle_route(state):
        return "stop" if (plan_complete(state) or plan_stalled(state)) else "go"

    g = Graph(PlanState)
    g.add_node("planner", planner)
    add_plan_executor(g, handlers={"agent_step": handler}, on_idle="idle", max_fanout=4)
    g.add_node("idle", idle)
    g.add_conditional_edges("idle", idle_route, {"stop": END, "go": "planner"})
    g.add_edge(START, "planner")
    g.add_edge("planner", "dispatch")
    # One executor round is 3 super-steps; size the limit from the plan so a
    # long plan does not hit the default ceiling (100).
    return g.compile(checkpointer=checkpointer, step_limit=4 * len(PLAN) + 10)


async def run_once(label: str, checkpointer) -> None:
    backend = FakeBackend(scripts=SCRIPTS)
    graph = build(backend, checkpointer)
    final = await graph.invoke({})
    results = {r["task_id"]: r for r in final["plan"]["results"]}

    print(f"=== [{label}] backend calls (step#attempt) ===")
    print("  " + ", ".join(backend.calls))
    print(f"  concurrency peak: {backend.peak}")
    print(f"  feedback to collect attempts: {backend.feedbacks['collect']}")

    # (1) plan ran as data, in dependency order, to completion.
    assert plan_complete(final), "plan did not complete"
    assert not any(r["failed"] for r in results.values()), "a step failed"

    # (2) dependency data flow: report actually RECEIVED both branch outputs.
    report_out = results["report"]["output"]["value"]
    assert report_out["deps_received"] == {
        "enrich_a": {"rows": [1, 2, 3]},
        "enrich_b": {"rows": [4, 5]},
    }, f"report did not receive both deps: {report_out['deps_received']}"

    # (3) retry WITH feedback: attempt 1 got no feedback; attempt 2 got the
    #     actual failure reason naming the missing field.
    fb = backend.feedbacks["collect"]
    assert fb[0] is None, "first attempt should have no feedback"
    assert fb[1] is not None and "ts" in fb[1], f"feedback did not carry reason: {fb[1]}"
    assert results["collect"]["output"]["attempts"] == 2
    assert results["enrich_a"]["output"]["attempts"] == 1

    # (4) real parallelism: the two enrich steps overlapped.
    assert backend.peak == 2, f"expected concurrency peak 2, got {backend.peak}"

    print("  OK: complete, deps delivered, feedback carried reason, peak==2\n")


async def main() -> None:
    await run_once("memory", MemoryCheckpointer())
    with tempfile.TemporaryDirectory() as d:
        await run_once("sqlite", SqliteCheckpointer(Path(d) / "spike.db"))
    print("All spike assertions passed on both checkpointers.")


if __name__ == "__main__":
    asyncio.run(main())
