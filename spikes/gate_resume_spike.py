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

"""Spike: does a step's attempt counter survive a crash + resume?

The claim under test (design notes §3 p.11): expanding a step into a quality-gate
sub-graph keeps the attempt count in durable state, whereas a retry loop that
lives inside the step handler loses it when the process dies mid-step. The
spike measures the difference side by side. Hermetic: a scripted backend double,
no CLI, no LLM.

Both variants run the SAME plan-as-data graph (``add_plan_executor``) with ONE
step whose checks can never pass, a budget of ``MAX_ATTEMPTS = 3`` attempts, and
the SAME fault: the backend call number 2 raises once ("the worker died in the
middle of attempt 2"). The run is then resumed.

  A. loop-in-handler : the handler loops ``attempt = 1..3`` itself.
  C. gate-subgraph   : the handler delegates to a compiled sub-graph
                       ``execute -> add_quality_gate(check) -> execute | done |
                       give_up`` running on a deterministic sub-thread. The
                       attempt count lives in the sub-graph's own checkpoint.

What is measured: how many attempts reached the checks in total, and whether
that stayed within the budget of 3.

HONEST SCOPE:
  * "Crash" is a Crash exception raised from the backend call, which fails the
    step exactly the way a dying worker would from the checkpointer's point of
    view (the step is not committed). It does not kill a real process.
  * Only one fault position is tried (call 2). The conclusion generalises
    because the argument is structural (where the counter is stored), but the
    spike does not enumerate every crash point.
  * The sub-graph delegation re-implements, in ~10 lines of the handler, the
    "done / resume / start" decision that ``Graph.add_subgraph`` makes for a
    sub-graph used as a NODE. A plan executor handler is not a node, so that
    wrapper cannot be reused as-is; a production layer would factor this out.
  * One gate per sub-graph invocation: the attempt counter is keyed by the gate
    name, and every step runs in its own sub-thread with its own state, so steps
    do not share counters. Putting several steps through ONE flat gate would.

Run it directly:

    python spikes/gate_resume_spike.py

It prints the comparison and asserts the outcome on BOTH an in-memory and a
SQLite checkpointer. It is a spike, not production code or a test.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from typing import Annotated, Any

from _checks import checkpointer_factories, run_checks
from agentflow import END, START, GateEvent, Graph, Hooks, NodeError, last
from agentflow.prebuilt import (
    GateResult,
    GateState,
    PlanState,
    Task,
    TaskResult,
    add_plan_executor,
    add_quality_gate,
)

MAX_ATTEMPTS = 3
CHECKS = [{"kind": "has_fields", "target": "records", "fields": ["id", "ts"]}]
STEP_ID = "s1"


class Crash(RuntimeError):
    """Stands in for 'the worker died in the middle of the step'."""


@dataclass
class FlakyBackend:
    """Output never satisfies CHECKS; the ``crash_on_call``-th call raises once."""

    crash_on_call: int = 2
    calls: list[int] = field(default_factory=list)  # attempt number of every call
    feedbacks: list[str | None] = field(default_factory=list)
    _crashed: bool = False

    async def run(self, attempt: int, feedback: str | None) -> dict[str, Any]:
        self.calls.append(attempt)
        self.feedbacks.append(feedback)
        if len(self.calls) == self.crash_on_call and not self._crashed:
            self._crashed = True
            raise Crash(f"worker died during call #{len(self.calls)} (attempt {attempt})")
        return {"records": [{"id": len(self.calls)}]}  # never has "ts" -> checks fail


class GateTrace(Hooks):
    """Collects GateEvent phases — the free trace the gate sub-graph produces."""

    def __init__(self) -> None:
        self.phases: list[tuple[str, int]] = []

    async def on_event(self, thread: str, step: int, node: str, event: Any) -> None:
        if isinstance(event, GateEvent):
            self.phases.append((event.phase, event.attempt))


# ---------------------------------------------------------------------------
# The parent graph: identical for both variants, only the handler differs.
# ---------------------------------------------------------------------------


def build_parent(handler, checkpointer):
    async def planner(state, ctx):
        if state.get("plan", {}).get("tasks"):
            return {}
        return {"plan": {"tasks": [Task(STEP_ID, "agent_step", input={"checks": CHECKS})]}}

    async def idle(state, ctx):
        return {}

    g = Graph(PlanState)
    g.add_node("planner", planner)
    add_plan_executor(g, handlers={"agent_step": handler}, on_idle="idle", max_fanout=1)
    g.add_node("idle", idle)
    g.add_edge(START, "planner")
    g.add_edge("planner", "dispatch")
    g.add_edge("idle", END)
    return g.compile(checkpointer=checkpointer, step_limit=50)


# ---------------------------------------------------------------------------
# Variant A: the retry loop lives inside the handler.
# ---------------------------------------------------------------------------


def make_loop_handler(backend: FlakyBackend, evaluated: list[int]):
    async def handler(task: Task, state: dict, ctx: Any) -> TaskResult:
        feedback: str | None = None
        reasons: list[str] = []
        for attempt in range(1, MAX_ATTEMPTS + 1):  # <- the counter is a local variable
            output = await backend.run(attempt, feedback)
            passed, reasons = run_checks(output, task.input["checks"])
            evaluated.append(attempt)
            if passed:
                return TaskResult(task.id, output={"attempts": attempt})
            feedback = "; ".join(reasons)
        return TaskResult(task.id, failed=True, error=f"escalated after {MAX_ATTEMPTS}: {reasons}")

    return handler


# ---------------------------------------------------------------------------
# Variant C: the handler delegates to a gate sub-graph on a durable sub-thread.
# ---------------------------------------------------------------------------


class StepState(GateState):
    output: Annotated[dict, last]
    status: Annotated[str, last]


def build_step_graph(backend: FlakyBackend, evaluated: list[int], checkpointer, hooks):
    def attempt_of(state) -> int:
        return int((state.get("gate_attempts") or {}).get("check", 0)) + 1

    async def execute(state, ctx):
        feedback = ((state.get("gate_results") or {}).get("check") or {}).get("feedback")
        return {"output": await backend.run(attempt_of(state), feedback)}

    async def evaluator(state, ctx) -> GateResult:
        passed, reasons = run_checks(state["output"], CHECKS)
        evaluated.append(attempt_of(state))
        return GateResult(passed=passed, reasons=reasons, feedback="; ".join(reasons) or None)

    async def done(state, ctx):
        return {"status": "passed"}

    async def give_up(state, ctx):
        return {"status": "escalated"}

    g = Graph(StepState)
    g.add_node("execute", execute)
    g.add_node("done", done)
    g.add_node("give_up", give_up)
    add_quality_gate(
        g,
        "check",
        evaluator,
        on_pass="done",
        on_fail="execute",
        on_escalate="give_up",
        max_attempts=MAX_ATTEMPTS,
    )
    g.add_edge(START, "execute")
    g.add_edge("execute", "check")
    g.add_edge("done", END)
    g.add_edge("give_up", END)
    return g.compile(checkpointer=checkpointer, hooks=hooks)


def make_gate_handler(child):
    async def handler(task: Task, state: dict, ctx: Any) -> TaskResult:
        sub_thread = f"{ctx.thread}::step::{task.id}"  # deterministic -> findable after a crash
        cp = await child.checkpointer.get(sub_thread)
        if cp is not None and cp.done:
            final = dict(cp.state)  # finished before the crash: do not run it again
        elif cp is not None:
            final = await child.resume(sub_thread, owner_check=ctx.owner_check)  # continue
        else:
            final = await child.invoke({}, thread=sub_thread, owner_check=ctx.owner_check)
        if final["status"] == "passed":
            return TaskResult(task.id, output={"status": "passed"})
        reasons = final["gate_results"]["check"]["reasons"]
        return TaskResult(task.id, failed=True, error=f"escalated after {MAX_ATTEMPTS}: {reasons}")

    return handler


# ---------------------------------------------------------------------------
# The measurement.
# ---------------------------------------------------------------------------


@dataclass
class Outcome:
    label: str
    calls: list[int]
    evaluated: list[int]
    result: dict[str, Any]
    gate_attempts_at_crash: Any = None
    phases: list[tuple[str, int]] = field(default_factory=list)


async def crash_then_resume(graph) -> dict[str, Any]:
    """Run until the injected crash, then resume the same thread to the end."""
    try:
        await graph.invoke({}, thread="run")
    except NodeError as exc:
        assert isinstance(exc.__cause__, Crash), f"expected the injected crash, got {exc!r}"
    else:  # pragma: no cover - the fault is injected, so this would be a spike bug
        raise AssertionError("the injected crash did not happen")
    return await graph.resume("run")


async def measure_loop(make_cp) -> Outcome:
    backend, evaluated = FlakyBackend(), []
    graph = build_parent(make_loop_handler(backend, evaluated), make_cp())
    final = await crash_then_resume(graph)
    return Outcome("A loop-in-handler", backend.calls, evaluated, final["plan"]["results"][0])


async def measure_gate(make_cp) -> Outcome:
    backend, evaluated, trace = FlakyBackend(), [], GateTrace()
    cp = make_cp()
    child = build_step_graph(backend, evaluated, cp, trace)
    graph = build_parent(make_gate_handler(child), cp)

    with contextlib.suppress(NodeError):
        await graph.invoke({}, thread="run")
    sub = await cp.get("run::step::s1")  # what the crash left behind in durable state
    at_crash = dict(sub.state.get("gate_attempts") or {})
    final = await graph.resume("run")
    await asyncio.sleep(0.05)  # GateEvents are delivered by fire-and-forget hook tasks
    return Outcome(
        "C gate-subgraph",
        backend.calls,
        evaluated,
        final["plan"]["results"][0],
        gate_attempts_at_crash=at_crash,
        phases=trace.phases,
    )


def show(label: str, a: Outcome, c: Outcome) -> None:
    print(f"=== [{label}] crash on backend call #2, then resume (budget = {MAX_ATTEMPTS}) ===")
    print(f"  {'variant':<20}{'backend calls':<22}{'attempts evaluated':<22}within budget?")
    for o in (a, c):
        n = len(o.evaluated)
        ok = "yes" if n <= MAX_ATTEMPTS else f"NO ({n} > {MAX_ATTEMPTS})"
        print(f"  {o.label:<20}{str(o.calls):<22}{str(o.evaluated):<22}{ok}")
    print(f"  C: gate_attempts persisted at the crash: {c.gate_attempts_at_crash}")
    print(f"  C: gate events: {c.phases}")


async def run_once(label: str, make_cp) -> None:
    a = await measure_loop(make_cp)
    c = await measure_gate(make_cp)
    show(label, a, c)

    # A: the counter was a local variable; the resume restarted it at 1, so the
    # step evaluated 1 attempt before the crash and 3 after — more than the budget.
    assert a.calls == [1, 2, 1, 2, 3], a.calls
    assert a.evaluated == [1, 1, 2, 3], a.evaluated
    assert len(a.evaluated) > MAX_ATTEMPTS, "the loop-in-handler overran its budget"

    # C: the counter was durable; after the crash the step carried on with attempt 2.
    assert c.gate_attempts_at_crash == {"check": 1}, c.gate_attempts_at_crash
    assert c.calls == [1, 2, 2, 3], c.calls  # call #2 crashed; attempt 2 was retried
    assert c.evaluated == [1, 2, 3], c.evaluated
    assert len(c.evaluated) == MAX_ATTEMPTS, "the gate stayed exactly within budget"
    assert c.phases == [
        ("started", 1),
        ("failed", 1),
        ("started", 2),
        ("failed", 2),
        ("started", 3),
        ("escalated", 3),
    ], c.phases

    # Both end honestly: the step is FAILED with a reason, not silently passed.
    for o in (a, c):
        assert o.result["failed"] is True and "has_fields" in o.result["error"], o.result
    print("  OK\n")


async def main() -> None:
    for label, make_cp in checkpointer_factories():
        await run_once(label, make_cp)
    print("All spike assertions passed on both checkpointers.")


if __name__ == "__main__":
    asyncio.run(main())
