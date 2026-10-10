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

"""Tests for the plan_executor prebuilt (plan-as-data DAG executor).

Covers dependency ordering (linear, parallel, diamond), real concurrency of
independent tasks within one super-step, failed-prerequisite blocking, stall
detection, the re-plan loop, resume determinism (frontier derived from
done_ids), and task.id as the idempotency key.
"""

from __future__ import annotations

from typing import Annotated

from agentflow import END, START, Graph, MemoryCheckpointer, last
from agentflow.prebuilt import (
    PlanState,
    Task,
    TaskResult,
    add_plan_executor,
    plan_complete,
    plan_stalled,
)


class S(PlanState):
    seeded: Annotated[bool, last]


def _build(tasks, handlers, *, max_fanout=8, checkpointer=None):
    """A standard harness: a one-shot planner that seeds `tasks`, the executor,
    and a gate at on_idle that ends when the plan is complete or stalled."""

    async def planner(state, ctx):
        if state.get("seeded"):
            return {}
        return {"plan": {"tasks": list(tasks)}, "seeded": True}

    async def gate(state, ctx):
        return {}

    def gate_route(state):
        return "stop" if (plan_complete(state) or plan_stalled(state)) else "go"

    g = Graph(S)
    g.add_node("planner", planner)
    add_plan_executor(g, handlers=handlers, on_idle="gate", max_fanout=max_fanout)
    g.add_node("gate", gate)
    g.add_conditional_edges("gate", gate_route, {"stop": END, "go": "planner"})
    g.add_edge(START, "planner")
    g.add_edge("planner", "dispatch")
    return g.compile(checkpointer=checkpointer)


# --- dependency ordering ---------------------------------------------------


async def test_linear_chain_runs_in_order():
    order = []

    async def h(task, state, ctx):
        order.append(task.id)
        return TaskResult(task.id)

    tasks = [
        Task("a", "k"),
        Task("b", "k", prerequisites=("a",)),
        Task("c", "k", prerequisites=("b",)),
    ]
    out = await _build(tasks, {"k": h}).invoke({})
    assert order == ["a", "b", "c"]
    assert sorted(out["plan"]["done_ids"]) == ["a", "b", "c"]
    assert plan_complete(out)


async def test_independent_tasks_all_run():
    seen = set()

    async def h(task, state, ctx):
        seen.add(task.id)
        return TaskResult(task.id, output=task.input)

    tasks = [Task(f"t{i}", "k", input=i) for i in range(5)]
    out = await _build(tasks, {"k": h}).invoke({})
    assert seen == {f"t{i}" for i in range(5)}
    outputs = {r["task_id"]: r["output"] for r in out["plan"]["results"]}
    assert outputs == {f"t{i}": i for i in range(5)}


async def test_diamond_dependency_order():
    order = []

    async def h(task, state, ctx):
        order.append(task.id)
        return TaskResult(task.id)

    tasks = [
        Task("a", "k"),
        Task("b", "k", prerequisites=("a",)),
        Task("c", "k", prerequisites=("a",)),
        Task("d", "k", prerequisites=("b", "c")),
    ]
    out = await _build(tasks, {"k": h}).invoke({})
    # a before b,c before d; b,c order between themselves is not asserted
    assert order[0] == "a"
    assert order[-1] == "d"
    assert set(order[1:3]) == {"b", "c"}
    assert plan_complete(out)


# --- concurrency -----------------------------------------------------------


async def test_independent_tasks_run_in_one_super_step():
    """Independent tasks occupy slots in the SAME super-step (true fan-out)."""

    async def h(task, state, ctx):
        return TaskResult(task.id)

    tasks = [Task(f"t{i}", "k") for i in range(3)]
    app = _build(tasks, {"k": h})

    steps_with_slots: dict[int, int] = {}
    async for ev in app.stream({}):
        if ev.kind == "node_start" and "__run_" in (ev.node or ""):
            steps_with_slots[ev.step] = steps_with_slots.get(ev.step, 0) + 1
    # all three ran together in one step
    assert 3 in steps_with_slots.values()


async def test_max_fanout_caps_concurrency():
    """With max_fanout=2, five independent tasks run at most 2 per super-step."""

    async def h(task, state, ctx):
        return TaskResult(task.id)

    tasks = [Task(f"t{i}", "k") for i in range(5)]
    app = _build(tasks, {"k": h}, max_fanout=2)

    per_step: dict[int, int] = {}
    async for ev in app.stream({}):
        if ev.kind == "node_start" and "__run_" in (ev.node or ""):
            per_step[ev.step] = per_step.get(ev.step, 0) + 1
    assert per_step  # some slots ran
    assert max(per_step.values()) <= 2
    assert sum(per_step.values()) == 5  # all tasks ran, just batched


# --- failure and stall -----------------------------------------------------


async def test_failed_task_blocks_dependents():
    ran = []

    async def h(task, state, ctx):
        ran.append(task.id)
        if task.id == "a":
            return TaskResult(task.id, failed=True, error="boom")
        return TaskResult(task.id)

    tasks = [
        Task("a", "k"),
        Task("b", "k", prerequisites=("a",)),  # depends on the failed task
        Task("c", "k"),  # independent, should still run
    ]
    out = await _build(tasks, {"k": h}).invoke({})
    assert "a" in ran and "c" in ran
    assert "b" not in ran  # blocked by failed prerequisite
    # a and c are done; b never ran
    assert sorted(out["plan"]["done_ids"]) == ["a", "c"]
    assert not plan_complete(out)  # b is still unfinished
    assert plan_stalled(out)  # ...but nothing is runnable


async def test_missing_handler_fails_task_not_run():
    async def h(task, state, ctx):
        return TaskResult(task.id)

    tasks = [Task("a", "unknown_kind")]
    out = await _build(tasks, {"k": h}).invoke({})
    results = out["plan"]["results"]
    assert len(results) == 1
    assert results[0]["failed"] is True
    assert "unknown_kind" in results[0]["error"]


async def test_cycle_is_stalled_not_spinning():
    async def h(task, state, ctx):
        return TaskResult(task.id)

    # a and b depend on each other: neither is ever runnable
    tasks = [
        Task("a", "k", prerequisites=("b",)),
        Task("b", "k", prerequisites=("a",)),
    ]
    out = await _build(tasks, {"k": h}).invoke({})
    assert out["plan"].get("done_ids", set()) == set() or not out["plan"]["done_ids"]
    assert plan_stalled(out)
    assert not plan_complete(out)


# --- re-plan loop ----------------------------------------------------------


async def test_planner_can_propose_more_after_results():
    """A planner at on_idle proposes a second wave once the first finishes.

    This is the re-plan shape: the executor routes to on_idle (here the planner
    itself) when nothing is runnable, and the planner observes results and
    either proposes more tasks or routes to END. The planner owns the stop
    decision — it knows what it has yet to propose.
    """

    async def h(task, state, ctx):
        return TaskResult(task.id, output=task.id.upper())

    async def planner(state, ctx):
        done = set(state.get("plan", {}).get("done_ids") or set())
        waves = state.get("waves", 0)
        if not done:
            return {"plan": {"tasks": [Task("a", "k"), Task("b", "k")]}, "waves": 1}
        if done == {"a", "b"}:
            # second wave, now that wave one is in
            return {
                "plan": {"tasks": [Task("c", "k", prerequisites=("a", "b"))]},
                "waves": waves + 1,
            }
        return {"waves": waves + 1}  # nothing more to add

    def planner_route(state):
        # keep going while there is runnable work or more to propose; the
        # planner stops when the plan is complete and it has proposed c
        if plan_complete(state) and "c" in (state.get("plan", {}).get("done_ids") or set()):
            return "stop"
        return "run"

    class RS(PlanState):
        waves: Annotated[int, last]

    g = Graph(RS)
    g.add_node("planner", planner)
    add_plan_executor(g, handlers={"k": h}, on_idle="planner")
    g.add_conditional_edges("planner", planner_route, {"run": "dispatch", "stop": END})
    g.add_edge(START, "planner")

    out = await g.compile(step_limit=200).invoke({})
    assert sorted(out["plan"]["done_ids"]) == ["a", "b", "c"]
    assert plan_complete(out)


# --- resume determinism ----------------------------------------------------


async def test_resume_does_not_rerun_completed_tasks():
    """Suspend mid-plan, resume, and confirm finished tasks are not re-run.

    One handler interrupts on its first visit (standing in for a crash point).
    After resume, the already-done tasks must not execute again — the runnable
    frontier is derived from the durable done_ids, so completed work is skipped.
    """
    runs: list[str] = []

    async def h(task, state, ctx):
        runs.append(task.id)
        if task.id == "b":
            # suspend once, right after a has completed and been checkpointed
            await ctx.interrupt({"pause": task.id})
        return TaskResult(task.id)

    tasks = [
        Task("a", "k"),
        Task("b", "k", prerequisites=("a",)),
        Task("c", "k", prerequisites=("b",)),
    ]
    cp = MemoryCheckpointer()
    app = _build(tasks, {"k": h}, checkpointer=cp)

    await app.invoke({}, thread="r1")
    state = await app.get_state("r1")
    assert state.interrupted  # parked inside task b
    assert "a" in state.state["plan"]["done_ids"]  # a already durable
    runs_before = list(runs)
    assert runs_before.count("a") == 1

    out = await app.resume("r1", value=None)
    # a must not have run again after resume
    assert runs.count("a") == 1
    assert sorted(out["plan"]["done_ids"]) == ["a", "b", "c"]
    assert plan_complete(out)


# --- idempotency key -------------------------------------------------------


async def test_task_id_is_the_stable_dedup_key():
    """A handler keyed by task.id runs the effect once per id across the plan."""
    effects: dict[str, int] = {}

    async def h(task, state, ctx):
        # task.id is the stable idempotency key the design promises
        effects[task.id] = effects.get(task.id, 0) + 1
        return TaskResult(task.id)

    tasks = [
        Task("x", "k"),
        Task("y", "k", prerequisites=("x",)),
    ]
    await _build(tasks, {"k": h}).invoke({})
    assert effects == {"x": 1, "y": 1}  # each id executed exactly once


# --- builder validation ----------------------------------------------------


async def test_duplicate_node_name_rejected():
    import pytest

    g = Graph(S)

    async def planner(state, ctx):
        return {}

    g.add_node("dispatch", planner)  # collide with the executor's node
    with pytest.raises(ValueError, match="dispatch"):
        add_plan_executor(g, handlers={}, on_idle="x")


async def test_max_fanout_must_be_positive():
    import pytest

    g = Graph(S)
    with pytest.raises(ValueError, match="max_fanout"):
        add_plan_executor(g, handlers={}, on_idle="x", max_fanout=0)


# --- durable checkpointer composition --------------------------------------


async def test_plan_is_json_serializable_in_state():
    """Tasks and results are stored as plain dicts, so a durable (JSON)
    checkpointer can persist the plan. Regression: they used to be stored as
    dataclasses and broke file/sqlite/postgres/redis checkpointers."""
    import json

    async def h(task, state, ctx):
        return TaskResult(task.id, output={"ok": True})

    tasks = [Task("a", "k"), Task("b", "k", prerequisites=("a",))]
    out = await _build(tasks, {"k": h}).invoke({})
    plan = out["plan"]
    assert all(isinstance(t, dict) for t in plan["tasks"])
    assert all(isinstance(r, dict) for r in plan["results"])
    json.dumps(out["plan"])  # the plan channel must round-trip through JSON


async def test_plan_executor_survives_a_sqlite_checkpointer(tmp_path):
    from agentflow import SqliteCheckpointer

    async def h(task, state, ctx):
        return TaskResult(task.id, output=task.id.upper())

    tasks = [Task("a", "k"), Task("b", "k", prerequisites=("a",))]
    cp = SqliteCheckpointer(str(tmp_path / "plan.db"))
    out = await _build(tasks, {"k": h}, checkpointer=cp).invoke({}, thread="p1")
    assert sorted(out["plan"]["done_ids"]) == ["a", "b"]
    reloaded = await _build(tasks, {"k": h}, checkpointer=cp).get_state("p1")
    assert sorted(reloaded.state["plan"]["done_ids"]) == ["a", "b"]
