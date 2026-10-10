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

"""Plan-as-data execution: run a dependency graph of tasks the planner emits.

The engine graph is static — a conditional edge may only fan out to targets
declared at compile time. This prebuilt gives you the other shape without
changing the engine: a *planner* writes a dependency graph of tasks into state,
and ``add_plan_executor`` runs them by readiness (a task runs once all its
prerequisites have finished), fanning independent tasks out concurrently,
collecting results, and looping until nothing is runnable.

The plan is data in a ``by_key`` channel, so it is checkpointed and a resume
simply recomputes which tasks are runnable from durable state. No node is added
to the graph at runtime; the handler pool is fixed at build time and a new task
is new *data*, never a new node.

    class OptState(PlanState):            # the plan channel comes from the mixin
        incumbent: Annotated[dict, last]

    async def try_index(task, state, ctx) -> TaskResult:
        return TaskResult(task.id, output=evaluate(task.input))

    g = Graph(OptState)
    g.add_node("planner", planner)        # writes plan.tasks (your code)
    add_plan_executor(g, handlers={"index": try_index}, on_idle="planner")
    g.add_edge(START, "planner")
    g.add_edge("planner", "dispatch")     # planner hands off to the executor

``planner`` proposes tasks; the executor runs them; ``on_idle`` (here the
planner again) is reached when no task is runnable, so the planner can observe
results and propose more, or route to ``END`` to stop. The executor owns only
the run mechanics (dispatch, run, collect); the propose/stop policy is yours.

Scope, stated plainly: this does not mutate the graph, is not a ``Send``-style
dynamic spawn, and is not a general scheduler — "runnable" means "dependencies
satisfied", nothing more. It gives at-least-once execution like the rest of the
SDK; make handlers idempotent (``task.id`` is the stable key to pass to
``IdempotentOp`` or a downstream dedup token) for exactly-once effects.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Annotated, Any

from agentflow.graph import Graph
from agentflow.state import State, by_key, last

__all__ = [
    "Task",
    "TaskResult",
    "PlanState",
    "Handler",
    "add_plan_executor",
    "plan_complete",
    "plan_stalled",
]


@dataclass(frozen=True)
class Task:
    """One unit of work in a plan.

    ``id`` is unique within a plan and must be *stable across a resume* — it is
    the dedup key, so a re-dispatched task is recognized as the same one. Let
    the planner choose a meaningful id (e.g. ``"try_index:orders"``); it doubles
    as a natural idempotency key. ``kind`` selects the handler. ``input`` is the
    payload the handler reads. ``prerequisites`` are task ids that must finish
    before this task becomes runnable.
    """

    id: str
    kind: str
    input: Any = None
    prerequisites: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """A JSON-serializable view, as stored in the ``plan.tasks`` channel."""
        return {
            "id": self.id,
            "kind": self.kind,
            "input": self.input,
            "prerequisites": list(self.prerequisites),
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> Task:
        return cls(
            id=d["id"],
            kind=d["kind"],
            input=d.get("input"),
            prerequisites=tuple(d.get("prerequisites") or ()),
        )


@dataclass(frozen=True)
class TaskResult:
    """A handler's result for one task.

    ``failed`` marks a task that finished unsuccessfully: it still counts as
    *done* (so the plan can make progress), but it blocks its dependents — they
    never become runnable. ``error`` carries a message for observability.
    """

    task_id: str
    output: Any = None
    failed: bool = False
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """A JSON-serializable view, as stored in the ``plan.results`` channel."""
        return {
            "task_id": self.task_id,
            "output": self.output,
            "failed": self.failed,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> TaskResult:
        return cls(
            task_id=d["task_id"],
            output=d.get("output"),
            failed=bool(d.get("failed", False)),
            error=d.get("error"),
        )


# A handler runs one task: async (task, state, ctx) -> TaskResult. It receives
# the whole state so it can read problem facts without threading them through
# every task input.
Handler = Callable[[Task, Mapping[str, Any], Any], Awaitable[TaskResult]]


def _append_tasks(current: Any, update: Any) -> list:
    """append, but coerce any Task objects to plain dicts (JSON-serializable)."""
    base = list(current) if current is not None else []
    items = update if isinstance(update, (list, tuple)) else [update]
    base.extend(t.to_dict() if isinstance(t, Task) else dict(t) for t in items)
    return base


def _append_results(current: Any, update: Any) -> list:
    """append, but coerce any TaskResult objects to plain dicts."""
    base = list(current) if current is not None else []
    items = update if isinstance(update, (list, tuple)) else [update]
    base.extend(r.to_dict() if isinstance(r, TaskResult) else dict(r) for r in items)
    return base


def _union_ids(current: Any, update: Any) -> list:
    """Set-union of task ids, kept as a sorted list so it is JSON-serializable.

    ``done_ids`` is a membership set conceptually, but a durable checkpointer
    stores state as JSON and JSON has no set type — so we hold it as a sorted,
    de-duplicated list and union new ids in.
    """
    seen = set(current) if current is not None else set()
    items = update if isinstance(update, (set, frozenset, list, tuple)) else {update}
    seen.update(items)
    return sorted(seen)


class PlanState(State):
    """Mix into a state schema to add the channel ``add_plan_executor`` needs.

    One structured ``plan`` channel holds the whole execution plan, each field
    folded by its own reducer via :func:`~agentflow.state.by_key`:

    - ``tasks`` (append): the planner's proposals accumulate — the backlog.
      Stored as plain dicts; a planner may append ergonomic :class:`Task`
      objects and the reducer coerces them, so the plan stays
      JSON-serializable for a durable checkpointer.
    - ``results`` (append): one result per completed task, stored as a plain
      dict (a :class:`TaskResult` is coerced on write). Read back as dicts, or
      via :meth:`TaskResult.from_dict`.
    - ``done_ids``: the finished task ids, unioned in and kept as a sorted
      list (JSON-serializable), for a fast readiness check without scanning
      ``results``.
    - ``assigned`` (last): transient slot -> task-id map for the current
      dispatch; overwritten each round and never trusted across a resume.

    The runnable frontier is derived from ``done_ids`` every super-step, never
    stored — which is what makes a resume need no special path.
    """

    plan: Annotated[
        dict,
        by_key(
            {
                # Tasks and results are stored as plain dicts so the plan is
                # JSON-serializable and survives a durable checkpointer. The
                # reducers coerce Task/TaskResult objects to dicts on write, so
                # a planner can still append ergonomic Task(...) objects.
                "tasks": _append_tasks,
                "results": _append_results,
                "done_ids": _union_ids,
                "assigned": last,
            }
        ),
    ]


# ---------------------------------------------------------------------------
# Derived views over the plan (pure; never stored)
# ---------------------------------------------------------------------------


def _plan(state: Mapping[str, Any]) -> dict[str, Any]:
    return dict(state.get("plan") or {})


def _tasks_by_id(plan: Mapping[str, Any]) -> dict[str, Task]:
    # tasks are stored as plain dicts (JSON-serializable); rebuild Task objects
    return {t["id"]: Task.from_dict(t) for t in plan.get("tasks") or ()}


def _failed_ids(plan: Mapping[str, Any]) -> set[str]:
    return {r["task_id"] for r in plan.get("results") or () if r.get("failed")}


def _runnable(plan: Mapping[str, Any]) -> list[Task]:
    """Tasks whose prerequisites are all done-and-not-failed, not themselves done.

    A failed prerequisite blocks its dependents: it is in ``done_ids`` but also
    in the failed set, so a dependent requiring it never satisfies the
    "prereqs are a subset of the *successful* done ids" test.
    """
    done: set[str] = set(plan.get("done_ids") or set())
    failed = _failed_ids(plan)
    succeeded = done - failed
    runnable: list[Task] = []
    for task in _tasks_by_id(plan).values():
        if task.id in done:
            continue
        if set(task.prerequisites).issubset(succeeded):
            runnable.append(task)
    return runnable


def _unfinished(plan: Mapping[str, Any]) -> list[Task]:
    done: set[str] = set(plan.get("done_ids") or set())
    return [t for t in _tasks_by_id(plan).values() if t.id not in done]


# ---------------------------------------------------------------------------
# Public status helpers (for a planner at on_idle to tell done from stalled)
# ---------------------------------------------------------------------------


def plan_complete(state: Mapping[str, Any]) -> bool:
    """True when every proposed task has finished (succeeded or failed)."""
    return not _unfinished(_plan(state))


def plan_stalled(state: Mapping[str, Any]) -> bool:
    """True when tasks remain unfinished but none is runnable.

    This means the remaining tasks are all blocked — by a failed prerequisite
    or a dependency cycle — so the plan cannot progress without new tasks. A
    planner reaching ``on_idle`` uses this to tell "nothing left to do" from
    "stuck", and can propose a different approach or escalate.
    """
    plan = _plan(state)
    return bool(_unfinished(plan)) and not _runnable(plan)


# ---------------------------------------------------------------------------
# The builder
# ---------------------------------------------------------------------------


def add_plan_executor(
    g: Graph,
    *,
    handlers: Mapping[str, Handler],
    on_idle: str,
    dispatch: str = "dispatch",
    collect: str = "collect",
    max_fanout: int = 8,
) -> Graph:
    """Add a plan-as-data executor to ``g`` and return ``g``.

    Creates a ``dispatch`` node, ``max_fanout`` run-slot nodes, and a
    ``collect`` node, wired as dispatch -> (slots) -> collect -> dispatch, with
    ``collect`` and ``dispatch`` routing to ``on_idle`` when no task is
    runnable. Your planner node (not created here) writes ``plan.tasks`` and
    hands off to ``dispatch``; ``on_idle`` is where control goes when the plan
    has no runnable task — typically back to the planner to propose more, or
    :data:`~agentflow.graph.END` to stop.

    ``handlers`` maps a task ``kind`` to an async ``(task, state, ctx) ->
    TaskResult``. A task whose ``kind`` has no handler fails with an error (it
    does not raise, so one bad task does not abort the run — it blocks its
    dependents like any other failure). ``max_fanout`` is the fixed number of
    run slots, so at most that many tasks run concurrently per super-step; it is
    a compile-time ceiling, mirroring ``max_node_concurrency`` in spirit.

    Mix :class:`PlanState` into your schema so the ``plan`` channel is declared
    with the right per-field reducers. Returns ``g`` for chaining.
    """
    if max_fanout < 1:
        raise ValueError("max_fanout must be >= 1")
    for created in (dispatch, collect):
        if created in g.nodes:
            raise ValueError(f"node {created!r} already exists")
    slot_names = [f"{dispatch}__run_{i}" for i in range(max_fanout)]

    # -- dispatch: assign up to max_fanout runnable tasks to slots -----------

    async def dispatch_node(state: Mapping[str, Any], ctx: Any) -> dict[str, Any]:
        plan = _plan(state)
        runnable = _runnable(plan)[:max_fanout]
        assigned = {str(i): task.id for i, task in enumerate(runnable)}
        # overwrite the transient assignment (last reducer); nothing else moves
        return {"plan": {"assigned": assigned}}

    def dispatch_route(state: Mapping[str, Any]) -> list[str]:
        plan = _plan(state)
        assigned = plan.get("assigned") or {}
        if assigned:
            return [str(i) for i in range(len(assigned))]
        # nothing runnable: go idle (plan done, or stalled on a failed prereq)
        return ["idle"]

    # -- run slot: execute the one task assigned to this slot ----------------

    def _make_slot(index: int):
        key = str(index)

        async def run_slot(state: Mapping[str, Any], ctx: Any) -> dict[str, Any] | None:
            plan = _plan(state)
            assigned = plan.get("assigned") or {}
            task_id = assigned.get(key)
            if task_id is None:
                return None  # this slot was not used this round
            task = _tasks_by_id(plan).get(task_id)
            if task is None:  # pragma: no cover - defensive; id always present
                return None
            handler = handlers.get(task.kind)
            if handler is None:
                result = TaskResult(
                    task.id, failed=True, error=f"no handler for kind {task.kind!r}"
                )
            else:
                result = await handler(task, state, ctx)
            return {"plan": {"results": [result], "done_ids": {result.task_id}}}

        run_slot.__name__ = f"slot[{index}]"
        return run_slot

    # -- collect: fan-in, then loop or go idle -------------------------------

    async def collect_node(state: Mapping[str, Any], ctx: Any) -> dict[str, Any] | None:
        return None  # pure routing point; results already folded by the slots

    def collect_route(state: Mapping[str, Any]) -> str:
        return "more" if _runnable(_plan(state)) else "idle"

    # -- wire it together ----------------------------------------------------

    g.add_node(dispatch, dispatch_node)
    for i, name in enumerate(slot_names):
        g.add_node(name, _make_slot(i))
    g.add_node(collect, collect_node)

    slot_mapping: dict[str, Any] = {str(i): slot_names[i] for i in range(max_fanout)}
    slot_mapping["idle"] = on_idle
    g.add_conditional_edges(dispatch, dispatch_route, slot_mapping)

    for name in slot_names:
        g.add_edge(name, collect)
    g.add_conditional_edges(collect, collect_route, {"more": dispatch, "idle": on_idle})

    return g
