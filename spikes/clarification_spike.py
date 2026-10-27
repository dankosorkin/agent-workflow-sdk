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

"""Spike: a clarifying question as a plan-level behaviour (design notes OQ-11).

Decision under test: a step may ask a human a short question ONLY between steps,
declared in the plan (``may_ask``), implemented as a separate ``ask`` task that
sits between the step that discovered the ambiguity and the step that reruns
with the answer. Hermetic: a scripted agent double, no CLI, no LLM.

The flow, all as plan data (no node is added at run time):

    s1 --(returns {"needs_clarification": {...}})--> ask:s1:1 --> s1#2 (answer in input)

The planner, on idle, reads the committed result of ``s1`` and appends the two
follow-up tasks. ``ask`` is a pure task: it calls ``ctx.interrupt`` first thing.

What is measured, on BOTH a memory and a SQLite checkpointer:

  1. Payload   : the interrupt carries ``type="clarification"``, the question
                 and the options, readable from the checkpoint.
  2. Cost      : two-phase (this design) = 2 agent calls; the alternative
                 (probe, then ``ctx.interrupt`` inside the step handler) replays
                 the probe on resume = 3 agent calls.
  3. Sibling   : a side-effecting task the planner adds in the SAME call as the
                 ``ask`` shares its round and is re-run on resume; ordering it
                 after the answer-bearing step keeps it at exactly one run.
                 (Tasks declared earlier cannot share the round: ``ask`` only
                 exists once the plan went idle, i.e. they have all finished.)
  4. Cache key : the answer is part of the step input, so the content key of
                 the rerun differs from the first run's and from a run that
                 got a different answer; the same answer gives the same key.
  5. No human  : auto mode without ``default_answer`` stops with a reason and
                 no interrupt; with ``default_answer`` it continues silently.
  6. Limits    : an answer outside ``options`` fails the ask; a step that keeps
                 asking beyond ``max_questions`` fails with a clear reason.

HONEST SCOPE:
  * "Cache key" is computed here with ``artifact_key`` over (step input, answers).
    The spike does not build a result cache; it shows what the key would be.
  * Only the ordering rule "declare the sibling's prerequisite on the rerun id" is
    tried. A production layer would rewrite dependents when it inserts an ask.
  * The interrupt/resume cycle is a single process. The engine's checkpoint is
    what carries it, which is the point, but no real restart is simulated.

Run it directly:

    python spikes/clarification_spike.py
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from _checks import checkpointer_factories
from agentflow import END, START, ClarificationRequest, Graph
from agentflow.prebuilt import (
    PlanState,
    Task,
    TaskResult,
    add_plan_executor,
    artifact_key,
    plan_complete,
    plan_stalled,
)

STEP = "s1"
BASE = {"goal": "deploy the service"}
QUESTION = {"question": "Which environment?", "options": ["staging", "prod"]}


@dataclass
class World:
    """Everything observable from outside the graph."""

    mode: str = "human"  # "human" | "auto"
    default_answer: str | None = None
    max_questions: int = 1
    stubborn: bool = False  # the agent keeps asking even after an answer
    agent_calls: list[tuple[str, tuple[str, ...]]] = field(default_factory=list)
    ask_bodies: int = 0  # times the ask handler body started
    sibling_runs: int = 0
    keys: list[str] = field(default_factory=list)

    async def agent(self, task_id: str, answers: list[str]) -> dict[str, Any]:
        self.agent_calls.append((task_id, tuple(answers)))
        if not answers or self.stubborn:
            return {"needs_clarification": dict(QUESTION)}
        return {"deployed_to": answers[-1]}


def _results(state: dict) -> dict[str, dict]:
    return {r["task_id"]: r for r in (state.get("plan") or {}).get("results") or ()}


def build(world: World, checkpointer, *, extra: str | None = None, inline: bool = False):
    # ---- handlers ----------------------------------------------------------
    async def agent_step(task: Task, state: dict, ctx: Any) -> TaskResult:
        res = _results(state)
        answers = [res[a]["output"]["answer"] for a in task.input["ask_ids"]]
        world.keys.append(artifact_key({"input": task.input["base"], "answers": answers}))
        out = await world.agent(task.id, answers)
        if inline and "needs_clarification" in out:
            # The alternative under comparison: ask from inside the step.
            q = out["needs_clarification"]
            answer = await ctx.interrupt(ClarificationRequest(**q).to_payload())
            out = await world.agent(task.id, [*answers, answer])
        return TaskResult(task.id, output=out)

    async def ask(task: Task, state: dict, ctx: Any) -> TaskResult:
        world.ask_bodies += 1
        q = task.input
        if world.mode == "auto":  # nobody to ask: never interrupt
            if world.default_answer is None:
                return TaskResult(
                    task.id, failed=True, error=f"needs_clarification: {q['question']}"
                )
            answer = world.default_answer
        else:
            payload = ClarificationRequest(q["question"], q["options"], {"step": STEP}).to_payload()
            answer = await ctx.interrupt(payload)
        if q["options"] and answer not in q["options"]:
            return TaskResult(
                task.id, failed=True, error=f"answer {answer!r} not in {q['options']}"
            )
        return TaskResult(task.id, output={"question": q["question"], "answer": answer})

    async def side_effect(task: Task, state: dict, ctx: Any) -> TaskResult:
        world.sibling_runs += 1
        return TaskResult(task.id, output={"sent": True})

    async def fail(task: Task, state: dict, ctx: Any) -> TaskResult:
        return TaskResult(task.id, failed=True, error=task.input["error"])

    # ---- planner: pure function of the committed results -------------------
    def next_actions(state: dict) -> list[Task]:
        plan = state.get("plan") or {}
        ids = {t["id"] for t in plan.get("tasks") or ()}
        res = _results(state)
        n = sum(1 for i in ids if i.startswith(f"ask:{STEP}:"))
        cur = STEP if n == 0 else f"{STEP}#{n + 1}"
        r = res.get(cur)
        if not r or r["failed"] or "needs_clarification" not in (r["output"] or {}):
            return []
        if n >= world.max_questions:
            fid = f"fail:{STEP}"
            err = f"{STEP}: max_questions={world.max_questions} exceeded"
            return [] if fid in ids else [Task(fid, "fail", input={"error": err})]
        q = r["output"]["needs_clarification"]
        ask_ids = [f"ask:{STEP}:{k}" for k in range(1, n + 2)]
        tasks = [
            Task(ask_ids[-1], "ask", input=q, prerequisites=(cur,)),
            Task(
                f"{STEP}#{n + 2}",
                "agent_step",
                input={"base": BASE, "ask_ids": ask_ids},
                prerequisites=(ask_ids[-1],),
            ),
        ]
        if extra and n == 0:  # unrelated work the planner adds in the same call
            after = () if extra == "same_round" else (f"{STEP}#2",)
            tasks.append(Task("extra", "side_effect", prerequisites=after))
        return tasks

    async def planner(state, ctx):
        if not (state.get("plan") or {}).get("tasks"):
            return {
                "plan": {"tasks": [Task(STEP, "agent_step", input={"base": BASE, "ask_ids": []})]}
            }
        return {"plan": {"tasks": next_actions(state)}}

    async def idle(state, ctx):
        return {}

    def idle_route(state):
        if next_actions(state):
            return "go"
        return "stop"

    g = Graph(PlanState)
    g.add_node("planner", planner)
    add_plan_executor(
        g,
        handlers={"agent_step": agent_step, "ask": ask, "side_effect": side_effect, "fail": fail},
        on_idle="idle",
        max_fanout=2,
    )
    g.add_node("idle", idle)
    g.add_conditional_edges("idle", idle_route, {"stop": END, "go": "planner"})
    g.add_edge(START, "planner")
    g.add_edge("planner", "dispatch")
    return g.compile(checkpointer=checkpointer, step_limit=80)


async def run_human(world: World, make_cp, *, extra=None, inline: bool = False, answer="staging"):
    graph = build(world, make_cp(), extra=extra, inline=inline)
    await graph.invoke({}, thread="run")
    state = await graph.get_state("run")
    assert state.interrupted, "the run should be suspended on the question"
    payload = state.interrupt_payload
    calls_before = len(world.agent_calls)
    final = await graph.resume("run", value=answer)
    return final, payload, calls_before


async def scenarios(label: str, make_cp) -> None:
    print(f"=== [{label}] ===")

    # 1+2+3+4: the recommended design; the sibling is ordered after the rerun.
    w = World()
    final, payload, before = await run_human(w, make_cp, extra="after_rerun")
    assert payload["type"] == "clarification", payload
    assert payload["question"] == QUESTION["question"] and payload["options"] == QUESTION["options"]
    assert before == 1, "only the first agent call happened before the question"
    assert w.agent_calls == [(STEP, ()), (f"{STEP}#2", ("staging",))], w.agent_calls
    assert plan_complete(final) and not any(r["failed"] for r in _results(final).values())
    assert _results(final)[f"{STEP}#2"]["output"] == {"deployed_to": "staging"}
    assert _results(final)[f"ask:{STEP}:1"]["output"]["answer"] == "staging"  # the trace record
    assert w.ask_bodies == 2, "ask body: once fresh, once replayed (it is pure, so harmless)"
    assert w.sibling_runs == 1, "ordered after the rerun: the side effect ran once"
    first_key, second_key = w.keys
    assert first_key != second_key, "the answer must change the step's content key"
    print(
        f"  two-phase: agent calls={len(w.agent_calls)}, ask bodies={w.ask_bodies},"
        f" sibling runs={w.sibling_runs}"
    )

    # 3 (hazard): a side effect the planner adds in the ask's round is replayed.
    w = World()
    final, _, _ = await run_human(w, make_cp, extra="same_round")
    assert plan_complete(final)
    assert w.sibling_runs == 2, f"sibling in the ask round should replay, got {w.sibling_runs}"
    print(f"  extra task in the ask round: runs={w.sibling_runs}  <- replayed by the interrupt")

    # 2 (comparison): ask from inside the step replays the probe.
    w = World()
    final, _, _ = await run_human(w, make_cp, extra="after_rerun", inline=True)
    assert len(w.agent_calls) == 3, w.agent_calls
    print(f"  inline interrupt: agent calls={len(w.agent_calls)} (probe replayed)")

    # 4: same answer -> same key, different answer -> different key.
    ka = artifact_key({"input": BASE, "answers": ["staging"]})
    kp = artifact_key({"input": BASE, "answers": ["prod"]})
    assert ka != kp and ka == artifact_key({"input": BASE, "answers": ["staging"]})
    assert ka != artifact_key({"input": BASE, "answers": []})

    # 5: no human.
    w = World(mode="auto")
    graph = build(w, make_cp())
    final = await graph.invoke({}, thread="auto")
    assert not (await graph.get_state("auto")).interrupted, "auto mode must never interrupt"
    err = _results(final)[f"ask:{STEP}:1"]["error"]
    assert err.startswith("needs_clarification:") and plan_stalled(final)
    assert len(w.agent_calls) == 1
    w = World(mode="auto", default_answer="staging")
    graph = build(w, make_cp())
    final = await graph.invoke({}, thread="auto2")
    assert plan_complete(final) and len(w.agent_calls) == 2
    assert _results(final)[f"ask:{STEP}:1"]["output"]["answer"] == "staging"
    print("  auto: stalls with a reason, or continues on default_answer; never interrupts")

    # 6: limits.
    w = World()
    final, _, _ = await run_human(w, make_cp, answer="nonsense")
    assert _results(final)[f"ask:{STEP}:1"]["failed"] and plan_stalled(final)
    w = World(stubborn=True)
    final, _, _ = await run_human(w, make_cp)
    fr = _results(final)[f"fail:{STEP}"]
    assert fr["failed"] and "max_questions=1 exceeded" in fr["error"], fr
    assert len(w.agent_calls) == 2, "the stubborn agent was not asked a second question"
    print("  limits: invalid answer fails the ask; max_questions fails the step")
    print("  OK\n")


async def main() -> None:
    for label, make_cp in checkpointer_factories():
        await scenarios(label, make_cp)
    print("All spike assertions passed on both checkpointers.")


if __name__ == "__main__":
    asyncio.run(main())
