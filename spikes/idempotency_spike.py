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

"""Spike: what does a resume cost, and what does it repeat? (design notes §3 p.13)

The engine's delivery contract is at-least-once. Two steps `a` and `b` have no
prerequisites, so the plan executor runs them in the SAME round (super-step).
If one of them raises, the whole round is discarded and BOTH re-run on resume —
including the one that already finished and already did its side effect.

This spike counts, per variant, how often the model backend was called and how
often the external effect happened for step `a`:

  V0 none          the handler just does backend call + effect.
  V1 result cache  a ``Store`` caches the step's ``TaskResult`` keyed by
                   ``artifact_key([task.id, task.input])``: a re-run is a cache
                   hit, so it costs no model call and repeats no effect.
  V2 effect guard  ``IdempotentOp`` protects only the effect (``effect_key``):
                   the model is called again, the effect is not repeated.
  V3 both          cache + effect guard.

Two faults are injected once, in two scenarios:

  S1 sibling fails       `b` raises BEFORE doing any work (a worker death in the
                         other slot). `a` had already finished.
  S2 crash window        `a` raises AFTER its effect but BEFORE its result is
                         cached/returned — the window no cache can close, because
                         the result was never written.

Expected (asserted):

                      S1: a calls / a effects     S2: a calls / a effects
    V0 none                 2 / 2                       2 / 2
    V1 result cache         1 / 1                       2 / 2   <- window open
    V2 effect guard         2 / 1                       2 / 1
    V3 both                 1 / 1                       2 / 1   <- closes both

So a cache saves COST (model calls) but cannot protect an effect from the crash
window; the effect guard protects the effect but not the cost; only both together
cover S1 cheaply and S2 safely. The guard is per EFFECT, the cache per RESULT.

HONEST SCOPE:
  * ``MemoryStore`` markers live in this process. Across a worker restart they
    would be lost — a real deployment needs ``PostgresStore``.
  * ``IdempotentOp`` makes the effect once from the ORCHESTRATOR's side. A crash
    INSIDE the effect, or a lost response, still needs idempotency at the
    receiver (pass ``effect_key`` as the receiver's token). Not exercised here.
  * ``skip_if_done`` wraps NODES; a plan executor handler is not a node, so the
    spike applies the same recipe (hash the input, look up, store the result)
    directly with ``Store`` and ``artifact_key``.
  * The cache key here is ``[task.id, task.input]``. A real key must also cover
    the resolved outputs of the prerequisites, or a changed upstream result would
    be served a stale cache hit.

Run it directly:

    python spikes/idempotency_spike.py
"""

from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import dataclass
from typing import Any

from _checks import checkpointer_factories
from agentflow import END, START, Graph, MemoryStore, NodeError
from agentflow.prebuilt import (
    IdempotentOp,
    PlanState,
    Task,
    TaskResult,
    add_plan_executor,
    artifact_key,
    effect_key,
    plan_complete,
)

NS = ("cache", "step_results")


class Crash(RuntimeError):
    """Stands in for 'the worker died'."""


@dataclass(frozen=True)
class Variant:
    name: str
    cache: bool
    guard: bool


VARIANTS = [
    Variant("V0 none", cache=False, guard=False),
    Variant("V1 result cache", cache=True, guard=False),
    Variant("V2 effect guard", cache=False, guard=True),
    Variant("V3 both", cache=True, guard=True),
]


@dataclass
class Fault:
    """One crash, fired once. ``where`` is "before" (S1) or "after_effect" (S2)."""

    scenario: str
    step: str
    where: str
    fired: bool = False

    def maybe(self, step: str, where: str) -> None:
        if not self.fired and step == self.step and where == self.where:
            self.fired = True
            raise Crash(f"{self.scenario}: worker died ({where} of step {step})")


class World:
    """The outside world: counts model calls and external effects per step."""

    def __init__(self) -> None:
        self.model_calls: Counter[str] = Counter()
        self.effects: Counter[str] = Counter()


def build(variant: Variant, world: World, fault: Fault, checkpointer):
    cache = MemoryStore()
    guard = IdempotentOp(MemoryStore(), ("effects", "demo"))

    async def handler(task: Task, state: dict, ctx: Any) -> TaskResult:
        fault.maybe(task.id, "before")

        key = artifact_key([task.id, task.input])
        if variant.cache:
            hit = await cache.get(NS, key)
            if hit is not None:
                return TaskResult.from_dict(hit.value)  # no model call, no effect

        world.model_calls[task.id] += 1  # the model call
        await asyncio.sleep(0.01)  # overlap with the sibling, as real agents do

        async def effect() -> dict[str, str]:
            world.effects[task.id] += 1  # the external, non-rollbackable effect
            return {"effect_id": f"e-{task.id}"}

        if variant.guard:
            receipt = await guard.run(effect_key("effect", task.id), effect)
        else:
            receipt = await effect()

        fault.maybe(task.id, "after_effect")

        result = TaskResult(task.id, output={"receipt": receipt})
        if variant.cache:
            await cache.put(NS, key, result.to_dict())
        return result

    async def planner(state, ctx):
        if state.get("plan", {}).get("tasks"):
            return {}
        return {"plan": {"tasks": [Task("a", "agent_step"), Task("b", "agent_step")]}}

    async def idle(state, ctx):
        return {}

    g = Graph(PlanState)
    g.add_node("planner", planner)
    add_plan_executor(g, handlers={"agent_step": handler}, on_idle="idle", max_fanout=2)
    g.add_node("idle", idle)
    g.add_edge(START, "planner")
    g.add_edge("planner", "dispatch")
    g.add_edge("idle", END)
    return g.compile(checkpointer=checkpointer, step_limit=30)


async def crash_then_resume(variant: Variant, scenario: str, make_cp) -> tuple[World, dict]:
    world = World()
    fault = (
        Fault("S1", step="b", where="before")
        if scenario == "S1"
        else Fault("S2", step="a", where="after_effect")
    )
    graph = build(variant, world, fault, make_cp())
    try:
        await graph.invoke({}, thread="run")
    except NodeError as exc:
        assert isinstance(exc.__cause__, Crash), f"expected the injected crash, got {exc!r}"
    else:  # pragma: no cover - the fault is injected, so this would be a spike bug
        raise AssertionError("the injected crash did not happen")
    final = await graph.resume("run")
    return world, final


# (model calls for `a`, effects for `a`) expected per scenario and variant.
EXPECTED = {
    "S1": {
        "V0 none": (2, 2),
        "V1 result cache": (1, 1),
        "V2 effect guard": (2, 1),
        "V3 both": (1, 1),
    },
    "S2": {
        "V0 none": (2, 2),
        "V1 result cache": (2, 2),
        "V2 effect guard": (2, 1),
        "V3 both": (2, 1),
    },
}
TITLES = {
    "S1": "S1 sibling fails: `b` dies, `a` had already finished and done its effect",
    "S2": "S2 crash window: `a` dies after its effect, before its result is recorded",
}


async def run_once(label: str, make_cp) -> None:
    print(f"=== [{label}] ===")
    for scenario in ("S1", "S2"):
        print(f"  {TITLES[scenario]}")
        print(f"    {'variant':<18}{'model calls (a)':<18}{'effects (a)':<14}complete?")
        for variant in VARIANTS:
            world, final = await crash_then_resume(variant, scenario, make_cp)
            got = (world.model_calls["a"], world.effects["a"])
            print(f"    {variant.name:<18}{got[0]:<18}{got[1]:<14}{plan_complete(final)}")

            assert plan_complete(final), (scenario, variant.name)
            assert got == EXPECTED[scenario][variant.name], (
                f"{scenario}/{variant.name}: expected {EXPECTED[scenario][variant.name]}, got {got}"
            )
    print("  OK\n")


async def main() -> None:
    for label, make_cp in checkpointer_factories():
        await run_once(label, make_cp)
    print("All spike assertions passed on both checkpointers.")


if __name__ == "__main__":
    asyncio.run(main())
