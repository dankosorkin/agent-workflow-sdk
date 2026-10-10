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

"""Optimizer tutorial, chapter 8: surviving restarts.

Run: python examples/tutorial/08_durability.py

Two durability concerns. First the search: with a checkpointer, a run that
stops mid-search resumes from its last super-step instead of starting over. We
pause the run with an interrupt (standing in for a crash), then resume it from
a *fresh* compiled graph that shares the same SQLite checkpoint file — a
different "process". Second the apply step: applying a plan is a side effect,
and a resume can re-run its node, so we guard it with IdempotentOp keyed on the
plan — a second apply returns the stored receipt without firing the effect.

Note the state is all JSON-serializable (plans are tuples, not sets): a durable
checkpointer stores state as JSON, so sets would not round-trip. The whole
tutorial has used tuples for exactly this moment.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from typing import Annotated

from agentflow import END, START, Graph, SqliteCheckpointer, append, by_key, last, merge
from agentflow.prebuilt import GateResult, GateState, IdempotentOp, add_quality_gate
from agentflow.store import MemoryStore

BASE_COST = 100.0
STRATEGY_FACTORS = {
    "add_index": 0.4,
    "push_filter": 0.6,
    "reorder_stages": 0.9,
    "cache_result": 1.1,
}


def cost_of(plan) -> float:
    cost = BASE_COST
    for strategy in plan:
        cost *= STRATEGY_FACTORS.get(strategy, 1.0)
    return round(cost, 2)


def with_strategy(plan, strategy: str) -> tuple[str, ...]:
    return tuple(sorted(set(plan) | {strategy}))


class OptState(GateState):
    problem: Annotated[dict, merge]
    search: Annotated[dict, by_key({"candidates": append, "incumbent": last})]
    applied: Annotated[list, append]


# The store holding the apply effect's idempotency marker, and a counter of how
# many times the real side effect actually fired (to prove it is exactly once).
APPLY_STORE = MemoryStore()
APPLY_CALLS: list = []


async def propose(state: OptState, ctx) -> dict:
    plan = state["search"]["incumbent"]["plan"]
    cands = [
        {"plan": with_strategy(plan, s), "cost": cost_of(with_strategy(plan, s))}
        for s in STRATEGY_FACTORS
        if s not in plan
    ]
    return {"search": {"candidates": cands}}


def _best(state: OptState):
    incumbent = state["search"]["incumbent"]
    plan = incumbent["plan"]
    this_round = [
        c
        for c in state["search"].get("candidates", [])
        if set(plan).issubset(c["plan"]) and len(c["plan"]) == len(plan) + 1
    ]
    better = [c for c in this_round if c["cost"] < incumbent["cost"]]
    return min(better, key=lambda c: c["cost"]) if better else None


async def evaluate(state: OptState, ctx) -> GateResult:
    return GateResult(passed=_best(state) is not None)


async def promote(state: OptState, ctx) -> dict:
    best = _best(state)
    if best is None:
        return {}
    return {"search": {"incumbent": best}}


def after_promote(state: OptState) -> str:
    incumbent = state["search"]["incumbent"]
    plan = incumbent["plan"]
    improvable = any(
        s not in plan and cost_of(with_strategy(plan, s)) < incumbent["cost"]
        for s in STRATEGY_FACTORS
    )
    return "again" if improvable else "apply"


async def apply_plan(state: OptState, ctx) -> dict:
    """Apply the final plan — a side effect guarded for exactly-once."""
    plan = state["search"]["incumbent"]["plan"]
    key = "|".join(plan)
    op = IdempotentOp(APPLY_STORE, ("effects", "apply_plan"))

    async def do_apply():
        APPLY_CALLS.append(plan)  # the real effect; counted to prove once-only
        return {"applied_plan": list(plan)}

    receipt = await op.run(key, do_apply, on_incomplete="rerun")
    return {"applied": [receipt]}


async def approve(state: OptState, ctx) -> dict:
    """Park for sign-off before applying. This is where 'process A' stops."""
    plan = state["search"]["incumbent"]["plan"]
    answer = await ctx.interrupt({"question": "apply?", "plan": list(plan)})
    return {} if str(answer).lower() in ("y", "yes") else {}


def build() -> Graph:
    g = Graph(OptState)
    g.add_node("propose", propose)
    g.add_node("promote", promote)
    g.add_node("approve", approve)
    g.add_node("apply", apply_plan)
    add_quality_gate(
        g, "gate", evaluate, on_pass="promote", on_fail="propose", on_escalate="approve"
    )
    g.add_edge(START, "propose")
    g.add_edge("propose", "gate")
    g.add_conditional_edges("promote", after_promote, {"again": "propose", "apply": "approve"})
    g.add_edge("approve", "apply")
    g.add_edge("apply", END)
    return g


async def main() -> None:
    db = os.path.join(tempfile.mkdtemp(), "opt.db")
    problem = {"base_cost": BASE_COST}
    init = {"problem": problem, "search": {"incumbent": {"plan": (), "cost": BASE_COST}}}

    # Process A: optimize until the approval pause, then "crash" (we just stop
    # using this graph object). Everything is on disk in the sqlite file.
    app_a = build().compile(checkpointer=SqliteCheckpointer(db), step_limit=100)
    print("process A: optimizing (durable to sqlite):")
    await app_a.invoke(init, thread="run-1")
    parked = await app_a.get_state("run-1")
    assert parked.interrupted
    plan_so_far = parked.state["search"]["incumbent"]["plan"]
    print(f"  parked for approval with plan {list(plan_so_far)} — process A ends here")
    del app_a  # the process is gone; only the checkpoint survives

    # Process B: a brand-new compiled graph, same db file, no shared memory with
    # A. It resumes the parked run straight from the checkpoint and finishes.
    app_b = build().compile(checkpointer=SqliteCheckpointer(db), step_limit=100)
    print("process B: resuming from the checkpoint:")
    out = await app_b.resume("run-1", value="yes")
    final_plan = out["search"]["incumbent"]["plan"]
    print(
        f"  resumed and finished: plan {list(final_plan)} cost {out['search']['incumbent']['cost']}"
    )
    print(f"  apply effect fired {len(APPLY_CALLS)} time(s)")

    # Re-apply the same plan again (as a redundant retry would): the idempotent
    # guard returns the stored receipt without firing the effect a second time.
    op = IdempotentOp(APPLY_STORE, ("effects", "apply_plan"))

    async def do_apply():
        APPLY_CALLS.append(final_plan)
        return {"applied_plan": "second time"}

    await op.run("|".join(final_plan), do_apply, on_incomplete="rerun")
    print(f"  after a redundant re-apply, effect STILL fired only {len(APPLY_CALLS)} time(s)")


if __name__ == "__main__":
    asyncio.run(main())
