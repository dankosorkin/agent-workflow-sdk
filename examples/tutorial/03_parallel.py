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

"""Optimizer tutorial, chapter 3: parallel experiments.

Run: python examples/tutorial/03_parallel.py

Instead of trying one strategy per round, we try every remaining strategy at
once — each in its own node, all in a single super-step — and merge their
results through an append reducer. A picker node then promotes the best. This
is the fan-out / fan-in shape, and the reason the engine advances in
super-steps rather than one node at a time.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from agentflow import END, START, Graph, State, append, last

BASE_COST = 100.0
STRATEGY_FACTORS = {
    "add_index": 0.4,
    "push_filter": 0.6,
    "reorder_stages": 0.9,
    "cache_result": 1.1,
}


def cost_of(plan: tuple[str, ...]) -> float:
    cost = BASE_COST
    for strategy in plan:
        cost *= STRATEGY_FACTORS.get(strategy, 1.0)
    return round(cost, 2)


def with_strategy(plan: tuple[str, ...], strategy: str) -> tuple[str, ...]:
    """A plan is a sorted tuple (JSON-serializable, so it survives chapter 8)."""
    return tuple(sorted(set(plan) | {strategy}))


class OptState(State):
    plan: Annotated[tuple, last]
    cost: Annotated[float, last]
    # each trial node appends its (strategy, cost) here; the picker drains it
    trials: Annotated[list, append]


def _trial_node(strategy: str):
    """Build a node that evaluates adding one specific strategy.

    Each trial tags its result with the plan it was based on. That tag is what
    lets the picker ignore stale entries from earlier rounds — `trials` is an
    append channel, so it accumulates; writing `[]` to it would not clear it
    (append of an empty list adds nothing). Keying by the current plan is the
    honest way to drain an append channel. (Chapter 4 removes the accumulation
    entirely with a structured channel.)
    """

    async def trial(state: OptState, ctx) -> dict:
        plan = state["plan"]
        if strategy in plan:
            return {}  # already applied; nothing to try
        return {"trials": [(plan, strategy, cost_of(with_strategy(plan, strategy)))]}

    trial.__name__ = f"trial_{strategy}"
    return trial


async def pick(state: OptState, ctx) -> dict:
    """Promote the best improving trial for the current plan."""
    plan = state["plan"]
    current = cost_of(plan)
    # only this round's trials: those based on the current plan
    improving = [(s, c) for p, s, c in state.get("trials", []) if p == plan and c < current]
    if not improving:
        return {}  # no improvement; leave plan as-is, router will stop
    strategy, new_cost = min(improving, key=lambda t: t[1])
    print(f"  best this round: + {strategy}: {current} -> {new_cost}")
    return {"plan": with_strategy(plan, strategy), "cost": new_cost}


def fan_out(state: OptState) -> list[str]:
    """Dispatch one trial branch per not-yet-applied strategy."""
    plan = state["plan"]
    remaining = [s for s in STRATEGY_FACTORS if s not in plan]
    return remaining or ["none"]


def after_pick(state: OptState) -> str:
    plan = state["plan"]
    current = cost_of(plan)
    improvable = any(
        s not in plan and cost_of(with_strategy(plan, s)) < current for s in STRATEGY_FACTORS
    )
    return "again" if improvable else "done"


async def noop(state: OptState, ctx) -> dict:
    return {}


def build() -> Graph:
    g = Graph(OptState)
    # a dispatch node whose only job is to fan out to the trial nodes
    g.add_node("dispatch", noop)
    for strategy in STRATEGY_FACTORS:
        g.add_node(f"trial_{strategy}", _trial_node(strategy))
    g.add_node("none", noop)
    g.add_node("pick", pick)

    g.add_edge(START, "dispatch")
    targets = {s: f"trial_{s}" for s in STRATEGY_FACTORS}
    targets["none"] = "none"
    g.add_conditional_edges("dispatch", fan_out, targets)
    for strategy in STRATEGY_FACTORS:
        g.add_edge(f"trial_{strategy}", "pick")
    g.add_edge("none", "pick")
    g.add_conditional_edges("pick", after_pick, {"again": "dispatch", "done": END})
    return g


async def main() -> None:
    app = build().compile()
    print("optimizing with parallel trials each round:")
    out = await app.invoke({"plan": (), "cost": BASE_COST})
    print(f"final plan {list(out['plan'])} -> cost {out['cost']}")


if __name__ == "__main__":
    asyncio.run(main())
