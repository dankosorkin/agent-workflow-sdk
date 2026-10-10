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

"""Optimizer tutorial, chapter 5: a real planner backend.

Run: python examples/tutorial/05_backend.py

The proposer stops enumerating every strategy and instead *asks a model* which
few to try next. The verifier (cost_of) stays deterministic: the model only
proposes, code decides. We use a deterministic fake proposer so the chapter
runs offline and reproducibly; a one-line swap turns it into a real Ollama
call (see propose_with_ollama, commented).
"""

from __future__ import annotations

import asyncio
from typing import Annotated, Protocol

from agentflow import END, START, Graph, State, append, by_key, last, merge

BASE_COST = 100.0
STRATEGY_FACTORS = {
    "add_index": 0.4,
    "push_filter": 0.6,
    "reorder_stages": 0.9,
    "cache_result": 1.1,
}


def cost_of(plan) -> float:
    """The verifier. Deterministic, pure code — the model never scores a plan."""
    cost = BASE_COST
    for strategy in plan:
        cost *= STRATEGY_FACTORS.get(strategy, 1.0)
    return round(cost, 2)


def with_strategy(plan, strategy: str) -> tuple[str, ...]:
    """A plan is a sorted tuple (JSON-serializable, so it survives chapter 8)."""
    return tuple(sorted(set(plan) | {strategy}))


# --- the proposer boundary -------------------------------------------------
# A proposer suggests which strategies to try next, given the problem and the
# current best plan. It may be a model, a heuristic, or anything — the graph
# only depends on this small protocol.


class Proposer(Protocol):
    async def propose(self, problem: dict, incumbent: dict) -> list[str]: ...


class HeuristicProposer:
    """A deterministic stand-in for a model. Proposes the two cheapest-looking
    untried strategies by the problem's stated factor hints."""

    async def propose(self, problem: dict, incumbent: dict) -> list[str]:
        plan = incumbent["plan"]
        hints = problem["factor_hints"]  # strategy -> rough factor the model sees
        untried = [s for s in hints if s not in plan]
        # a model would reason here; the fake sorts by the hint and takes two
        ranked = sorted(untried, key=lambda s: hints[s])
        return ranked[:2]


# --- a real proposer, for when you want the model to actually reason --------
# Uncomment and pass OllamaProposer() to main() to use a local model. The graph
# does not change at all — only the object implementing `propose` does.
#
# from agentflow.backends.ollama import OllamaBackend
# from agentflow.events import Message
#
# class OllamaProposer:
#     def __init__(self, model="llama3.2"):
#         self.llm = OllamaBackend(model)
#     async def propose(self, problem, incumbent):
#         prompt = (
#             f"Strategies: {list(problem['factor_hints'])}. "
#             f"Already applied: {sorted(incumbent['plan'])}. "
#             "Name up to two more to try, comma-separated, no prose."
#         )
#         text = ""
#         async with self.llm:
#             async for ev in self.llm.chat([Message('user', prompt)]):
#                 from agentflow.events import TurnEnd
#                 if isinstance(ev, TurnEnd):
#                     text = ev.text
#         names = [n.strip() for n in text.split(',')]
#         return [n for n in names if n in problem['factor_hints']][:2]


class OptState(State):
    problem: Annotated[dict, merge]
    search: Annotated[dict, by_key({"candidates": append, "incumbent": last})]


def make_graph(proposer: Proposer) -> Graph:
    async def propose(state: OptState, ctx) -> dict:
        incumbent = state["search"]["incumbent"]
        suggestions = await proposer.propose(state["problem"], incumbent)
        plan = incumbent["plan"]
        cands = [
            {"plan": with_strategy(plan, s), "cost": cost_of(with_strategy(plan, s))}
            for s in suggestions
            if s not in plan
        ]
        return {"search": {"candidates": cands}}

    async def verify_and_promote(state: OptState, ctx) -> dict:
        incumbent = state["search"]["incumbent"]
        plan = incumbent["plan"]
        this_round = [
            c
            for c in state["search"].get("candidates", [])
            if set(plan).issubset(c["plan"]) and len(c["plan"]) == len(plan) + 1
        ]
        better = [c for c in this_round if c["cost"] < incumbent["cost"]]
        if not better:
            return {}
        best = min(better, key=lambda c: c["cost"])
        print(f"  promote {list(best['plan'])} cost {best['cost']}")
        return {"search": {"incumbent": best}}

    def route(state: OptState) -> str:
        incumbent = state["search"]["incumbent"]
        plan = incumbent["plan"]
        improvable = any(
            s not in plan and cost_of(with_strategy(plan, s)) < incumbent["cost"]
            for s in STRATEGY_FACTORS
        )
        return "again" if improvable else "done"

    g = Graph(OptState)
    g.add_node("propose", propose)
    g.add_node("verify", verify_and_promote)
    g.add_edge(START, "propose")
    g.add_edge("propose", "verify")
    g.add_conditional_edges("verify", route, {"again": "propose", "done": END})
    return g


async def main(proposer: Proposer | None = None) -> None:
    proposer = proposer or HeuristicProposer()
    app = make_graph(proposer).compile(step_limit=50)
    # the model sees rough factor hints, not the true cost model
    problem = {
        "base_cost": BASE_COST,
        "factor_hints": {
            "add_index": 0.4,
            "push_filter": 0.6,
            "reorder_stages": 0.9,
            "cache_result": 1.1,
        },
    }
    print(f"optimizing with a {type(proposer).__name__}:")
    out = await app.invoke(
        {"problem": problem, "search": {"incumbent": {"plan": (), "cost": BASE_COST}}}
    )
    incumbent = out["search"]["incumbent"]
    print(f"final plan {list(incumbent['plan'])} -> cost {incumbent['cost']}")


if __name__ == "__main__":
    asyncio.run(main())
