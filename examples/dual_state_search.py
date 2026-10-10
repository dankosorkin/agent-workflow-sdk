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

"""Dual-state search with a by_key channel: planner, workers, verifier.

Run: python examples/dual_state_search.py

Shows the shape a search/optimization loop takes on top of the core: one
structured ``search`` channel whose fields accumulate differently, folded by
``by_key``. A single planner is the only writer of ``intents``; several workers
run concurrently in one super-step and each appends a ``candidate``; a single
deterministic verifier is the promotion boundary — it appends to ``verified``,
replaces the current ``incumbent`` whole, and clears the ``events`` it handled.

Everything here is a plain function so the example runs on the dependency-free
core — no backend, no network. A real planner would call an LLM/agent and a
real worker would run one narrow experiment; the state plumbing is identical.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from agentflow import END, START, Graph, State, append, by_key, last

# One structured channel, each field with its own fold policy. intents and
# candidates and verified grow (append); events is replaced each step (last);
# incumbent is overwritten whole when a better candidate is promoted (last).
search_channel = by_key(
    {
        "intents": append,
        "candidates": append,
        "verified": append,
        "events": last,
        "incumbent": last,
    }
)


class SearchState(State):
    search: Annotated[dict, search_channel]
    rounds: Annotated[int, last]


# A tiny deterministic "objective": lower is better. Each strategy name maps to
# a score, so the run is fully reproducible without any model.
STRATEGY_SCORES = {
    "try_index": 4.0,
    "reorder_stages": 7.0,
    "push_match": 2.5,
}


async def planner(state: SearchState, ctx) -> dict:
    """The only writer of intents. Proposes strategies not yet verified."""
    search = state["search"]
    tried = {v["strategy"] for v in search.get("verified", [])}
    proposals = [s for s in STRATEGY_SCORES if s not in tried]
    # propose at most two per round, so re-planning happens more than once
    new_intents = proposals[:2]
    print(f"planner: proposing {new_intents or '(nothing new)'}")
    return {
        "search": {"intents": new_intents, "events": []},  # clear handled events
        "rounds": state.get("rounds", 0) + 1,
    }


def _worker(strategy: str):
    """Build a worker node that runs one narrow experiment for a strategy."""

    async def worker(state: SearchState, ctx) -> dict:
        # a real worker would run an index/pipeline experiment here
        candidate = {"strategy": strategy, "score": STRATEGY_SCORES[strategy]}
        return {"search": {"candidates": [candidate]}}

    worker.__name__ = f"worker_{strategy}"
    return worker


async def verifier(state: SearchState, ctx) -> dict:
    """Promotion boundary: deterministic, no model. Verifies new candidates,
    promotes a new incumbent if one beats the current best, emits events.

    Note ``candidates`` is an append channel: writing ``[]`` to it does not
    clear it (``append`` of an empty list is a no-op). So the verifier works
    off the candidates it has not verified yet, keyed by strategy, rather than
    relying on a clear. This is the honest way to drain an append channel.
    """
    search = state["search"]
    already = {v["strategy"] for v in search.get("verified", [])}
    candidates = [c for c in search.get("candidates", []) if c["strategy"] not in already]
    incumbent = search.get("incumbent")

    verified = [{"strategy": c["strategy"], "score": c["score"]} for c in candidates]
    best = min(candidates, key=lambda c: c["score"]) if candidates else None

    updates: dict = {"search": {"verified": verified}}
    if best is not None and (incumbent is None or best["score"] < incumbent["score"]):
        updates["search"]["incumbent"] = best
        updates["search"]["events"] = ["NEW_INCUMBENT"]
        print(f"verifier: NEW_INCUMBENT {best['strategy']} score={best['score']}")
    else:
        updates["search"]["events"] = ["NO_IMPROVEMENT"]
        print("verifier: no improvement this round")
    return updates


def route_after_verifier(state: SearchState) -> str:
    """Event-driven re-planning: loop back to the planner while there is still
    something to try and we improved; stop otherwise or at a round cap."""
    search = state["search"]
    tried = {v["strategy"] for v in search.get("verified", [])}
    exhausted = tried >= set(STRATEGY_SCORES)
    if exhausted or state.get("rounds", 0) >= 5:
        return "done"
    if "NEW_INCUMBENT" in search.get("events", []):
        return "replan"
    return "done"


def dispatch(state: SearchState) -> list[str]:
    """Fan out one worker per *runnable* intent, all in the next super-step.

    ``intents`` is an append channel, so it accumulates every proposal ever
    made. The runnable frontier is the subset not already verified — this is
    why you compute a frontier instead of dispatching the raw intents list.
    """
    search = state["search"]
    tried = {v["strategy"] for v in search.get("verified", [])}
    runnable = [i for i in search.get("intents", []) if i in STRATEGY_SCORES and i not in tried]
    return runnable or ["noop"]


async def noop(state: SearchState, ctx) -> dict:
    return {"search": {"candidates": []}}


def build() -> Graph:
    g = Graph(SearchState)
    g.add_node("planner", planner)
    for strategy in STRATEGY_SCORES:
        g.add_node(f"worker_{strategy}", _worker(strategy))
    g.add_node("noop", noop)
    g.add_node("verifier", verifier)

    g.add_edge(START, "planner")

    # planner fans out to a worker per runnable intent (concurrent super-step)
    worker_targets = {s: f"worker_{s}" for s in STRATEGY_SCORES}
    worker_targets["noop"] = "noop"
    g.add_conditional_edges("planner", dispatch, worker_targets)

    # every worker leads into the single verifier
    for strategy in STRATEGY_SCORES:
        g.add_edge(f"worker_{strategy}", "verifier")
    g.add_edge("noop", "verifier")

    # verifier either re-plans or ends
    g.add_conditional_edges("verifier", route_after_verifier, {"replan": "planner", "done": END})
    return g


async def main() -> None:
    app = build().compile()
    out = await app.invoke({"search": {}, "rounds": 0})
    search = out["search"]
    print()
    print(f"rounds run:    {out['rounds']}")
    print(f"verified:      {[v['strategy'] for v in search['verified']]}")
    print(f"best incumbent: {search['incumbent']}")
    # candidates was cleared by the verifier each round; events holds the last
    print(f"last events:   {search['events']}")


if __name__ == "__main__":
    asyncio.run(main())
