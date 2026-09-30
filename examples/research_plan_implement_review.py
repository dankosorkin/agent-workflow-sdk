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

"""A long autonomous workflow: research -> plan -> implement -> review.

Run: python examples/research_plan_implement_review.py

This is the canonical shape for a long, gated process:

    research -> plan --[plan_gate]--> implement --[review_gate]--> done
                  ^                       |            |
                  +----- replan ----------+            |
                                     revise <----------+

Two quality gates guard the risky transitions. The plan gate makes sure the
plan is sound before any implementation happens; the review gate checks the
result and sends it back to revise, or escalates. The implement step is wrapped
with skip_if_done so re-running the graph (a resume after a crash, or a second
review loop) does not redo identical work — it is keyed by the plan's content.

Everything here is pure code so it runs offline; swap the evaluators and the
work nodes for backend-driven ones without changing the graph.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from agentflow import END, START, Graph, Hooks, MemoryStore, add, last
from agentflow.events import GateEvent
from agentflow.prebuilt import GateResult, GateState, add_quality_gate, artifact_key, skip_if_done


class WorkState(GateState):
    topic: Annotated[str, last]
    findings: Annotated[str, last]
    plan: Annotated[str, last]
    plan_detail: Annotated[int, last]  # how thorough the plan is
    build: Annotated[str, last]
    build_runs: Annotated[int, add]  # how many times implement actually ran


class GateLog(Hooks):
    async def on_event(self, thread, step, node, event) -> None:
        if isinstance(event, GateEvent):
            print(f"  gate[{event.gate}] {event.phase} (attempt {event.attempt})")


async def research(state, ctx) -> dict:
    return {"findings": f"notes about {state['topic']}"}


async def plan(state, ctx) -> dict:
    # Each replan adds detail; the plan gate wants detail >= 2.
    return {"plan": f"plan for {state['topic']}", "plan_detail": state.get("plan_detail", 0) + 1}


async def replan(state, ctx) -> dict:
    # Loop back into planning with a nudge; plan() increments detail.
    return {}


async def evaluate_plan(state, ctx) -> GateResult:
    ok = state.get("plan_detail", 0) >= 2
    return GateResult(
        passed=ok, score=float(state.get("plan_detail", 0)), reasons=[] if ok else ["plan too thin"]
    )


async def _implement(state, ctx) -> dict:
    # The expensive step. Keyed by the plan so identical plans reuse the result.
    return {"build": f"built: {state['plan']}", "build_runs": 1}


async def revise(state, ctx) -> dict:
    # Improve the build; here we just mark it revised so review passes next time.
    return {"build": state.get("build", "") + " (revised)"}


async def evaluate_review(state, ctx) -> GateResult:
    ok = "revised" in state.get("build", "")
    return GateResult(passed=ok, reasons=[] if ok else ["needs one revision pass"])


async def done(state, ctx) -> dict:
    return {}


async def escalate(state, ctx) -> dict:
    print("  (escalated to a human)")
    return {}


def build_graph(store) -> Graph:
    implement = skip_if_done(
        _implement,
        store,
        namespace=("cache", "implement"),
        key_from=lambda s: artifact_key(s.get("plan", "")),
    )

    g = Graph(WorkState)
    g.add_node("research", research)
    g.add_node("plan", plan)
    g.add_node("replan", replan)
    g.add_node("implement", implement)
    g.add_node("revise", revise)
    g.add_node("done", done)
    g.add_node("escalate", escalate)

    add_quality_gate(
        g,
        "plan_gate",
        evaluate_plan,
        on_pass="implement",
        on_fail="replan",
        on_escalate="escalate",
        max_attempts=4,
    )
    add_quality_gate(
        g,
        "review_gate",
        evaluate_review,
        on_pass="done",
        on_fail="revise",
        on_escalate="escalate",
        max_attempts=3,
    )

    g.add_edge(START, "research")
    g.add_edge("research", "plan")
    g.add_edge("plan", "plan_gate")
    g.add_edge("replan", "plan")
    g.add_edge("implement", "review_gate")
    g.add_edge("revise", "review_gate")
    g.add_edge("done", END)
    g.add_edge("escalate", END)
    return g


async def main() -> None:
    store = MemoryStore()
    app = build_graph(store).compile(hooks=GateLog())
    out = await app.invoke({"topic": "durable agent loops"}, thread="run-1")
    print("\n--- result ---")
    print("plan_detail:", out["plan_detail"])
    print("build:      ", out["build"])
    print("build_runs: ", out["build_runs"], "(implement ran this many times)")
    print("gate attempts:", out["gate_attempts"])


if __name__ == "__main__":
    asyncio.run(main())
