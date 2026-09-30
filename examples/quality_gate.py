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

"""A quality gate with a revise loop and an escalation path.

Run: python examples/quality_gate.py

A draft node writes an artifact; a gate scores it. Below the bar it routes to a
revise node that improves the draft and loops back; once the bar is met it
publishes; if it burns through max_attempts it escalates to a human node
instead of silently shipping a failing artifact.

The evaluator here is pure code so the example runs offline. Swap it for one
that prompts a backend (an LLMBackend scoring the text, or an AgentBackend
reviewing a diff) — the graph shape does not change.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from agentflow import END, START, Graph, Hooks, last
from agentflow.events import GateEvent
from agentflow.prebuilt import GateResult, GateState, add_quality_gate

TARGET = 80.0


class DraftState(GateState):
    draft: Annotated[str, last]
    quality: Annotated[str, last]  # where the outcome lands


class GateLog(Hooks):
    """Print each gate decision so you can see the loop work."""

    async def on_event(self, thread, step, node, event) -> None:
        if isinstance(event, GateEvent):
            score = f" score={event.score:.0f}" if event.score is not None else ""
            reasons = f" reasons={list(event.reasons)}" if event.reasons else ""
            print(f"  gate[{event.gate}] {event.phase} (attempt {event.attempt}){score}{reasons}")


async def draft(state, ctx) -> dict:
    return {"draft": "hello"}


async def revise(state, ctx) -> dict:
    # A real revise would use the gate's feedback; here we just lengthen it,
    # which raises the score the evaluator computes below.
    return {"draft": state.get("draft", "") + " world"}


async def evaluate(state, ctx) -> GateResult:
    # Toy score: 20 points per word, capped at 100.
    words = len(state.get("draft", "").split())
    score = min(words * 20.0, 100.0)
    passed = score >= TARGET
    return GateResult(
        passed=passed,
        score=score,
        reasons=[] if passed else ["too short, add detail"],
        feedback=None if passed else "expand the draft",
    )


async def publish(state, ctx) -> dict:
    return {"quality": "published"}


async def escalate(state, ctx) -> dict:
    # In a real run this would ctx.interrupt(QualityGateReview(...).to_payload())
    # to hand the artifact to a human. Here we just record the outcome.
    return {"quality": "escalated to human"}


def build(max_attempts: int) -> Graph:
    g = Graph(DraftState)
    g.add_node("draft", draft)
    g.add_node("revise", revise)
    g.add_node("publish", publish)
    g.add_node("escalate", escalate)
    add_quality_gate(
        g,
        "review",
        evaluate,
        on_pass="publish",
        on_fail="revise",
        on_escalate="escalate",
        max_attempts=max_attempts,
    )
    g.add_edge(START, "draft")
    g.add_edge("draft", "review")
    g.add_edge("revise", "review")
    g.add_edge("publish", END)
    g.add_edge("escalate", END)
    return g


async def main() -> None:
    print("Case 1: enough attempts to reach the bar")
    app = build(max_attempts=6).compile(hooks=GateLog())
    out = await app.invoke({})
    print(f"  -> {out['quality']!r}, final draft: {out['draft']!r}\n")

    print("Case 2: too few attempts -> escalate")
    app = build(max_attempts=2).compile(hooks=GateLog())
    out = await app.invoke({})
    print(f"  -> {out['quality']!r}, final draft: {out['draft']!r}")


if __name__ == "__main__":
    asyncio.run(main())
