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

"""Tests for the quality-gate helper: the three routes, attempts, and events."""

from __future__ import annotations

from typing import Annotated

import pytest
from agentflow import END, START, Graph, Hooks
from agentflow.events import GateEvent
from agentflow.prebuilt import GateResult, GateState, add_quality_gate
from agentflow.state import add, last


class S(GateState):
    artifact: Annotated[str, last]
    revisions: Annotated[int, add]
    escalated: Annotated[bool, last]


async def _revise(state, ctx):
    # Grow the artifact by one char per revision.
    return {"artifact": state.get("artifact", "") + "x", "revisions": 1}


async def _publish(state, ctx):
    return {}


async def _ask_human(state, ctx):
    return {"escalated": True}


def _build(*, threshold: int, max_attempts: int) -> Graph:
    async def evaluate(state, ctx) -> GateResult:
        n = len(state.get("artifact", ""))
        return GateResult(
            passed=n >= threshold,
            score=float(n),
            reasons=[] if n >= threshold else ["too short"],
            feedback=None if n >= threshold else "make it longer",
        )

    g = Graph(S)
    g.add_node("revise", _revise)
    g.add_node("publish", _publish)
    g.add_node("ask_human", _ask_human)
    add_quality_gate(
        g,
        "review",
        evaluate,
        on_pass="publish",
        on_fail="revise",
        on_escalate="ask_human",
        max_attempts=max_attempts,
    )
    g.add_edge(START, "review")
    g.add_edge("revise", "review")
    g.add_edge("publish", END)
    g.add_edge("ask_human", END)
    return g


class _Capture(Hooks):
    def __init__(self) -> None:
        self.events: list[GateEvent] = []

    async def on_event(self, thread, step, node, event) -> None:
        if isinstance(event, GateEvent):
            self.events.append(event)


# --- the three routes ---


async def test_gate_passes_on_first_attempt():
    app = _build(threshold=3, max_attempts=5).compile()
    out = await app.invoke({"artifact": "abcd"})
    assert out.get("revisions", 0) == 0  # never revised
    assert out["gate_attempts"] == {"review": 1}
    assert out["gate_results"]["review"].passed is True


async def test_gate_fails_then_passes_via_revise():
    app = _build(threshold=3, max_attempts=10).compile()
    out = await app.invoke({"artifact": ""})
    # empty -> "x","xx","xxx"; passes when length reaches 3 on the 4th visit
    assert out["artifact"] == "xxx"
    assert out["gate_attempts"] == {"review": 4}
    assert out["gate_results"]["review"].passed is True
    assert not out.get("escalated")


async def test_gate_escalates_at_max_attempts():
    app = _build(threshold=3, max_attempts=2).compile()
    out = await app.invoke({"artifact": ""})
    assert out["escalated"] is True
    assert out["gate_attempts"] == {"review": 2}
    assert out["gate_results"]["review"].passed is False


# --- events ---


async def test_gate_emits_lifecycle_events():
    cap = _Capture()
    app = _build(threshold=2, max_attempts=5).compile(hooks=cap)
    await app.invoke({"artifact": ""})
    phases = [(e.phase, e.attempt) for e in cap.events]
    # gate runs before revise, so: attempt1 len0 fail -> revise "x";
    # attempt2 len1 fail -> revise "xx"; attempt3 len2 pass.
    assert phases == [
        ("started", 1),
        ("failed", 1),
        ("started", 2),
        ("failed", 2),
        ("started", 3),
        ("passed", 3),
    ]


async def test_escalated_event_carries_reasons():
    cap = _Capture()
    app = _build(threshold=5, max_attempts=1).compile(hooks=cap)
    await app.invoke({"artifact": ""})
    escalated = [e for e in cap.events if e.phase == "escalated"]
    assert len(escalated) == 1
    assert escalated[0].reasons == ("too short",)
    assert escalated[0].gate == "review"


# --- validation ---


async def test_duplicate_gate_name_rejected():
    g = Graph(S)
    g.add_node("review", _publish)
    with pytest.raises(ValueError, match="already exists"):
        add_quality_gate(
            g, "review", _fake_eval, on_pass=END, on_fail=END, on_escalate=END
        )


async def test_max_attempts_must_be_positive():
    g = Graph(S)
    with pytest.raises(ValueError, match="max_attempts"):
        add_quality_gate(
            g, "review", _fake_eval, on_pass=END, on_fail=END, on_escalate=END, max_attempts=0
        )


async def _fake_eval(state, ctx) -> GateResult:
    return GateResult(passed=True)


# --- two gates coexist without colliding ---


async def test_two_gates_share_state_without_collision():
    class T(GateState):
        stage: Annotated[str, last]

    async def plan_eval(state, ctx):
        return GateResult(passed=True, score=1.0)

    async def review_eval(state, ctx):
        return GateResult(passed=True, score=2.0)

    async def implement(state, ctx):
        return {"stage": "implemented"}

    async def done(state, ctx):
        return {}

    g = Graph(T)
    g.add_node("implement", implement)
    g.add_node("done", done)
    add_quality_gate(
        g, "plan_gate", plan_eval, on_pass="implement", on_fail="implement", on_escalate="done"
    )
    add_quality_gate(
        g, "review_gate", review_eval, on_pass="done", on_fail="implement", on_escalate="done"
    )
    g.add_edge(START, "plan_gate")
    g.add_edge("implement", "review_gate")
    g.add_edge("done", END)
    out = await g.compile().invoke({})
    assert out["gate_attempts"] == {"plan_gate": 1, "review_gate": 1}
    assert set(out["gate_results"]) == {"plan_gate", "review_gate"}
    assert out["gate_results"]["plan_gate"].score == 1.0
    assert out["gate_results"]["review_gate"].score == 2.0
