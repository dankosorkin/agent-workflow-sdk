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

"""Tests for the token/cost budget: record_usage, over_budget, TokenMeter."""

from __future__ import annotations

from typing import Annotated

from agentflow import END, START, Graph, TurnEnd, Usage, last
from agentflow.prebuilt import BudgetState, TokenMeter, over_budget, record_usage


class S(BudgetState):
    rounds: Annotated[int, last]


# --- record_usage ----------------------------------------------------------


def test_record_usage_none_is_empty():
    assert record_usage(None) == {}


def test_record_usage_adds_reported_fields():
    u = Usage(input_tokens=100, output_tokens=40)
    update = record_usage(u)
    assert update == {"budget": {"input_tokens": 100, "output_tokens": 40, "total_tokens": 140}}


def test_record_usage_prefers_reported_total():
    u = Usage(input_tokens=100, output_tokens=40, total_tokens=999)
    assert record_usage(u)["budget"]["total_tokens"] == 999


def test_record_usage_cost_only():
    u = Usage(cost_usd=0.25)
    assert record_usage(u) == {"budget": {"cost_usd": 0.25}}


def test_record_usage_all_none_is_empty():
    assert record_usage(Usage()) == {}


# --- over_budget -----------------------------------------------------------


def test_over_budget_no_caps_is_false():
    assert over_budget({"budget": {"total_tokens": 10**9}}) is False


def test_over_budget_tokens():
    state = {"budget": {"total_tokens": 100}}
    assert over_budget(state, max_tokens=100) is True  # >= is over
    assert over_budget(state, max_tokens=101) is False


def test_over_budget_cost():
    state = {"budget": {"cost_usd": 5.0}}
    assert over_budget(state, max_cost_usd=5.0) is True
    assert over_budget(state, max_cost_usd=5.01) is False


def test_over_budget_missing_budget_reads_zero():
    assert over_budget({}, max_tokens=1) is False


# --- accumulation + stop in a real graph -----------------------------------


async def test_budget_accumulates_and_stops():
    async def propose(state, ctx):
        turn = Usage(input_tokens=20000, output_tokens=10000)  # 30k per round
        return {"rounds": state.get("rounds", 0) + 1, **record_usage(turn)}

    def route(state):
        return "stop" if over_budget(state, max_tokens=100000) else "again"

    g = Graph(S)
    g.add_node("propose", propose)
    g.add_edge(START, "propose")
    g.add_conditional_edges("propose", route, {"again": "propose", "stop": END})

    out = await g.compile(step_limit=50).invoke({})
    assert out["rounds"] == 4  # 4*30k = 120k, crosses 100k on round 4
    assert out["budget"]["total_tokens"] == 120000


async def test_budget_survives_resume():
    from agentflow import MemoryCheckpointer

    class RS(BudgetState):
        rounds: Annotated[int, last]

    async def spend(state, ctx):
        # record the spend first, then the gate node may pause
        turn = Usage(total_tokens=50000)
        return {"rounds": state.get("rounds", 0) + 1, **record_usage(turn)}

    async def checkpoint_gate(state, ctx):
        # pause exactly once, after the first round's spend is in state
        if state.get("rounds") == 1:
            await ctx.interrupt({"pause": True})
        return {}

    g = Graph(RS)
    g.add_node("spend", spend)
    g.add_node("gate", checkpoint_gate)
    g.add_edge(START, "spend")
    g.add_edge("spend", "gate")
    g.add_conditional_edges(
        "gate",
        lambda s: "stop" if over_budget(s, max_tokens=150000) else "again",
        {"again": "spend", "stop": END},
    )
    app = g.compile(checkpointer=MemoryCheckpointer(), step_limit=50)
    await app.invoke({}, thread="b1")
    # parked after the first spend; it is already in the checkpoint
    mid = await app.get_state("b1")
    assert mid.state["budget"]["total_tokens"] == 50000
    out = await app.resume("b1", value=None)
    # resume continues adding onto the persisted budget, not from zero
    assert out["budget"]["total_tokens"] >= 150000


# --- TokenMeter ------------------------------------------------------------


async def test_token_meter_sums_from_emitted_turnends():
    async def propose(state, ctx):
        ctx.emit(TurnEnd(usage=Usage(input_tokens=100, output_tokens=50)))
        return {"rounds": state.get("rounds", 0) + 1}

    g = Graph(S)
    g.add_node("propose", propose)
    g.add_edge(START, "propose")
    g.add_conditional_edges(
        "propose",
        lambda s: "stop" if s.get("rounds", 0) >= 3 else "again",
        {"again": "propose", "stop": END},
    )
    meter = TokenMeter()
    await g.compile(step_limit=50, hooks=meter).invoke({})
    s = meter.summary()
    assert s["turns"] == 3
    assert s["total_tokens"] == 3 * 150
    assert s["input_tokens"] == 300 and s["output_tokens"] == 150


async def test_token_meter_ignores_events_without_usage():
    from agentflow import TextChunk

    async def propose(state, ctx):
        ctx.emit(TextChunk(text="hi"))  # no usage
        ctx.emit(TurnEnd(usage=None))  # explicit no usage
        return {"rounds": 1}

    g = Graph(S)
    g.add_node("propose", propose)
    g.add_edge(START, "propose")
    g.add_edge("propose", END)
    meter = TokenMeter()
    await g.compile(hooks=meter).invoke({})
    assert meter.summary()["turns"] == 0  # nothing with usage seen
