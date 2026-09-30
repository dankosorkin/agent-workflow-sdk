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

"""Tests for IdempotentOp: the effect-level claim -> run -> commit primitive."""

from __future__ import annotations

from typing import Annotated

import pytest
from agentflow import END, START, Graph, MemoryStore
from agentflow.prebuilt import IdempotentOp, IncompleteEffectError, effect_key
from agentflow.state import State, add, last

NS = ("effects", "test")


def _counting_effect():
    calls = {"n": 0}

    async def fn():
        calls["n"] += 1
        return {"receipt": f"r-{calls['n']}"}

    return fn, calls


# --- done path: run once, then serve the stored result ---


async def test_runs_once_then_returns_stored_result():
    store = MemoryStore()
    op = IdempotentOp(store, NS)
    fn, calls = _counting_effect()

    r1 = await op.run("k", fn)
    r2 = await op.run("k", fn)
    assert r1 == r2 == {"receipt": "r-1"}
    assert calls["n"] == 1  # fn not called the second time


async def test_distinct_keys_run_independently():
    store = MemoryStore()
    op = IdempotentOp(store, NS)
    fn, calls = _counting_effect()

    await op.run("a", fn)
    await op.run("b", fn)
    await op.run("a", fn)  # cached
    assert calls["n"] == 2


# --- failure inside fn drops the claim so a retry starts clean ---


async def test_exception_in_fn_drops_claim_and_reraises():
    store = MemoryStore()
    op = IdempotentOp(store, NS)
    attempts = {"n": 0}

    async def flaky():
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("boom")
        return {"ok": True}

    with pytest.raises(RuntimeError, match="boom"):
        await op.run("k", flaky)
    # claim was removed, so a later attempt runs fn again and can succeed
    assert await store.get(NS, "k") is None
    result = await op.run("k", flaky)
    assert result == {"ok": True}
    assert attempts["n"] == 2


# --- interrupted in-flight: each policy ---


async def test_in_flight_error_policy_raises():
    store = MemoryStore()
    op = IdempotentOp(store, NS)
    await store.put(NS, "k", {"status": "in_flight"})  # simulate a dead attempt
    fn, calls = _counting_effect()

    with pytest.raises(IncompleteEffectError) as ei:
        await op.run("k", fn, on_incomplete="error")
    assert ei.value.key == "k"
    assert ei.value.namespace == NS
    assert calls["n"] == 0  # fn never ran


async def test_in_flight_skip_policy_returns_none():
    store = MemoryStore()
    op = IdempotentOp(store, NS)
    await store.put(NS, "k", {"status": "in_flight"})
    fn, calls = _counting_effect()

    result = await op.run("k", fn, on_incomplete="skip")
    assert result is None
    assert calls["n"] == 0


async def test_in_flight_rerun_policy_runs_and_commits():
    store = MemoryStore()
    op = IdempotentOp(store, NS)
    await store.put(NS, "k", {"status": "in_flight"})
    fn, calls = _counting_effect()

    result = await op.run("k", fn, on_incomplete="rerun")
    assert result == {"receipt": "r-1"}
    assert calls["n"] == 1
    # now committed: a further call serves the stored result
    again = await op.run("k", fn, on_incomplete="rerun")
    assert again == {"receipt": "r-1"}
    assert calls["n"] == 1


async def test_error_is_the_default_policy():
    store = MemoryStore()
    op = IdempotentOp(store, NS)
    await store.put(NS, "k", {"status": "in_flight"})
    fn, _ = _counting_effect()
    with pytest.raises(IncompleteEffectError):
        await op.run("k", fn)  # no on_incomplete -> "error"


# --- effect_key ---


def test_effect_key_is_stable_and_order_independent():
    assert effect_key("a", {"x": 1, "y": 2}) == effect_key("a", {"y": 2, "x": 1})
    assert effect_key("a") != effect_key("b")
    assert len(effect_key("a")) == 64  # sha256 hex


# --- integration: protecting an effect inside a node, across a re-run ---


class S(State):
    seed: Annotated[int, last]
    receipt: Annotated[str, last]
    effect_runs: Annotated[int, add]


async def test_effect_protected_in_node_across_two_runs():
    store = MemoryStore()
    op = IdempotentOp(store, ("effects", "publish"))
    ran = {"n": 0}

    async def node(state, ctx):
        async def publish():
            ran["n"] += 1
            return f"published-{state['seed']}"

        key = effect_key("publish", state["seed"])
        receipt = await op.run(key, publish, on_incomplete="error")
        return {"receipt": receipt, "effect_runs": 1}

    g = Graph(S)
    g.add_node("node", node)
    g.add_edge(START, "node")
    g.add_edge("node", END)
    app = g.compile()

    out_a = await app.invoke({"seed": 7}, thread="a")
    out_b = await app.invoke({"seed": 7}, thread="b")  # same effect key, new run
    assert out_a["receipt"] == "published-7"
    assert out_b["receipt"] == "published-7"
    assert ran["n"] == 1  # the effect fired exactly once despite two runs


async def test_default_ttl_and_per_call_ttl_are_accepted():
    # A negative TTL is already expired, so the done marker never short-circuits
    # and the effect runs each time — proves ttl is threaded through.
    store = MemoryStore()
    op = IdempotentOp(store, NS, default_ttl=-1.0)
    fn, calls = _counting_effect()
    await op.run("k", fn)
    await op.run("k", fn)
    assert calls["n"] == 2
