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

"""Tests for the retry/timeout node wrappers."""

from __future__ import annotations

import asyncio
from typing import Annotated

import pytest
from agentflow import END, START, Graph, NodeError, State, add
from agentflow.errors import InterruptError
from agentflow.prebuilt import with_retry, with_timeout


class S(State):
    n: Annotated[int, add]


# --- with_retry ---


async def test_retry_succeeds_after_failures():
    attempts = {"count": 0}

    async def flaky(state, ctx):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise ValueError("transient")
        return {"n": attempts["count"]}

    wrapped = with_retry(flaky, retries=5, backoff=0)
    out = await wrapped({}, None)
    assert attempts["count"] == 3
    assert out == {"n": 3}


async def test_retry_exhausted_reraises_last():
    attempts = {"count": 0}

    async def always_fails(state, ctx):
        attempts["count"] += 1
        raise RuntimeError(f"fail {attempts['count']}")

    wrapped = with_retry(always_fails, retries=2, backoff=0)
    with pytest.raises(RuntimeError, match="fail 3"):
        await wrapped({}, None)
    assert attempts["count"] == 3  # 1 initial + 2 retries


async def test_retry_only_on_selected_exceptions():
    async def boom(state, ctx):
        raise KeyError("not retried")

    # KeyError is not in `on`, so it propagates on the first attempt.
    wrapped = with_retry(boom, retries=5, backoff=0, on=(ValueError,))
    with pytest.raises(KeyError):
        await wrapped({}, None)


async def test_retry_never_retries_interrupt():
    calls = {"count": 0}

    async def interrupts(state, ctx):
        calls["count"] += 1
        raise InterruptError("node", {"q": "?"})

    wrapped = with_retry(interrupts, retries=5, backoff=0)
    with pytest.raises(InterruptError):
        await wrapped({}, None)
    assert calls["count"] == 1  # interrupt is never retried


async def test_retry_rejects_negative():
    with pytest.raises(ValueError):
        with_retry(lambda s, c: None, retries=-1)


# --- with_timeout ---


async def test_timeout_passes_fast_node():
    async def quick(state, ctx):
        return {"n": 1}

    wrapped = with_timeout(quick, seconds=1.0)
    assert await wrapped({}, None) == {"n": 1}


async def test_timeout_fires_on_slow_node():
    async def slow(state, ctx):
        await asyncio.sleep(1.0)
        return {"n": 1}

    wrapped = with_timeout(slow, seconds=0.05)
    with pytest.raises(asyncio.TimeoutError):
        await wrapped({}, None)


async def test_timeout_lets_interrupt_through():
    async def interrupts(state, ctx):
        raise InterruptError("node", None)

    wrapped = with_timeout(interrupts, seconds=1.0)
    with pytest.raises(InterruptError):
        await wrapped({}, None)


# --- composition + engine integration ---


async def test_compose_retry_over_timeout():
    calls = {"count": 0}

    async def slow_then_ok(state, ctx):
        calls["count"] += 1
        if calls["count"] < 2:
            await asyncio.sleep(1.0)  # first attempt times out
        return {"n": calls["count"]}

    node = with_retry(
        with_timeout(slow_then_ok, seconds=0.05), retries=3, backoff=0, on=(asyncio.TimeoutError,)
    )
    out = await node({}, None)
    assert calls["count"] == 2
    assert out == {"n": 2}


async def test_wrapped_node_runs_in_graph_and_failure_becomes_nodeerror():
    async def always_fails(state, ctx):
        raise RuntimeError("nope")

    g = Graph(S)
    g.add_node("x", with_retry(always_fails, retries=1, backoff=0))
    g.add_edge(START, "x")
    g.add_edge("x", END)
    app = g.compile()
    with pytest.raises(NodeError):
        await app.invoke({})


async def test_retry_recovers_inside_graph():
    attempts = {"count": 0}

    async def flaky(state, ctx):
        attempts["count"] += 1
        if attempts["count"] < 2:
            raise ValueError("transient")
        return {"n": 42}

    g = Graph(S)
    g.add_node("x", with_retry(flaky, retries=3, backoff=0))
    g.add_edge(START, "x")
    g.add_edge("x", END)
    app = g.compile()
    out = await app.invoke({})
    assert out["n"] == 42
    assert attempts["count"] == 2
