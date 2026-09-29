# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""Typed provider errors + concurrency budgets (backend + super-step)."""

from __future__ import annotations

import asyncio
from typing import Annotated

import httpx
import pytest
from agentflow import (
    END,
    START,
    BackendRateLimitError,
    BackendTransportError,
    Graph,
    State,
    add,
)
from agentflow.backends._http import RetryPolicy
from agentflow.backends.ollama import OllamaBackend
from agentflow.events import Message


def _ndjson_ok() -> bytes:
    import json

    return (
        json.dumps(
            {"message": {"role": "assistant", "content": "hi"}, "done": True, "done_reason": "stop"}
        )
        + "\n"
    ).encode()


def _backend(handler, **kw):
    b = OllamaBackend("m", retry=RetryPolicy(max_retries=0), **kw)
    b._client = httpx.AsyncClient(base_url=b.host, transport=httpx.MockTransport(handler))
    return b


async def _drain(b):
    return [e async for e in b.chat([Message(role="user", content="hi")])]


# --- typed errors ---


async def test_429_raises_rate_limit_error_with_retry_after():
    def handler(request):
        return httpx.Response(429, headers={"retry-after": "12"}, content=b"slow down")

    b = _backend(handler)
    with pytest.raises(BackendRateLimitError) as ei:
        await _drain(b)
    await b.close()
    assert ei.value.status == 429
    assert ei.value.retry_after == 12.0


async def test_500_raises_transport_error_with_status():
    def handler(request):
        return httpx.Response(500, content=b"boom")

    b = _backend(handler)
    with pytest.raises(BackendTransportError) as ei:
        await _drain(b)
    await b.close()
    assert ei.value.status == 500
    assert not isinstance(ei.value, BackendRateLimitError)


# --- per-backend concurrency ---


async def test_backend_max_concurrency_limits_inflight():
    def handler(request):
        # httpx MockTransport handler is sync; can't easily block here, so we
        # assert the semaphore via the guard object instead (below).
        return httpx.Response(200, content=_ndjson_ok())

    b = _backend(handler, max_concurrency=2)
    # The guard should be a Semaphore with the configured value.
    guard = b.concurrency_guard()
    assert isinstance(guard, asyncio.Semaphore)
    assert guard._value == 2
    # And it is reused across calls (same instance).
    assert b.concurrency_guard() is guard
    await b.close()


async def test_no_concurrency_guard_by_default():
    import contextlib

    b = _backend(lambda r: httpx.Response(200, content=_ndjson_ok()))
    guard = b.concurrency_guard()
    # nullcontext when unset
    assert isinstance(guard, contextlib.nullcontext)
    await b.close()


# --- super-step fan-out concurrency limit ---


class S(State):
    n: Annotated[int, add]


async def test_max_node_concurrency_bounds_fanout():
    """With max_node_concurrency=2 and 4 fan-out nodes, no more than 2 run at once."""
    live = {"cur": 0, "max": 0}

    def worker(name):
        async def fn(state, ctx):
            live["cur"] += 1
            live["max"] = max(live["max"], live["cur"])
            await asyncio.sleep(0.02)
            live["cur"] -= 1
            return {"n": 1}

        return fn

    names = [f"w{i}" for i in range(4)]
    g = Graph(S)

    async def start(state, ctx):
        return {}

    g.add_node("start", start)
    for nm in names:
        g.add_node(nm, worker(nm))
    g.add_node("join", lambda s, c: {})
    g.add_edge(START, "start")
    g.add_conditional_edges("start", lambda s: list(names), {nm: nm for nm in names})
    for nm in names:
        g.add_edge(nm, "join")
    g.add_edge("join", END)

    app = g.compile(max_node_concurrency=2)
    out = await app.invoke({"n": 0})
    assert out["n"] == 4
    assert live["max"] <= 2  # never more than 2 concurrent


async def test_unbounded_fanout_runs_all_at_once():
    live = {"cur": 0, "max": 0}

    def worker(name):
        async def fn(state, ctx):
            live["cur"] += 1
            live["max"] = max(live["max"], live["cur"])
            await asyncio.sleep(0.02)
            live["cur"] -= 1
            return {"n": 1}

        return fn

    names = [f"w{i}" for i in range(4)]
    g = Graph(S)
    g.add_node("start", lambda s, c: {})
    for nm in names:
        g.add_node(nm, worker(nm))
    g.add_node("join", lambda s, c: {})
    g.add_edge(START, "start")
    g.add_conditional_edges("start", lambda s: list(names), {nm: nm for nm in names})
    for nm in names:
        g.add_edge(nm, "join")
    g.add_edge("join", END)

    app = g.compile()  # no limit
    await app.invoke({"n": 0})
    assert live["max"] == 4  # all four overlapped
