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

"""Prove super-step fan-out is genuinely concurrent and merges deterministically.

Two things are asserted:

1. Determinism: several nodes writing the same channel in one super-step have
   their updates folded through the channel reducer in frontier order — same
   result every run.
2. Real concurrency: the fan-out nodes overlap in time. Each node waits on a
   barrier that only releases once all fan-out nodes have arrived; if the
   engine ran them sequentially the barrier would never release and the test
   would hang (guarded by asyncio.wait_for).
"""

from __future__ import annotations

import asyncio
from typing import Annotated

import pytest
from agentflow import END, START, Graph, NodeError, State, add, append, union


class FanState(State):
    total: Annotated[int, add]
    log: Annotated[list, append]
    tags: Annotated[set, union]


def _fanout_graph(schema, worker_names, worker_fn, join_fn=None):
    g = Graph(schema)

    async def start(state, ctx):
        return {}

    g.add_node("start", start)
    for name in worker_names:
        g.add_node(name, worker_fn(name))
    g.add_node("join", join_fn or (lambda s, c: {}))

    g.add_edge(START, "start")
    g.add_conditional_edges("start", lambda s: list(worker_names), {n: n for n in worker_names})
    for name in worker_names:
        g.add_edge(name, "join")
    g.add_edge("join", END)
    return g


async def test_concurrent_add_is_deterministic():
    names = [f"w{i}" for i in range(5)]

    def worker(name):
        value = int(name[1:])

        async def fn(state, ctx):
            await asyncio.sleep(0.001)
            return {"total": value, "log": name}

        return fn

    app = _fanout_graph(FanState, names, worker).compile()
    out = await app.invoke({"total": 0, "log": [], "tags": set()})
    assert out["total"] == sum(range(5))  # 0+1+2+3+4 = 10
    # append preserves frontier (registration) order deterministically
    assert out["log"] == names


async def test_union_reducer_merges_concurrent_writes():
    names = ["a", "b", "c"]

    def worker(name):
        async def fn(state, ctx):
            return {"tags": {name}}

        return fn

    app = _fanout_graph(FanState, names, worker).compile()
    out = await app.invoke({"total": 0, "log": [], "tags": set()})
    assert out["tags"] == {"a", "b", "c"}


async def test_fanout_nodes_actually_overlap():
    """A barrier that only releases when all N nodes are inside proves the
    engine runs them concurrently, not one after another."""
    n = 4
    names = [f"w{i}" for i in range(n)]
    arrived = asyncio.Event()
    counter = {"in": 0}

    def worker(name):
        async def fn(state, ctx):
            counter["in"] += 1
            if counter["in"] == n:
                arrived.set()  # last one to arrive releases everyone
            # If execution were sequential, only 1 node would be "in" at a
            # time and this wait would never be released.
            await asyncio.wait_for(arrived.wait(), timeout=2.0)
            return {"total": 1, "log": name}

        return fn

    app = _fanout_graph(FanState, names, worker).compile()
    out = await asyncio.wait_for(app.invoke({"total": 0, "log": [], "tags": set()}), timeout=5.0)
    assert out["total"] == n
    assert counter["in"] == n


async def test_sequential_baseline_would_not_overlap():
    """Sanity: a single node that waits on an unset event DOES time out,
    confirming the barrier mechanism is real (not trivially satisfied)."""
    ev = asyncio.Event()

    async def waiter(state, ctx):
        await asyncio.wait_for(ev.wait(), timeout=0.1)  # nobody sets it
        return {}

    g = Graph(FanState)
    g.add_node("w", waiter)
    g.add_edge(START, "w")
    g.add_edge("w", END)
    app = g.compile()
    with pytest.raises(NodeError):  # wraps the TimeoutError
        await app.invoke({"total": 0, "log": [], "tags": set()})
