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

"""Tests for the idempotency recipe: artifact_key and skip_if_done."""

from __future__ import annotations

from typing import Annotated

from agentflow import END, START, Graph, MemoryStore
from agentflow.prebuilt import artifact_key, skip_if_done
from agentflow.state import State, add, last

# --- artifact_key ---


def test_artifact_key_is_stable_across_dict_order():
    assert artifact_key({"a": 1, "b": 2}) == artifact_key({"b": 2, "a": 1})


def test_artifact_key_differs_by_content():
    assert artifact_key({"a": 1}) != artifact_key({"a": 2})


def test_artifact_key_handles_nested():
    k1 = artifact_key({"x": [1, {"y": 2}]})
    k2 = artifact_key({"x": [1, {"y": 2}]})
    assert k1 == k2 and len(k1) == 64  # sha256 hex


# --- skip_if_done ---


async def test_runs_once_then_serves_cache():
    calls = {"n": 0}
    store = MemoryStore()

    async def expensive(state, ctx):
        calls["n"] += 1
        return {"result": state["seed"] * 2}

    node = skip_if_done(
        expensive,
        store,
        namespace=("cache", "expensive"),
        key_from=lambda s: artifact_key(s["seed"]),
    )

    out1 = await node({"seed": 21}, None)
    out2 = await node({"seed": 21}, None)
    assert out1 == {"result": 42}
    assert out2 == {"result": 42}
    assert calls["n"] == 1  # second call served from cache


async def test_distinct_keys_run_separately():
    calls = {"n": 0}
    store = MemoryStore()

    async def work(state, ctx):
        calls["n"] += 1
        return {"result": state["seed"]}

    node = skip_if_done(
        work, store, namespace=("c",), key_from=lambda s: artifact_key(s["seed"])
    )
    await node({"seed": 1}, None)
    await node({"seed": 2}, None)
    await node({"seed": 1}, None)  # cached
    assert calls["n"] == 2


async def test_none_update_is_cached():
    calls = {"n": 0}
    store = MemoryStore()

    async def maybe(state, ctx):
        calls["n"] += 1
        return None

    node = skip_if_done(maybe, store, namespace=("c",), key_from=lambda s: "fixed")
    assert await node({}, None) is None
    r2 = await node({}, None)
    assert r2 == {}  # cached empty update short-circuits
    assert calls["n"] == 1


class S(State):
    seed: Annotated[int, last]
    result: Annotated[int, last]
    runs: Annotated[int, add]


async def test_skip_if_done_in_graph_across_two_runs():
    store = MemoryStore()
    calls = {"n": 0}

    async def compute(state, ctx):
        calls["n"] += 1
        return {"result": state["seed"] * 10, "runs": 1}

    node = skip_if_done(
        compute, store, namespace=("cache",), key_from=lambda s: artifact_key(s["seed"])
    )
    g = Graph(S)
    g.add_node("compute", node)
    g.add_edge(START, "compute")
    g.add_edge("compute", END)
    app = g.compile()

    out_a = await app.invoke({"seed": 5}, thread="a")
    out_b = await app.invoke({"seed": 5}, thread="b")  # different run, same content
    assert out_a["result"] == 50
    assert out_b["result"] == 50
    assert calls["n"] == 1  # cache shared across threads via the store


async def test_ttl_is_passed_through():
    # A zero/negative TTL means the item is already expired, so it never
    # short-circuits — the node runs every time.
    calls = {"n": 0}
    store = MemoryStore()

    async def work(state, ctx):
        calls["n"] += 1
        return {"x": 1}

    node = skip_if_done(
        work, store, namespace=("c",), key_from=lambda s: "k", ttl=-1.0
    )
    await node({}, None)
    await node({}, None)
    assert calls["n"] == 2
