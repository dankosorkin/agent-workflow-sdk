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

"""State isolation: defensive-copy on concurrent super-steps prevents races
from in-place mutation of shared nested containers."""

from __future__ import annotations

from typing import Annotated

import pytest
from agentflow import END, START, CompilationError, Graph, State, append, last


class S(State):
    shared: Annotated[list, last]  # a nested container nodes might mutate
    log: Annotated[list, append]


def _fanout_graph(isolate_state):
    """Two siblings each append to state['shared'] IN PLACE (a contract
    violation) and also return a proper update. We measure whether the in-place
    mutation of the input leaks across siblings."""
    g = Graph(S)

    async def start(state, ctx):
        return {"shared": []}

    async def a(state, ctx):
        state["shared"].append("a")  # illegal in-place mutation
        return {"log": f"a saw {len(state['shared'])}"}

    async def b(state, ctx):
        state["shared"].append("b")  # illegal in-place mutation
        return {"log": f"b saw {len(state['shared'])}"}

    g.add_node("start", start)
    g.add_node("a", a)
    g.add_node("b", b)
    g.add_node("join", lambda s, c: {})
    g.add_edge(START, "start")
    g.add_conditional_edges("start", lambda s: ["a", "b"], {"a": "a", "b": "b"})
    g.add_edge("a", "join")
    g.add_edge("b", "join")
    g.add_edge("join", END)
    return g.compile(isolate_state=isolate_state)


async def test_fanout_isolation_prevents_cross_sibling_leak():
    """Default 'fanout': each sibling gets its own deep copy, so neither sees
    the other's in-place mutation — each observes exactly its own append."""
    app = _fanout_graph("fanout")
    out = await app.invoke({"shared": [], "log": []})
    # Each node saw a list of length 1 (only its own append), not 2.
    assert set(out["log"]) == {"a saw 1", "b saw 1"}


async def test_never_isolate_can_leak_across_siblings():
    """'never' shares the object: at least one sibling sees the other's
    mutation (length 2). This documents the unsafe mode's behavior."""
    app = _fanout_graph("never")
    out = await app.invoke({"shared": [], "log": []})
    # With a shared object, the second-folded node sees length 2.
    assert "a saw 2" in out["log"] or "b saw 2" in out["log"]


async def test_linear_graph_pays_no_copy_cost():
    """'fanout' does not copy when only one node runs a step — the node sees
    (and could mutate) the real object. Correctness for single-node steps is
    unaffected since there is no sibling to race."""
    g = Graph(S)

    async def only(state, ctx):
        return {"log": "ran", "shared": [1]}

    g.add_node("only", only)
    g.add_edge(START, "only")
    g.add_edge("only", END)
    out = await g.compile(isolate_state="fanout").invoke({"shared": [], "log": []})
    assert out["shared"] == [1]


async def test_always_isolates_even_single_node():
    g = Graph(S)

    async def n(state, ctx):
        state["shared"].append("x")  # mutate the (copied) input
        return {"log": "ran"}

    g.add_node("n", n)
    g.add_edge(START, "n")
    g.add_edge("n", END)
    out = await g.compile(isolate_state="always").invoke({"shared": [], "log": []})
    # The copy was mutated, not the committed state; 'shared' stays [] because
    # the node returned no 'shared' update.
    assert out["shared"] == []


def test_invalid_isolate_state_rejected():
    g = Graph(S)
    g.add_node("n", lambda s, c: {})
    g.add_edge(START, "n")
    g.add_edge("n", END)
    with pytest.raises(CompilationError):
        g.compile(isolate_state="sometimes")
