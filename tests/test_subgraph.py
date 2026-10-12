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

"""Subgraph-as-node: a CompiledGraph embedded via add_node / add_subgraph."""

from __future__ import annotations

from typing import Annotated

from agentflow import END, START, Graph, MemoryCheckpointer, State, add, append, last


class Inner(State):
    n: Annotated[int, add]
    trace: Annotated[list, append]


class Outer(State):
    n: Annotated[int, add]
    trace: Annotated[list, append]
    result: Annotated[int, last]  # distinct channel for subgraph output
    outer_only: Annotated[str, last]


def _inner_graph(checkpointer=None):
    g = Graph(Inner)

    async def double(state, ctx):
        return {"n": state.get("n", 0), "trace": "inner_double"}  # add doubles n

    async def bump(state, ctx):
        return {"n": 1, "trace": "inner_bump"}

    g.add_node("double", double)
    g.add_node("bump", bump)
    g.add_edge(START, "double")
    g.add_edge("double", "bump")
    g.add_edge("bump", END)
    return g.compile(checkpointer=checkpointer)


async def test_subgraph_via_add_node_default_passthrough():
    """Default add_node embedding: shared channels flow both ways. The subgraph
    computes n and its result merges back through the parent's `add`."""
    g = Graph(Outer)

    async def seed(state, ctx):
        return {"n": 3}

    g.add_node("seed", seed)
    g.add_node("inner", _inner_graph(MemoryCheckpointer()))
    g.add_edge(START, "seed")
    g.add_edge("seed", "inner")
    g.add_edge("inner", END)
    out = await g.compile().invoke({"n": 0, "trace": [], "result": 0, "outer_only": ""})
    # seed: n=3. subgraph gets n=3 -> double adds 3 -> 6 -> bump +1 -> 7.
    # merged back via parent add: 3 + 7 = 10.
    assert out["n"] == 10


async def test_subgraph_with_output_map_avoids_double_count():
    """Explicit maps route the subgraph's result into a distinct parent channel
    so accumulators are not double-counted."""
    g = Graph(Outer)

    async def seed(state, ctx):
        return {"n": 4}

    g.add_node("seed", seed)
    g.add_subgraph(
        "inner",
        _inner_graph(MemoryCheckpointer()),
        input_map={"n": "n"},  # feed parent n -> subgraph n
        output_map={"n": "result"},  # subgraph n -> parent `result` (last)
    )
    g.add_edge(START, "seed")
    g.add_edge("seed", "inner")
    g.add_edge("inner", END)
    out = await g.compile().invoke({"n": 0, "trace": [], "result": 0, "outer_only": ""})
    # subgraph: n=4 -> double->8 -> bump->9. Routed to `result` (last), parent n untouched.
    assert out["n"] == 4
    assert out["result"] == 9


async def test_subgraph_only_sees_mapped_input():
    g = Graph(Outer)
    g.add_subgraph(
        "inner", _inner_graph(MemoryCheckpointer()), input_map={"n": "n"}, output_map={"n": "result"}
    )
    g.add_edge(START, "inner")
    g.add_edge("inner", END)
    out = await g.compile().invoke({"n": 2, "trace": [], "result": 0, "outer_only": "keep"})
    assert out["outer_only"] == "keep"  # untouched
    assert out["result"] == 5  # 2 -> double->4 -> bump->5
    assert out["n"] == 2  # parent n not modified by subgraph


async def test_subgraph_checkpoints_on_isolated_subthread():
    inner_cp = MemoryCheckpointer()
    inner = _inner_graph(checkpointer=inner_cp)

    g = Graph(Outer)
    g.add_subgraph("inner", inner, input_map={"n": "n"}, output_map={"n": "result"})
    g.add_edge(START, "inner")
    g.add_edge("inner", END)
    parent_cp = MemoryCheckpointer()
    app = g.compile(checkpointer=parent_cp)
    await app.invoke({"n": 1, "trace": [], "result": 0, "outer_only": ""}, thread="parent")

    parent_steps = [c async for c in app.history("parent")]
    assert parent_steps and all(c.thread == "parent" for c in parent_steps)

    sub_steps = [c async for c in inner_cp.history("parent::inner")]
    assert sub_steps and all(c.thread == "parent::inner" for c in sub_steps)

    leaked = [c async for c in parent_cp.history("parent::inner")]
    assert leaked == []


# --- bug 2: a subgraph interrupt must pause the parent ----------------------


class InnerHitl(State):
    answer: Annotated[str, last]
    trace: Annotated[list, append]


class OuterHitl(State):
    answer: Annotated[str, last]
    trace: Annotated[list, append]


def _inner_hitl_graph(checkpointer, *, interrupts=1):
    g = Graph(InnerHitl)

    async def ask(state, ctx):
        if interrupts == 1:
            a = await ctx.interrupt("inner_q")
            return {"answer": a, "trace": "inner_ask"}
        a = await ctx.interrupt("inner_q1")
        b = await ctx.interrupt("inner_q2")
        return {"answer": f"{a}+{b}", "trace": "inner_ask"}

    g.add_node("ask", ask)
    g.add_edge(START, "ask")
    g.add_edge("ask", END)
    return g.compile(checkpointer=checkpointer)


def _parent_with_subgraph(parent_cp, inner, *, lead_node=False):
    g = Graph(OuterHitl)

    async def lead(state, ctx):
        return {"trace": "parent_lead"}

    async def finish(state, ctx):
        return {"trace": "parent_finish"}

    g.add_subgraph("sub", inner, input_map={}, output_map={"answer": "answer"})
    g.add_node("finish", finish)
    if lead_node:
        g.add_node("lead", lead)
        g.add_edge(START, "lead")
        g.add_edge("lead", "sub")
    else:
        g.add_edge(START, "sub")
    g.add_edge("sub", "finish")
    g.add_edge("finish", END)
    return g.compile(checkpointer=parent_cp)


async def test_subgraph_interrupt_pauses_parent():
    inner = _inner_hitl_graph(MemoryCheckpointer())
    parent_cp = MemoryCheckpointer()
    app = _parent_with_subgraph(parent_cp, inner)

    await app.invoke({"answer": "", "trace": []}, thread="p")
    cp = await parent_cp.get("p")
    assert cp.interrupted and not cp.done
    assert cp.interrupt_node == "sub"
    assert cp.interrupt_payload == "inner_q"
    # The parent's downstream node must NOT have run.
    assert "parent_finish" not in cp.state["trace"]


async def test_subgraph_interrupt_resumes_child_and_parent():
    inner = _inner_hitl_graph(MemoryCheckpointer())
    parent_cp = MemoryCheckpointer()
    app = _parent_with_subgraph(parent_cp, inner)

    await app.invoke({"answer": "", "trace": []}, thread="p")
    out = await app.resume("p", "yes")
    cp = await parent_cp.get("p")
    assert cp.done
    assert out["answer"] == "yes"
    assert "parent_finish" in out["trace"]


async def test_subgraph_interrupt_resumes_at_parent_step_gt_1():
    """A plain node runs BEFORE the subgraph so the interrupt is at step > 1.
    This locks the stable sub-thread key: resume must find the child's
    checkpoint even though the parent step advanced."""
    inner = _inner_hitl_graph(MemoryCheckpointer())
    parent_cp = MemoryCheckpointer()
    app = _parent_with_subgraph(parent_cp, inner, lead_node=True)

    await app.invoke({"answer": "", "trace": []}, thread="p")
    cp = await parent_cp.get("p")
    assert cp.interrupted and cp.interrupt_node == "sub"
    assert "parent_lead" in cp.state["trace"]

    out = await app.resume("p", "ok")
    assert out["answer"] == "ok"
    assert "parent_finish" in out["trace"]


async def test_subgraph_two_inner_interrupts_across_two_parent_resumes():
    inner = _inner_hitl_graph(MemoryCheckpointer(), interrupts=2)
    parent_cp = MemoryCheckpointer()
    app = _parent_with_subgraph(parent_cp, inner)

    await app.invoke({"answer": "", "trace": []}, thread="p")
    cp1 = await parent_cp.get("p")
    assert cp1.interrupted and cp1.interrupt_payload == "inner_q1"

    await app.resume("p", "A")
    cp2 = await parent_cp.get("p")
    assert cp2.interrupted and cp2.interrupt_payload == "inner_q2"

    out = await app.resume("p", "B")
    assert out["answer"] == "A+B"


async def test_subgraph_interrupt_without_checkpointer_errors():
    from agentflow import GraphError

    inner = _inner_hitl_graph(None)  # subgraph has NO checkpointer
    parent_cp = MemoryCheckpointer()
    app = _parent_with_subgraph(parent_cp, inner)

    import pytest

    with pytest.raises(GraphError, match="sub"):
        await app.invoke({"answer": "", "trace": []}, thread="p")
