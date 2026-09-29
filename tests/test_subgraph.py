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
    g.add_node("inner", _inner_graph())
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
        _inner_graph(),
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
    g.add_subgraph("inner", _inner_graph(), input_map={"n": "n"}, output_map={"n": "result"})
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

    sub_steps = [c async for c in inner_cp.history("parent::inner@1")]
    assert sub_steps and all(c.thread == "parent::inner@1" for c in sub_steps)

    leaked = [c async for c in parent_cp.history("parent::inner@1")]
    assert leaked == []
