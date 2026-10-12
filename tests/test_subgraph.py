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

    # The sub-thread now carries the per-invocation ``#inv`` suffix (bug 3).
    sub_steps = [c async for c in inner_cp.history("parent::inner#0")]
    assert sub_steps and all(c.thread == "parent::inner#0" for c in sub_steps)

    leaked = [c async for c in parent_cp.history("parent::inner#0")]
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


# --- bug 3: a loop that re-enters a subgraph node must not collide ----------


class LoopState(State):
    i: Annotated[int, last]
    done_child: Annotated[list, append]
    answer: Annotated[str, last]


class LoopInner(State):
    i: Annotated[int, last]
    answer: Annotated[str, last]
    steps: Annotated[list, append]


async def test_loop_revisits_subgraph_node_fresh_invocation():
    """Bug 3: a parent loop re-enters the same subgraph node. Iteration 0's
    child finishes; iteration 1's child pauses at a HITL interrupt. The old
    ``{thread}::{name}`` key returned iteration 0's finished checkpoint for
    iteration 1, so the parent never saw the new interrupt and ran to
    step_limit. The per-invocation ``#inv`` suffix keeps them distinct.

    Pre-fix failure mode: the parent never becomes ``interrupted`` and instead
    raises a step-limit NodeError (or completes without pausing).
    """
    inner_cp = MemoryCheckpointer()

    def _inner():
        g = Graph(LoopInner)

        async def child(state, ctx):
            # On the FIRST invocation (i == 0) the child finishes immediately.
            # On the SECOND invocation (i == 1) it interrupts for a human.
            if state.get("i", 0) >= 1:
                a = await ctx.interrupt("inner_pause")
                return {"answer": a, "steps": "paused"}
            return {"answer": "auto", "steps": "auto"}

        g.add_node("child", child)
        g.add_edge(START, "child")
        g.add_edge("child", END)
        return g.compile(checkpointer=inner_cp)

    g = Graph(LoopState)
    inner = _inner()

    async def tick(state, ctx):
        return {"i": state.get("i", 0) + 1}

    g.add_subgraph("sub", inner, input_map={"i": "i"}, output_map={"answer": "answer"})
    g.add_node("tick", tick)

    def route(state):
        # Run the subgraph for i in {0, 1}: enter sub, tick, enter sub again.
        return "sub" if state.get("i", 0) < 2 else END

    g.add_edge(START, "sub")
    g.add_edge("sub", "tick")
    g.add_conditional_edges("tick", route, {"sub": "sub", END: END})
    parent_cp = MemoryCheckpointer()
    app = g.compile(checkpointer=parent_cp, step_limit=20)

    await app.invoke({"i": 0, "done_child": [], "answer": ""}, thread="parent")

    cp = await parent_cp.get("parent")
    assert cp.interrupted and not cp.done
    assert cp.interrupt_node == "sub"

    # iteration 0's child is independently done; iteration 1's child is the new
    # interrupted checkpoint — on distinct sub-threads.
    cp0 = await inner_cp.get("parent::sub#0")
    cp1 = await inner_cp.get("parent::sub#1")
    assert cp0 is not None and cp0.done
    assert cp1 is not None and cp1.interrupted and not cp1.done


class SibInner(State):
    tag: Annotated[str, last]
    out: Annotated[str, last]


_sibling_side_effects: list[str] = []


async def test_resume_does_not_reinvoke_finished_sibling():
    """Bug 3 (concurrent frontier): one super-step fans out to two subgraph
    nodes. ``child_a`` interrupts; ``child_b`` runs to completion. On resume the
    whole frontier replays — ``child_a`` must reuse ``#0`` and ``child_b`` must
    NOT be re-invoked from START (its side effect fires exactly once total).

    Pre-fix failure mode: both reused the stable ``{thread}::{name}`` key so the
    allocation question did not arise, but with per-invocation keys a naive rule
    that bumps every non-interrupt node would re-run the finished sibling.
    """
    _sibling_side_effects.clear()
    cp_a = MemoryCheckpointer()
    cp_b = MemoryCheckpointer()

    def _mk_a():
        g = Graph(SibInner)

        async def a(state, ctx):
            v = await ctx.interrupt("pause_a")
            return {"out": v}

        g.add_node("a", a)
        g.add_edge(START, "a")
        g.add_edge("a", END)
        return g.compile(checkpointer=cp_a)

    def _mk_b():
        g = Graph(SibInner)

        async def b(state, ctx):
            _sibling_side_effects.append("b-ran")
            return {"out": "b-done"}

        g.add_node("b", b)
        g.add_edge(START, "b")
        g.add_edge("b", END)
        return g.compile(checkpointer=cp_b)

    class Fan(State):
        answer: Annotated[str, last]

    g = Graph(Fan)
    g.add_subgraph("child_a", _mk_a(), input_map={}, output_map={"out": "answer"})
    g.add_subgraph("child_b", _mk_b(), input_map={}, output_map={})
    g.add_edge(START, "child_a")
    g.add_edge(START, "child_b")
    g.add_edge("child_a", END)
    g.add_edge("child_b", END)
    parent_cp = MemoryCheckpointer()
    app = g.compile(checkpointer=parent_cp)

    await app.invoke({"answer": ""}, thread="p")
    cp = await parent_cp.get("p")
    assert cp.interrupted and cp.interrupt_node == "child_a"
    assert cp.invocations.get("child_a") == 0 and cp.invocations.get("child_b") == 0
    assert _sibling_side_effects == ["b-ran"]  # ran once pre-resume

    await app.resume("p", "answered")
    # child_b must not have been re-invoked on resume.
    assert _sibling_side_effects == ["b-ran"]
    assert [c async for c in cp_b.history("p::child_b#0")]  # same sub-thread, no #1
    assert await cp_b.get("p::child_b#1") is None


_ordinary_child_steps: list[str] = []


async def test_resume_ordinary_child_checkpoint_does_not_repeat_step():
    """Bug 5: the child's first step is checkpointed, its second step crashes,
    leaving an ORDINARY (non-interrupt) unfinished child checkpoint plus an
    ordinary unfinished PARENT checkpoint. A parent-level resume must resume the
    child from its own checkpoint, not re-invoke it from START (which would run
    the child's first step twice).

    Pre-fix failure mode: ``run_subgraph`` keyed resume on ``child_cp.interrupted``,
    so an ordinary post-crash child checkpoint was treated as a fresh invoke and
    the child's first step ran twice.
    """
    _ordinary_child_steps.clear()

    class RaiseOnSecondPut(MemoryCheckpointer):
        def __init__(self):
            super().__init__()
            self._puts = 0

        async def put(self, cp, *, if_revision=None):
            self._puts += 1
            # Allow the first step's checkpoint; crash before the second lands.
            if self._puts == 2:
                raise RuntimeError("boom: second child put")
            return await super().put(cp, if_revision=if_revision)

    child_cp = RaiseOnSecondPut()

    def _inner():
        g = Graph(LoopInner)

        async def step1(state, ctx):
            _ordinary_child_steps.append("step1")
            return {"steps": "s1"}

        async def step2(state, ctx):
            return {"steps": "s2"}

        g.add_node("step1", step1)
        g.add_node("step2", step2)
        g.add_edge(START, "step1")
        g.add_edge("step1", "step2")
        g.add_edge("step2", END)
        return g.compile(checkpointer=g_child_cp)

    g_child_cp = child_cp
    inner = _inner()

    class P(State):
        answer: Annotated[str, last]
        trace: Annotated[list, append]

    g = Graph(P)

    async def lead(state, ctx):
        return {"trace": "parent_lead"}

    g.add_node("lead", lead)
    g.add_subgraph("child", inner, input_map={}, output_map={})
    g.add_edge(START, "lead")
    g.add_edge("lead", "child")
    g.add_edge("child", END)
    parent_cp = MemoryCheckpointer()
    app = g.compile(checkpointer=parent_cp)

    # First drive: the parent's lead step checkpoints, then the subgraph node's
    # child crashes inside the child's second put, leaving an ordinary unfinished
    # PARENT checkpoint (frontier == ["child"]) and an ordinary unfinished CHILD
    # checkpoint.
    import pytest
    from agentflow.errors import NodeError

    with pytest.raises(NodeError, match="boom"):
        await app.invoke({"answer": "", "trace": []}, thread="p")

    assert _ordinary_child_steps == ["step1"]
    # The child's stored checkpoint is ordinary (not interrupted) and unfinished.
    ccp = await child_cp.get("p::child#0")
    assert ccp is not None and not ccp.interrupted and not ccp.done
    # The parent checkpoint is ordinary unfinished (frontier still holds "child").
    pcp = await parent_cp.get("p")
    assert pcp is not None and not pcp.interrupted and not pcp.done
    assert "child" in pcp.next

    # Make the child's second put succeed on the re-drive, then resume at parent.
    child_cp._puts = 10  # past the crash trigger
    await app.resume("p")

    # The child's step1 ran EXACTLY once across both drives — it resumed from
    # its own checkpoint rather than restarting from START.
    assert _ordinary_child_steps == ["step1"]
    assert await child_cp.get("p::child#0") is not None


async def test_runtime_invocation_reuse_then_bump():
    """Runtime-level: the ordinal is reused on the first resumed super-step
    (replaying) and bumped on a later forward re-entry to the same node."""
    from agentflow.runtime import _invocation_for

    # fresh allocation starts at 0 and bumps
    inv0, m0 = _invocation_for("child", {}, replaying=False)
    assert inv0 == 0 and m0 == {"child": 0}
    inv1, m1 = _invocation_for("child", m0, replaying=False)
    assert inv1 == 1 and m1 == {"child": 1}
    # on a replayed super-step the stored ordinal is reused, map unchanged
    inv_r, m_r = _invocation_for("child", {"child": 1}, replaying=True)
    assert inv_r == 1 and m_r == {"child": 1}
    # a node unseen on a replayed step still allocates fresh
    inv_new, m_new = _invocation_for("other", {"child": 1}, replaying=True)
    assert inv_new == 0 and m_new == {"child": 1, "other": 0}
    # the helper never mutates the input dict
    seed = {"child": 1}
    _invocation_for("child", seed, replaying=False)
    assert seed == {"child": 1}
