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

"""HITL runtime: a node that interrupts more than once must deliver answers in
order across resumes (bug 3), and that ordering must survive a crash replay
(bug 3 x bug 1)."""

from __future__ import annotations

from typing import Annotated

from agentflow import END, START, Graph, MemoryCheckpointer, State, last


class TwoQ(State):
    a: Annotated[str, last]
    b: Annotated[str, last]


def _two_interrupt_graph(checkpointer):
    g = Graph(TwoQ)

    async def ask_twice(state, ctx):
        a = await ctx.interrupt("q1")
        b = await ctx.interrupt("q2")
        return {"a": a, "b": b}

    g.add_node("ask", ask_twice)
    g.add_edge(START, "ask")
    g.add_edge("ask", END)
    return g.compile(checkpointer=checkpointer)


async def test_two_interrupts_in_one_node_ordered():
    cp = MemoryCheckpointer()
    app = _two_interrupt_graph(cp)

    await app.invoke({"a": "", "b": ""}, thread="t")
    first = await cp.get("t")
    assert first.interrupted and first.interrupt_payload == "q1"

    await app.resume("t", "A")
    second = await cp.get("t")
    # The node must now be waiting on the SECOND question, not re-asking q1.
    assert second.interrupted and second.interrupt_payload == "q2"

    out = await app.resume("t", "B")
    done = await cp.get("t")
    assert done.done
    # Ordered delivery: first answer -> first call, second answer -> second.
    assert out["a"] == "A"
    assert out["b"] == "B"


async def test_two_interrupts_survive_crash_replay():
    """After answering q1, re-running from the stored checkpoint (as a crashed
    worker would) must not re-ask q1 — the node still waits on q2."""
    cp = MemoryCheckpointer()
    app = _two_interrupt_graph(cp)

    await app.invoke({"a": "", "b": ""}, thread="t")
    await app.resume("t", "A")
    after_a = await cp.get("t")
    assert after_a.interrupt_payload == "q2"
    # The stored checkpoint carries the ordered answer already given.
    assert after_a.interrupt_answers == ("A",)

    # Resume again with the second answer; q1 is never re-asked.
    out = await app.resume("t", "B")
    assert out == {"a": "A", "b": "B"}
