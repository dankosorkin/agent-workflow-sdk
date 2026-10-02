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

"""Core watch / ctx.wait tests: timer suspend, inline driver, the watch loop."""

from __future__ import annotations

from typing import Annotated

import pytest
from agentflow import (
    END,
    START,
    Graph,
    MemoryCheckpointer,
    State,
    append,
    last,
)
from agentflow.prebuilt import WatchResult, route_watch, watch_node


class WatchState(State):
    watch_result: Annotated[object, last]
    watch_cursor: Annotated[object, last]
    responses: Annotated[list, append]


# --- ctx.wait at the engine level ---


async def test_ctx_wait_suspends_as_timer():
    class S(State):
        done: Annotated[bool, last]

    async def waiter(state, ctx):
        await ctx.wait("2099-01-01T00:00:00+00:00", payload={"c": 1})
        return {"done": True}

    g = Graph(S)
    g.add_node("w", waiter)
    g.add_edge(START, "w")
    g.add_edge("w", END)
    app = g.compile(checkpointer=MemoryCheckpointer())

    state = await app.invoke({}, thread="t")
    cp = await app.get_state("t")
    assert cp.interrupted is True
    assert cp.suspend_kind == "timer"
    assert cp.wake_at == "2099-01-01T00:00:00+00:00"
    assert not cp.done
    assert not state.get("done")  # node did not complete; it parked


async def test_human_interrupt_is_not_a_timer():
    class S(State):
        answer: Annotated[str, last]

    async def ask(state, ctx):
        a = await ctx.interrupt({"q": "ok?"})
        return {"answer": a}

    g = Graph(S)
    g.add_node("ask", ask)
    g.add_edge(START, "ask")
    g.add_edge("ask", END)
    app = g.compile(checkpointer=MemoryCheckpointer())

    await app.invoke({}, thread="t")
    cp = await app.get_state("t")
    assert cp.suspend_kind == "human"
    assert cp.wake_at is None


async def test_resume_continues_a_timer_wait():
    class S(State):
        n: Annotated[int, last]

    async def once(state, ctx):
        await ctx.wait("2099-01-01T00:00:00+00:00")
        return {"n": 42}

    g = Graph(S)
    g.add_node("once", once)
    g.add_edge(START, "once")
    g.add_edge("once", END)
    app = g.compile(checkpointer=MemoryCheckpointer())

    await app.invoke({}, thread="t")
    assert (await app.get_state("t")).suspend_kind == "timer"
    out = await app.resume("t")  # clock "fired"
    assert out["n"] == 42
    assert (await app.get_state("t")).done


# --- the watch loop via run_until_done ---


def _seq_watcher(results):
    it = iter(results)
    calls = {"n": 0}

    async def poll(cursor):
        calls["n"] += 1
        return next(it)

    return poll, calls


async def test_watch_loop_parks_then_reacts_then_exits():
    poll, calls = _seq_watcher(
        [
            WatchResult.idle(cursor={"n": 1}),
            WatchResult.idle(cursor={"n": 2}),
            WatchResult.activity(payload="comment", cursor={"n": 3}),
            WatchResult.terminal(payload="merged"),
        ]
    )

    async def respond(state, ctx):
        return {"responses": state["watch_result"].payload}

    g = Graph(WatchState)
    g.add_node("wait", watch_node(poll, poll_interval=0.02))
    g.add_node("respond", respond)
    g.add_edge(START, "wait")
    g.add_conditional_edges("wait", route_watch, {"activity": "respond", "terminal": END})
    g.add_edge("respond", "wait")
    app = g.compile(checkpointer=MemoryCheckpointer())

    out = await app.run_until_done({"responses": []}, thread="w", max_sleep=0.02)
    assert calls["n"] == 4
    assert out["responses"] == ["comment"]
    assert (await app.get_state("w")).done


async def test_watch_node_requires_no_idle_to_route():
    # First poll is terminal — the loop should return immediately, no park.
    poll, calls = _seq_watcher([WatchResult.terminal(payload="done")])

    g = Graph(WatchState)
    g.add_node("wait", watch_node(poll, poll_interval=0.02))
    g.add_edge(START, "wait")
    g.add_conditional_edges("wait", route_watch, {"activity": END, "terminal": END})
    app = g.compile(checkpointer=MemoryCheckpointer())

    out = await app.run_until_done({"responses": []}, thread="w", max_sleep=0.02)
    assert calls["n"] == 1
    assert out["watch_result"].outcome == "terminal"


async def test_run_until_done_requires_checkpointer():
    from agentflow.errors import CheckpointError

    async def once(state, ctx):
        return {}

    g = Graph(WatchState)
    g.add_node("n", once)
    g.add_edge(START, "n")
    g.add_edge("n", END)
    app = g.compile()  # no checkpointer
    with pytest.raises(CheckpointError):
        await app.run_until_done({"responses": []})


# --- WatchResult / CommandWatcher basics ---


def test_watch_result_constructors():
    assert WatchResult.idle(cursor=1).outcome == "idle"
    a = WatchResult.activity("p", cursor=2)
    assert a.outcome == "activity" and a.payload == "p" and a.cursor == 2
    assert WatchResult.terminal("done").outcome == "terminal"


async def test_command_watcher_parses_json(tmp_path):
    from agentflow.prebuilt import CommandWatcher

    # A script that emits one activity result as JSON.
    w = CommandWatcher("""printf '{"outcome":"activity","payload":"hi","cursor":{"seen":1}}'""")
    result = await w.poll(None)
    assert result.outcome == "activity"
    assert result.payload == "hi"
    assert result.cursor == {"seen": 1}


async def test_command_watcher_nonzero_exit_is_idle_by_default():
    from agentflow.prebuilt import CommandWatcher

    w = CommandWatcher("exit 3")
    result = await w.poll({"keep": 1})
    assert result.outcome == "idle"
    assert result.cursor == {"keep": 1}  # cursor preserved across a failed poll


async def test_command_watcher_strict_raises_on_failure():
    from agentflow.prebuilt import CommandWatcher

    w = CommandWatcher("exit 3", strict=True)
    with pytest.raises(RuntimeError):
        await w.poll(None)
