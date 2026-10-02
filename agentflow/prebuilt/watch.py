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

"""Watch an external system without burning turns while nothing changes.

A *watch* node polls something outside the graph — a pull request, a build, a
deployment, a ticket, anything a command can check — and hands control back to
the graph only when there is activity or a terminal state. Between polls it
*parks*: the node calls ``ctx.wait(wake_at)``, which suspends the run until the
time passes. Under the control plane that means the worker is released and the
run costs nothing while parked (it is re-claimed when ``wake_at`` arrives);
inline, :meth:`CompiledGraph.run_until_done` sleeps until then.

A watcher reports one of three outcomes each poll:

- idle — nothing changed; park and poll again later.
- activity — something happened; return it to the graph to react to.
- terminal — the thing reached a final state; the loop should exit.

Build a watch loop from the primitives: a ``watch_node`` that parks on idle and
writes activity/terminal to state, then a conditional edge that routes activity
to a responder (and back to the watch) and terminal to the exit.

    g.add_node("wait", watch_node(CommandWatcher("./poll.sh"), poll_interval=60))
    g.add_node("respond", respond)   # reads state["watch_result"]["payload"]
    g.add_conditional_edges("wait", route_watch, {"activity": "respond", "terminal": END})
    g.add_edge("respond", "wait")

## Honest limits

Polling is not push: "no model turns while parked" is not "no latency" — an
event is seen at the next poll, so ``poll_interval`` trades freshness for cost.
Wake granularity is also bounded by how often the control-plane workers claim
(their own poll interval). And the cursor a watcher returns is persisted on the
wait checkpoint, so it survives the park, but a watcher must be able to resume
from a cursor rather than assuming it kept in-memory state across polls.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "WatchResult",
    "Watcher",
    "CommandWatcher",
    "watch_node",
    "route_watch",
]


@dataclass(frozen=True)
class WatchResult:
    """One poll's outcome.

    ``outcome`` is ``"idle"``, ``"activity"``, or ``"terminal"``. ``payload`` is
    the event data for activity/terminal (what a responder reacts to).
    ``cursor`` is opaque state handed back to the next poll so the watcher can
    resume where it left off; it is persisted across the park.
    """

    outcome: str  # "idle" | "activity" | "terminal"
    payload: Any = None
    cursor: Any = None

    @classmethod
    def idle(cls, cursor: Any = None) -> WatchResult:
        return cls("idle", cursor=cursor)

    @classmethod
    def activity(cls, payload: Any, cursor: Any = None) -> WatchResult:
        return cls("activity", payload=payload, cursor=cursor)

    @classmethod
    def terminal(cls, payload: Any = None) -> WatchResult:
        return cls("terminal", payload=payload)


@runtime_checkable
class Watcher(Protocol):
    """Polls an external system. Must not block the loop for long.

    ``poll`` is called once per visit with the previous poll's cursor (``None``
    the first time) and returns a :class:`WatchResult`. It should return
    promptly — the parking between polls is the loop's job, not the watcher's.
    """

    async def poll(self, cursor: Any) -> WatchResult: ...


class CommandWatcher:
    """Watch anything a command can check: run it, parse one JSON object.

    The command prints a single JSON object to stdout describing the current
    state; this watcher maps it to a :class:`WatchResult`. The shape mirrors a
    minimal, tool-agnostic contract::

        {"outcome": "idle" | "activity" | "terminal",
         "payload": <any, optional>,
         "cursor": <any, optional>}

    The previous cursor is passed to the command as a ``AGENTFLOW_CURSOR`` env
    var (JSON) so a stateless script can resume. A non-zero exit is treated as
    an idle poll by default (transient failure); set ``strict=True`` to raise.
    GitHub, CI, a ticket system — all are a ``CommandWatcher`` over your script,
    which is why no provider specifics live here.
    """

    def __init__(
        self,
        command: str,
        *,
        cwd: str | None = None,
        timeout: float = 30.0,
        strict: bool = False,
    ) -> None:
        self.command = command
        self.cwd = cwd
        self.timeout = timeout
        self.strict = strict

    async def poll(self, cursor: Any) -> WatchResult:
        import os

        env = dict(os.environ)
        env["AGENTFLOW_CURSOR"] = json.dumps(cursor)
        proc = await asyncio.create_subprocess_shell(
            self.command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=self.cwd,
            env=env,
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout=self.timeout)
        except TimeoutError:
            proc.kill()
            if self.strict:
                raise
            return WatchResult.idle(cursor)

        if proc.returncode != 0:
            if self.strict:
                raise RuntimeError(
                    f"watch command failed ({proc.returncode}): {err.decode('utf-8', 'replace')}"
                )
            return WatchResult.idle(cursor)

        try:
            data = json.loads(out.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as exc:
            if self.strict:
                raise RuntimeError(f"watch command did not print valid JSON: {exc}") from exc
            return WatchResult.idle(cursor)

        return WatchResult(
            outcome=data.get("outcome", "idle"),
            payload=data.get("payload"),
            cursor=data.get("cursor", cursor),
        )


# State channel names the watch node reads/writes. Kept simple and documented
# so a graph's state schema can declare them.
_WATCH_RESULT = "watch_result"
_WATCH_CURSOR = "watch_cursor"


def watch_node(
    watcher: Watcher | Callable[[Any], Awaitable[WatchResult]],
    *,
    poll_interval: float = 60.0,
) -> Callable[[Any, Any], Awaitable[dict[str, Any]]]:
    """Build a node that polls ``watcher`` and parks on idle.

    On an idle poll the node calls ``ctx.wait(now + poll_interval, cursor)`` to
    suspend the run until the next poll, carrying the watcher's cursor across
    the park. On activity or terminal it returns an update writing the result
    (and cursor) to state, where :func:`route_watch` can branch on it.

    The watcher may be a :class:`Watcher` or a bare ``async (cursor) ->
    WatchResult`` callable. The node reads/writes two channels, ``watch_result``
    and ``watch_cursor``; declare them on your state (both ``last``-reduced).
    """
    poll = watcher.poll if isinstance(watcher, Watcher) else watcher

    async def node(state: Any, ctx: Any) -> dict[str, Any]:
        cursor = state.get(_WATCH_CURSOR)
        # Poll until something happens, parking between idle polls. ctx.wait
        # suspends the run; on resume it returns here and the loop polls again.
        # The node only returns (to the router) on activity or terminal, so the
        # router always sees a fresh, non-idle result.
        while True:
            result = await poll(cursor)
            cursor = result.cursor
            if result.outcome != "idle":
                # Write a plain dict, not the WatchResult dataclass: state must
                # be JSON-serializable to survive a durable checkpointer. The
                # payload is the caller's and must itself be JSON-safe.
                return {
                    _WATCH_RESULT: {"outcome": result.outcome, "payload": result.payload},
                    _WATCH_CURSOR: cursor,
                }
            wake_at = (datetime.now(UTC) + timedelta(seconds=poll_interval)).isoformat()
            await ctx.wait(wake_at, payload={"cursor": cursor})

    node.__name__ = "watch_node"
    return node


def route_watch(state: Any) -> str:
    """Route out of a watch node: ``"activity"`` or ``"terminal"``.

    Reads the ``watch_result`` channel the node wrote — a dict
    ``{"outcome", "payload"}``. Map these keys to your responder and your exit,
    e.g. ``{"activity": "respond", "terminal": END}``. A responder reads the
    event via ``state["watch_result"]["payload"]``.
    """
    result = state.get(_WATCH_RESULT)
    if result is not None and result.get("outcome") == "terminal":
        return "terminal"
    return "activity"
