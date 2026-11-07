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

"""run_until_done timer honesty: a capped (max_sleep) sleep must still wait for
the real wake_at deadline rather than firing after a single short nap (bug 7)."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Annotated

from agentflow import END, START, Graph, MemoryCheckpointer, State, last


class TimerState(State):
    done: Annotated[bool, last]
    runs: Annotated[int, last]


async def test_timer_wait_not_fired_early():
    counter = {"n": 0}

    def build():
        g = Graph(TimerState)

        async def waiter(state, ctx):
            counter["n"] += 1
            if not state.get("done"):
                wake = (datetime.now(UTC) + timedelta(seconds=0.05)).isoformat()
                await ctx.wait(wake)
                return {"done": True}
            return {"done": True}

        g.add_node("waiter", waiter)
        g.add_edge(START, "waiter")
        g.add_edge("waiter", END)
        return g.compile(checkpointer=MemoryCheckpointer())

    app = build()
    t0 = time.monotonic()
    # max_sleep caps each nap at 0.01s, but the 0.05s deadline must still hold.
    await app.run_until_done({"done": False, "runs": 0}, thread="t", max_sleep=0.01)
    elapsed = time.monotonic() - t0

    # datetime.now() has ~15 ms resolution on Windows, so allow that much slack.
    assert elapsed >= 0.03, f"timer fired early after {elapsed:.3f}s"
    # The node ran exactly twice: once to park, once after the real deadline.
    assert counter["n"] == 2
