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

"""Watch an external system until it finishes, parking between polls.

Run: python examples/watch_until_done.py

Models the publish-and-respond shape: a loop watches something outside the
graph (here a stand-in "job" a shell script reports on), responds to each piece
of activity, and exits when the thing reaches a terminal state. Between polls
the run *parks* on ``ctx.wait`` — no work happens while nothing changes.

The watcher is a CommandWatcher over a tiny script that reports "running" a few
times, then "done" — a stand-in for polling a PR, a CI build, or a deployment.
Swap the command for ``gh pr view ... --json state`` (or any program that
prints the contract JSON) and the same loop waits on a real pull request.

Everything is offline. This drives the loop inline with run_until_done, which
keeps the process alive across the waits; to park with zero cost across a
restart, drive the same graph through the control plane instead (a parked run
is WAITING in the queue and is re-claimed when its wake time passes).
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from typing import Annotated

from agentflow import END, START, Graph, MemoryCheckpointer, State, append, last
from agentflow.prebuilt import CommandWatcher, route_watch, watch_node

# A stand-in external system: a script that counts its own invocations via a
# file and reports a JSON status. First calls say "running" (idle), then it
# flips to "done" (terminal). This is exactly the shape a real poll script
# would print — outcome/payload/cursor — so CommandWatcher maps it directly.
POLL_SCRIPT = r"""
count_file="$1"
n=$(cat "$count_file" 2>/dev/null || echo 0)
n=$((n + 1))
echo "$n" > "$count_file"
if [ "$n" -lt 3 ]; then
  printf '{"outcome":"idle","cursor":{"polls":%s}}' "$n"
else
  printf '{"outcome":"terminal","payload":"job %s finished","cursor":{"polls":%s}}' "$n" "$n"
fi
"""


class WatchState(State):
    watch_result: Annotated[object, last]
    watch_cursor: Annotated[object, last]
    log: Annotated[list, append]


async def main() -> None:
    workdir = tempfile.mkdtemp()
    script = os.path.join(workdir, "poll.sh")
    count = os.path.join(workdir, "count")
    with open(script, "w") as fh:
        fh.write(POLL_SCRIPT)

    watcher = CommandWatcher(f"sh {script} {count}")

    async def report(state, ctx):
        payload = state["watch_result"]["payload"]
        print(f"  terminal: {payload}")
        return {"log": payload}

    g = Graph(WatchState)
    g.add_node("wait", watch_node(watcher, poll_interval=0.2))
    g.add_node("report", report)
    g.add_edge(START, "wait")
    # This watcher only ever reports idle or terminal, so route terminal to a
    # final report; an activity branch would loop back to "wait" to keep going.
    g.add_conditional_edges("wait", route_watch, {"activity": "wait", "terminal": "report"})
    g.add_edge("report", END)

    app = g.compile(checkpointer=MemoryCheckpointer())

    print("watching (parks between polls)...")
    out = await app.run_until_done({"log": []}, thread="job-1", max_sleep=0.2)
    print(f"done. log={out['log']}")
    with open(count) as fh:
        print(f"the script was polled {fh.read().strip()} times")


if __name__ == "__main__":
    asyncio.run(main())
