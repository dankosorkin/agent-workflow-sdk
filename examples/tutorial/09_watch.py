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

"""Optimizer tutorial, chapter 9: waiting on a benchmark.

Run: python examples/tutorial/09_watch.py

The modeled cost says a plan is better, but we want to confirm it with a real
benchmark before trusting it. A benchmark takes time, and we do not want to
burn a worker spinning while it runs. So the optimizer *parks* on a watch node:
it kicks off a benchmark, polls for the result, and suspends between polls via
ctx.wait. When the benchmark reports terminal, the run wakes, reads the result,
and promotes. Driven inline here with run_until_done; under the control plane
the same park would release the worker entirely (chapter 10).
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from typing import Annotated

from agentflow import END, START, Graph, MemoryCheckpointer, State, last
from agentflow.prebuilt import CommandWatcher, route_watch, watch_node

BASE_COST = 100.0
STRATEGY_FACTORS = {"add_index": 0.4, "push_filter": 0.6, "reorder_stages": 0.9}


def cost_of(plan) -> float:
    cost = BASE_COST
    for strategy in plan:
        cost *= STRATEGY_FACTORS.get(strategy, 1.0)
    return round(cost, 2)


# A stand-in benchmark: a script that reports "running" a couple of times, then
# "done" with the measured cost. This is exactly the contract CommandWatcher
# expects — outcome/payload/cursor as one JSON object — so a real `gh run view`
# or a CI poll would drop straight in.
BENCH_SCRIPT = r"""
count_file="$1"
measured="$2"
n=$(cat "$count_file" 2>/dev/null || echo 0)
n=$((n + 1))
echo "$n" > "$count_file"
if [ "$n" -lt 3 ]; then
  printf '{"outcome":"idle","cursor":{"polls":%s}}' "$n"
else
  printf '{"outcome":"terminal","payload":{"measured_cost":%s},"cursor":{"polls":%s}}' "$measured" "$n"
fi
"""


class OptState(State):
    plan: Annotated[tuple, last]
    modeled_cost: Annotated[float, last]
    measured_cost: Annotated[float, last]
    watch_result: Annotated[object, last]
    watch_cursor: Annotated[object, last]


def build(script: str, count: str, measured: float) -> Graph:
    watcher = CommandWatcher(f"sh {script} {count} {measured}")

    async def start_benchmark(state: OptState, ctx) -> dict:
        print(f"  kicking off benchmark for plan {list(state['plan'])}")
        return {}

    async def promote(state: OptState, ctx) -> dict:
        measured = state["watch_result"]["payload"]["measured_cost"]
        print(f"  benchmark done: measured cost {measured} (modeled {state['modeled_cost']})")
        return {"measured_cost": measured}

    g = Graph(OptState)
    g.add_node("start", start_benchmark)
    g.add_node("wait", watch_node(watcher, poll_interval=0.2))
    g.add_node("promote", promote)
    g.add_edge(START, "start")
    g.add_edge("start", "wait")
    # the benchmark only ever reports idle or terminal; terminal -> promote
    g.add_conditional_edges("wait", route_watch, {"activity": "wait", "terminal": "promote"})
    g.add_edge("promote", END)
    return g


async def main() -> None:
    workdir = tempfile.mkdtemp()
    script = os.path.join(workdir, "bench.sh")
    count = os.path.join(workdir, "count")
    with open(script, "w") as fh:
        fh.write(BENCH_SCRIPT)

    plan = ("add_index", "push_filter")
    modeled = cost_of(plan)
    app = build(script, count, measured=25.0).compile(checkpointer=MemoryCheckpointer())

    print("optimizing with a benchmark gate (parks between polls):")
    out = await app.run_until_done(
        {"plan": plan, "modeled_cost": modeled}, thread="bench-1", max_sleep=0.2
    )
    print(
        f"final: plan {list(out['plan'])} modeled {out['modeled_cost']} measured {out['measured_cost']}"
    )
    with open(count) as fh:
        print(f"the benchmark was polled {fh.read().strip()} times")


if __name__ == "__main__":
    asyncio.run(main())
