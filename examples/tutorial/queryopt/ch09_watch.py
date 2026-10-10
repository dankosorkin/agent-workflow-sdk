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

"""Query-optimizer tutorial, chapter 9: waiting on a benchmark.

Run: python -m examples.tutorial.queryopt.ch09_watch

The EXPLAIN-based score is cheap and deterministic — a great in-loop guide. But
before applying an index set to production, you want to confirm it on a full
benchmark against realistic data volumes, which takes minutes. The optimizer
kicks off that benchmark and parks on a watch node until it reports back,
instead of holding a worker busy while it runs.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from typing import Annotated

from agentflow import END, START, Graph, MemoryCheckpointer, State, last
from agentflow.prebuilt import CommandWatcher, route_watch, watch_node

# A stand-in benchmark: a script reporting "running" a couple of times then a
# "done" with a measured latency. A real one would run the query against a
# large dataset and print the same contract JSON.
BENCH_SCRIPT = r"""
count_file="$1"
latency="$2"
n=$(cat "$count_file" 2>/dev/null || echo 0)
n=$((n + 1))
echo "$n" > "$count_file"
if [ "$n" -lt 3 ]; then
  printf '{"outcome":"idle","cursor":{"polls":%s}}' "$n"
else
  printf '{"outcome":"terminal","payload":{"latency_ms":%s},"cursor":{"polls":%s}}' "$latency" "$n"
fi
"""


class OptState(State):
    indexes: Annotated[tuple, last]
    benchmark_ms: Annotated[float, last]
    watch_result: Annotated[object, last]
    watch_cursor: Annotated[object, last]


def build(script: str, count: str, latency: float) -> Graph:
    watcher = CommandWatcher(f"sh {script} {count} {latency}")

    async def start_benchmark(state: OptState, ctx) -> dict:
        print(f"  submitted benchmark for {len(state['indexes'])} indexes")
        return {}

    async def record(state: OptState, ctx) -> dict:
        ms = state["watch_result"]["payload"]["latency_ms"]
        print(f"  benchmark reported {ms} ms")
        return {"benchmark_ms": ms}

    g = Graph(OptState)
    g.add_node("start", start_benchmark)
    g.add_node("wait", watch_node(watcher, poll_interval=0.2))
    g.add_node("record", record)
    g.add_edge(START, "start")
    g.add_edge("start", "wait")
    g.add_conditional_edges("wait", route_watch, {"activity": "wait", "terminal": "record"})
    g.add_edge("record", END)
    return g


async def main() -> None:
    workdir = tempfile.mkdtemp()
    script = os.path.join(workdir, "bench.sh")
    count = os.path.join(workdir, "count")
    with open(script, "w") as fh:
        fh.write(BENCH_SCRIPT)

    chosen = ("CREATE INDEX i_o_date ON orders(order_date)",)
    app = build(script, count, latency=42.0).compile(checkpointer=MemoryCheckpointer())
    print("confirming the chosen plan on a benchmark (parks between polls):")
    out = await app.run_until_done({"indexes": chosen}, thread="bench-1", max_sleep=0.2)
    print(f"\nconfirmed: {len(out['indexes'])} indexes, benchmark {out['benchmark_ms']} ms")
    with open(count) as fh:
        print(f"the benchmark was polled {fh.read().strip()} times")


if __name__ == "__main__":
    asyncio.run(main())
