"""Measure Kiro start-up cost: a new backend per step vs one reused process.

    python bench/kiro_pool/setup_sandbox.py
    python bench/kiro_pool/run_bench.py --steps 6 --mode both

Scenarios (the same cycle of agents in each):
  fresh   a new KiroBackend per step (what the engine does today)
  shared  one KiroBackend; for each step set ``backend.agent`` and call
          ``reset_session()`` (v3 selects the agent as a session mode)

Every step asks for the single word "pong" with no tools, so cost is dominated
by start-up. Phases timed per step: start (process + initialize + session/new
+ set_mode), first token, turn end. MCP spawns are counted from the sandbox
``spawns.log`` written by fake_mcp.py (fake profile only).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
from pathlib import Path

from agentflow.backends.base import AllowAll
from agentflow.backends.kiro import KiroBackend
from agentflow.events import ErrorEvent, TextChunk, TurnEnd

HERE = Path(__file__).resolve().parent
PROMPT = "Reply with exactly the word: pong. Do not use any tools."
AGENTS = ["bench-light", "bench-medium", "bench-heavy"]


async def _turn(backend: KiroBackend) -> tuple[float | None, float, str]:
    t0 = time.perf_counter()
    first: float | None = None
    err = ""
    async for ev in backend.prompt(PROMPT):
        if isinstance(ev, TextChunk) and first is None:
            first = time.perf_counter() - t0
        elif isinstance(ev, ErrorEvent):
            err = str(ev)
        elif isinstance(ev, TurnEnd):
            break
    return first, time.perf_counter() - t0, err


def _spawns(log: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    if log.exists():
        for line in log.read_text(encoding="utf-8").splitlines():
            counts[line.split("\t")[0]] = counts.get(line.split("\t")[0], 0) + 1
    return counts


async def run(mode: str, steps: int, sandbox: Path, engine: str) -> dict:
    log = sandbox / "spawns.log"
    log.write_text("", encoding="utf-8")
    rows: list[dict] = []
    backend: KiroBackend | None = None
    wall0 = time.perf_counter()
    try:
        for i in range(steps):
            agent = AGENTS[i % len(AGENTS)]
            t0 = time.perf_counter()
            if mode == "fresh":
                backend = KiroBackend(agent, engine=engine, cwd=sandbox, permission=AllowAll())
                await backend.start()
            elif backend is None:
                backend = KiroBackend(agent, engine=engine, cwd=sandbox, permission=AllowAll())
                await backend.start()
            else:
                backend.agent = agent
                await backend.reset_session()
            start = time.perf_counter() - t0
            first, turn, err = await _turn(backend)
            t1 = time.perf_counter()
            if mode == "fresh":
                await backend.close()
            close = time.perf_counter() - t1
            rows.append({"step": i + 1, "agent": agent, "start": start, "first_token": first,
                         "turn": turn, "close": close, "error": err})
            print(f"  [{mode}] step {i + 1} {agent:13s} start={start:5.1f}s first={first or 0:5.1f}s "
                  f"turn={turn:5.1f}s {('ERR ' + err) if err else ''}", flush=True)
    finally:
        if mode == "shared" and backend is not None:
            await backend.close()
    return {"mode": mode, "wall": time.perf_counter() - wall0, "rows": rows, "spawns": _spawns(log)}


def _summary(res: dict) -> str:
    starts = [r["start"] for r in res["rows"]]
    return (f"{res['mode']:7s} wall={res['wall']:6.1f}s  start: first={starts[0]:.1f}s "
            f"median-after={statistics.median(starts[1:] or starts):.1f}s  mcp spawns={res['spawns']}")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["fresh", "shared", "both"], default="both")
    ap.add_argument("--steps", type=int, default=6)
    ap.add_argument("--engine", default="v3")
    ap.add_argument("--sandbox", type=Path, default=HERE / ".sandbox")
    args = ap.parse_args()
    modes = ["fresh", "shared"] if args.mode == "both" else [args.mode]
    results = []
    for m in modes:
        print(f"== {m}")
        results.append(await run(m, args.steps, args.sandbox.resolve(), args.engine))
    print()
    for r in results:
        print(_summary(r))
    if len(results) == 2:
        a, b = results
        print(f"speedup (wall): {a['wall'] / b['wall']:.2f}x")
    out = HERE / "results"
    out.mkdir(exist_ok=True)
    (out / f"{int(time.time())}.json").write_text(json.dumps(results, indent=2), encoding="utf-8")


if __name__ == "__main__":
    asyncio.run(main())
