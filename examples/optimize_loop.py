"""The iterate-until-converged prebuilt with a trivial in-memory worker.

Run: python examples/optimize_loop.py

Shows how the old baseline->iterate->stop loop is now just a compiled graph.
The worker here climbs toward a perfect score; swap it for one that prompts a
real backend (KiroBackend / OllamaBackend) and scores the result.
"""

from __future__ import annotations

import asyncio

from agentflow.prebuilt import Candidate, iterate_until_converged


async def main() -> None:
    # A fake "optimizer" that improves by 25 points a turn until it maxes out.
    async def work(state) -> Candidate:
        current_best = state.get("best_score", 0.0)
        score = min(current_best + 25.0, 100.0)
        return Candidate(score=score, payload={"guess": score})

    app = iterate_until_converged(work, perfect_score=100.0, patience=4, max_iterations=20)

    final = await app.invoke({})
    print("stop_reason:", final["stop_reason"])
    print("best_score: ", final["best_score"])
    print("iterations: ", final["iterations"])
    print("history:    ", final["history"])


if __name__ == "__main__":
    asyncio.run(main())
