"""Tests for the iterate-until-converged prebuilt — each stop condition."""

from __future__ import annotations

from agentflow.prebuilt import Candidate, iterate_until_converged


async def test_stops_on_optimal():
    async def work(state):
        best = state.get("best_score", 0.0)
        return Candidate(score=min(best + 25.0, 100.0))

    app = iterate_until_converged(work, perfect_score=100.0, patience=10, max_iterations=50)
    final = await app.invoke({})
    assert final["stop_reason"] == "optimal"
    assert final["best_score"] == 100.0
    assert final["iterations"] == 4  # 25,50,75,100


async def test_stops_on_done():
    async def work(state):
        return Candidate(score=42.0, done=True)

    app = iterate_until_converged(work)
    final = await app.invoke({})
    assert final["stop_reason"] == "done"
    assert final["best_score"] == 42.0
    assert final["iterations"] == 1


async def test_stops_on_converged():
    # Never improves after the first turn -> streak grows to patience.
    async def work(state):
        return Candidate(score=10.0)

    app = iterate_until_converged(work, perfect_score=100.0, patience=3, max_iterations=50)
    final = await app.invoke({})
    assert final["stop_reason"] == "converged"
    assert final["best_score"] == 10.0
    # first turn sets best (streak 0), then 3 non-improving turns
    assert final["iterations"] == 4


async def test_stops_on_exhausted():
    # Strictly increasing forever, but capped by max_iterations.
    async def work(state):
        return Candidate(score=state.get("last_score", 0.0) + 1.0)

    app = iterate_until_converged(work, perfect_score=1000.0, patience=100, max_iterations=5)
    final = await app.invoke({})
    assert final["stop_reason"] == "exhausted"
    assert final["iterations"] == 5


async def test_history_tracks_scores():
    async def work(state):
        best = state.get("best_score", 0.0)
        return Candidate(score=min(best + 50.0, 100.0))

    app = iterate_until_converged(work, perfect_score=100.0)
    final = await app.invoke({})
    assert final["history"] == [50.0, 100.0]
