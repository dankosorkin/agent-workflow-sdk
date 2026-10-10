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

"""Tests for the scoring harness: Score, score_candidates, best_of, ScoreCache."""

from __future__ import annotations

from agentflow.prebuilt import Score, ScoreCache, artifact_key, best_of, score_candidates
from agentflow.store import MemoryStore


async def _toy_scorer(candidate, problem):
    """Lower total is better; infeasible when longer than the budget."""
    cost = sum(candidate)
    feasible = len(candidate) <= problem["budget"]
    return Score(value=-float(cost), feasible=feasible, metrics={"cost": cost})


# --- score_candidates ------------------------------------------------------


async def test_scores_all_candidates_in_input_order():
    cands = [(1,), (3,), (2,)]
    scored = await score_candidates(cands, {"budget": 5}, _toy_scorer)
    assert [c for c, _ in scored] == cands  # order preserved
    assert [s.metrics["cost"] for _, s in scored] == [1, 3, 2]


async def test_empty_candidates_returns_empty():
    assert await score_candidates([], {"budget": 5}, _toy_scorer) == []


async def test_max_concurrency_bounds_but_preserves_order():
    cands = [(i,) for i in range(10)]
    scored = await score_candidates(cands, {"budget": 5}, _toy_scorer, max_concurrency=3)
    assert [c for c, _ in scored] == cands


async def test_marks_infeasible():
    cands = [(1, 2), (1, 2, 3)]
    scored = await score_candidates(cands, {"budget": 2}, _toy_scorer)
    assert scored[0][1].feasible is True
    assert scored[1][1].feasible is False


# --- best_of ---------------------------------------------------------------


async def test_best_of_picks_highest_feasible():
    scored = await score_candidates([(1,), (5,), (2,)], {"budget": 5}, _toy_scorer)
    best = best_of(scored)
    assert best[0] == (1,)  # cost 1 -> value -1, the highest value


async def test_best_of_ignores_infeasible():
    # a one-element candidate is feasible under budget 1; a two-element one is
    # not. The 2-element (1,1) has the lower cost but is infeasible, so the
    # feasible (5,) must win despite its higher cost.
    scored = await score_candidates([(1, 1), (5,)], {"budget": 1}, _toy_scorer)
    best = best_of(scored)
    assert best[0] == (5,)  # the only feasible candidate wins


async def test_best_of_keeps_incumbent_on_tie():
    scored = await score_candidates([(1,)], {"budget": 5}, _toy_scorer)  # value -1
    incumbent = ((9,), Score(value=-1.0))  # same value
    best = best_of(scored, incumbent=incumbent)
    assert best[0] == (9,)  # incumbent survives a tie (strict-beat required)


async def test_best_of_challenger_strictly_beats_incumbent():
    scored = await score_candidates([(1,)], {"budget": 5}, _toy_scorer)  # value -1
    incumbent = ((9,), Score(value=-5.0))  # worse
    best = best_of(scored, incumbent=incumbent)
    assert best[0] == (1,)  # challenger beats a worse incumbent


async def test_best_of_returns_incumbent_when_nothing_feasible():
    scored = await score_candidates([(1, 2, 3)], {"budget": 1}, _toy_scorer)  # infeasible
    incumbent = ((9,), Score(value=-5.0))
    assert best_of(scored, incumbent=incumbent) == incumbent


async def test_best_of_empty_is_none():
    assert best_of([]) is None


async def test_best_of_all_infeasible_no_incumbent_is_none():
    scored = await score_candidates([(1, 1), (2, 2)], {"budget": 1}, _toy_scorer)
    assert all(not s.feasible for _, s in scored)
    assert best_of(scored) is None


# --- ScoreCache ------------------------------------------------------------


async def test_cache_scores_each_candidate_once():
    calls: list = []

    async def counting(candidate, problem):
        calls.append(candidate)
        return Score(value=-float(sum(candidate)))

    cache = ScoreCache(counting, namespace=("scores", "test"))
    p = {"budget": 5}
    await cache.scorer([1, 2], p)
    await cache.scorer([1, 2], p)  # identical -> served from cache
    await cache.scorer([3], p)
    assert calls == [[1, 2], [3]]  # [1,2] scored once, not twice


async def test_cache_round_trips_score_fields():
    async def scorer(candidate, problem):
        return Score(value=-1.0, feasible=False, metrics={"a": 1}, reason="nope")

    cache = ScoreCache(scorer, namespace=("scores", "rt"))
    first = await cache.scorer([1], {})
    second = await cache.scorer([1], {})  # from cache
    assert second.value == first.value == -1.0
    assert second.feasible is False
    assert second.metrics == {"a": 1}
    assert second.reason == "nope"


async def test_cache_accepts_injected_store():
    store = MemoryStore()

    async def scorer(candidate, problem):
        return Score(value=0.0)

    cache = ScoreCache(scorer, namespace=("scores", "inj"), store=store)
    await cache.scorer([1], {})
    # the score landed in the injected store
    hit = await store.get(("scores", "inj"), artifact_key([1]))
    assert hit is not None
