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

"""Backend-independent scoring: a deterministic measure of a candidate.

An optimizer loop has two halves: a *proposer* that suggests candidates (a
heuristic, a local coding agent, a hosted LLM) and a *scorer* that measures
them. The whole design rests on the scorer being deterministic and independent
of who proposed the candidate — otherwise swapping the proposer (an agent
locally, an LLM in production) silently changes the measured result and an
improvement is indistinguishable from noise.

This module makes that measure a first-class, swappable contract. A
:class:`Scorer` maps a candidate to a :class:`Score`:

    async def score_plan(candidate, problem) -> Score:
        cost = await measure(candidate, problem)   # real work: run a query, EXPLAIN, tests
        return Score(value=-cost, metrics={"cost": cost})   # higher is better

    scored = await score_candidates(candidates, problem, score_plan)
    incumbent = best_of(scored, incumbent=incumbent)

Two conventions, applied everywhere, each removing a class of bug:

- Higher is better. One direction end to end means no min-vs-max confusion when
  comparing. A cost domain negates: ``value = -cost``.
- Feasibility is separate from value. A candidate can be invalid (a query
  rewrite that returns different rows, an index set over budget). An infeasible
  candidate is never promoted regardless of value — folding that into a very
  low value would let a feasible-but-worse candidate lose to an
  infeasible-but-better-looking one.

"Deterministic" means the same candidate always yields the same score — not
that scoring is cheap or does no I/O. Running a query or `EXPLAIN` is fine;
depending on wall-clock time as the primary metric is not (report it in
``metrics`` and use it only as a tie-break).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from agentflow.prebuilt.idempotent import artifact_key
from agentflow.store.base import Namespace, Store
from agentflow.store.memory import MemoryStore

__all__ = [
    "Score",
    "Scorer",
    "score_candidates",
    "best_of",
    "ScoreCache",
]

C = TypeVar("C")  # candidate type (consumer-defined)
P = TypeVar("P")  # problem type (consumer-defined)


@dataclass(frozen=True)
class Score:
    """A candidate's measured score. Higher ``value`` is better.

    ``feasible`` is the hard gate: an infeasible candidate is never promoted,
    whatever its ``value``. ``metrics`` carries the components behind the score
    for observability (and tie-breaks); ``reason`` explains an infeasible or
    notable result for logs and humans.
    """

    value: float
    feasible: bool = True
    metrics: dict[str, Any] = field(default_factory=dict)
    reason: str | None = None


# A scorer measures one candidate against the problem: async, deterministic
# (same candidate -> same Score), independent of who proposed the candidate.
Scorer = Callable[[C, P], Awaitable[Score]]


async def score_candidates(
    candidates: Iterable[C],
    problem: P,
    scorer: Scorer[C, P],
    *,
    max_concurrency: int | None = None,
) -> list[tuple[C, Score]]:
    """Score every candidate against ``problem``, returning ``[(candidate, Score)]``.

    Candidates are scored concurrently (bounded by ``max_concurrency`` when
    set, to cap pressure on a shared resource like a database). Order of the
    returned list matches the input order, so a deterministic scorer gives a
    deterministic result regardless of how the concurrency resolved.
    """
    items = list(candidates)
    if not items:
        return []

    sem = asyncio.Semaphore(max_concurrency) if max_concurrency else None

    async def _one(candidate: C) -> Score:
        if sem is None:
            return await scorer(candidate, problem)
        async with sem:
            return await scorer(candidate, problem)

    scores = await asyncio.gather(*(_one(c) for c in items))
    return list(zip(items, scores, strict=True))


def best_of(
    scored: Sequence[tuple[C, Score]],
    *,
    incumbent: tuple[C, Score] | None = None,
) -> tuple[C, Score] | None:
    """Pick the best *feasible* candidate, keeping ``incumbent`` on a tie or miss.

    Considers only feasible candidates. Returns the one with the highest
    ``value``; if ``incumbent`` is given, a challenger must *strictly beat* it
    to win (so the incumbent survives a tie — stable, no churn on equal scores).
    Returns the incumbent unchanged when nothing feasible beats it, or ``None``
    when there is no incumbent and nothing feasible.

    This is the promotion rule in one place: "ignore infeasible, keep the
    incumbent unless strictly beaten." Centralizing it means every optimizer
    promotes identically.
    """
    feasible = [(c, s) for c, s in scored if s.feasible]
    best = incumbent
    for cand, score in feasible:
        if best is None or score.value > best[1].value:
            best = (cand, score)
    return best


class ScoreCache(Generic[C, P]):
    """Wrap a :class:`Scorer` so an identical candidate is scored once.

    Real scoring is expensive (a query run, an `EXPLAIN`, a test suite) and the
    same candidate recurs — across rounds of a search and across a resume.
    This caches a :class:`Score` keyed by a content hash of the candidate
    (:func:`~agentflow.prebuilt.artifact_key`), in a :class:`~agentflow.store.
    Store`: a ``MemoryStore`` by default, or a ``PostgresStore`` for a durable,
    shared cache.

        cache = ScoreCache(score_plan, namespace=("scores", "query_opt"))
        scored = await score_candidates(candidates, problem, cache.scorer)

    The candidate must be JSON-serializable (it is hashed and the key is stored).
    Only cache a deterministic scorer — a cached score is reused forever (within
    the ttl), so caching a non-deterministic measure would serve a stale verdict.
    """

    def __init__(
        self,
        scorer: Scorer[C, P],
        *,
        namespace: Namespace,
        store: Store | None = None,
        ttl: float | None = None,
        key_from: Callable[[C], str] | None = None,
    ) -> None:
        self._scorer = scorer
        self._namespace = namespace
        self._store = store if store is not None else MemoryStore()
        self._ttl = ttl
        self._key_from = key_from or artifact_key

    async def scorer(self, candidate: C, problem: P) -> Score:
        """A drop-in :class:`Scorer` that serves a cached score when present."""
        key = self._key_from(candidate)
        hit = await self._store.get(self._namespace, key)
        if hit is not None:
            return _score_from_dict(hit.value)
        score = await self._scorer(candidate, problem)
        await self._store.put(self._namespace, key, _score_to_dict(score), ttl=self._ttl)
        return score


def _score_to_dict(score: Score) -> dict[str, Any]:
    return {
        "value": score.value,
        "feasible": score.feasible,
        "metrics": dict(score.metrics),
        "reason": score.reason,
    }


def _score_from_dict(d: Mapping[str, Any]) -> Score:
    return Score(
        value=d["value"],
        feasible=d.get("feasible", True),
        metrics=dict(d.get("metrics") or {}),
        reason=d.get("reason"),
    )
