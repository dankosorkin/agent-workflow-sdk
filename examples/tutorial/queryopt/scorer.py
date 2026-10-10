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

"""The measure: a deterministic SQLite Scorer for a candidate.

A candidate is a set of indexes plus an optional query rewrite. Scoring it:

1. If a rewrite is present, run both the original and the rewrite and require
   identical result sets — otherwise the candidate is infeasible (a rewrite
   that changes results is not a valid optimization, however fast).
2. Apply the candidate's indexes to a scratch copy of the database.
3. Read ``EXPLAIN QUERY PLAN`` and derive a plan cost: full table scans are
   expensive, index searches are cheap. This is the deterministic metric — it
   does not depend on machine speed.
4. value = -(plan_cost + index_penalty * number_of_indexes): the trade-off
   that keeps the optimizer from just adding every index.

Wall-clock time is measured too, but only reported in metrics as a tie-break;
the primary score is the plan cost, which is reproducible.
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass, field

from agentflow.prebuilt import Score
from examples.tutorial.queryopt.db import QUERY, build_db

__all__ = ["Candidate", "Problem", "make_scorer", "plan_cost", "explain"]


@dataclass(frozen=True)
class Candidate:
    """A proposed optimization: indexes to create and an optional rewrite."""

    indexes: tuple[str, ...] = ()
    rewrite: str | None = None


@dataclass(frozen=True)
class Problem:
    """The optimization problem: the query, the data seed, and the budget."""

    query: str = QUERY
    seed: int = 1234
    index_penalty: float = 2.0  # cost added per index (the trade-off knob)
    sizes: dict = field(default_factory=dict)


# Cost weights for plan steps, from EXPLAIN QUERY PLAN detail text. A full scan
# is the thing we want to eliminate; an index search is cheap.
_SCAN_COST = 10.0
_SEARCH_COST = 1.0


def explain(conn: sqlite3.Connection, query: str) -> list[str]:
    """Return the EXPLAIN QUERY PLAN detail lines for ``query``."""
    rows = conn.execute(f"EXPLAIN QUERY PLAN {query}").fetchall()
    # each row: (id, parent, notused, detail)
    return [r[3] for r in rows]


def plan_cost(detail_lines: list[str]) -> float:
    """A deterministic cost from the plan: scans are dear, searches are cheap.

    SQLite's plan prints "SCAN <table>" for a full scan and "SEARCH <table>
    USING INDEX ..." when it uses an index. Counting those, weighted, gives a
    stable proxy for how much work the plan does without timing anything.
    """
    cost = 0.0
    for line in detail_lines:
        if "SCAN" in line:
            cost += _SCAN_COST
        elif "SEARCH" in line:
            cost += _SEARCH_COST
    return cost


def _apply_indexes(conn: sqlite3.Connection, indexes: tuple[str, ...]) -> None:
    for stmt in indexes:
        conn.execute(stmt)


def make_scorer(problem: Problem):
    """Build a deterministic Scorer bound to ``problem``.

    Returns an ``async (candidate, problem) -> Score`` suitable for
    ``score_candidates`` / ``ScoreCache``. Each call builds a fresh scratch
    database from the seed, so scoring one candidate never affects another.
    """

    async def scorer(candidate: Candidate, _problem: Problem) -> Score:
        conn = sqlite3.connect(":memory:")
        try:
            build_db(conn, seed=problem.seed, **problem.sizes)
            query = candidate.rewrite or problem.query

            # 1. Rewrite must return the same rows as the original.
            if candidate.rewrite is not None:
                base = conn.execute(problem.query).fetchall()
                alt = conn.execute(candidate.rewrite).fetchall()
                if base != alt:
                    return Score(
                        value=float("-inf"),
                        feasible=False,
                        reason="rewrite changed the result set",
                    )

            # 2. Apply the candidate's indexes to this scratch db.
            try:
                _apply_indexes(conn, candidate.indexes)
            except sqlite3.Error as exc:
                return Score(value=float("-inf"), feasible=False, reason=f"bad index: {exc}")

            # 3. Deterministic plan cost from EXPLAIN QUERY PLAN.
            detail = explain(conn, query)
            cost = plan_cost(detail)

            # 4. Trade-off: penalize each index so more is not automatically better.
            penalized = cost + problem.index_penalty * len(candidate.indexes)

            # Wall-clock, for insight / tie-break only — never the primary score.
            t0 = time.perf_counter()
            conn.execute(query).fetchall()
            elapsed_ms = (time.perf_counter() - t0) * 1000.0

            return Score(
                value=-penalized,
                feasible=True,
                metrics={
                    "plan_cost": cost,
                    "indexes": len(candidate.indexes),
                    "penalized_cost": penalized,
                    "elapsed_ms": round(elapsed_ms, 2),
                    "plan": detail,
                },
            )
        finally:
            conn.close()

    return scorer
