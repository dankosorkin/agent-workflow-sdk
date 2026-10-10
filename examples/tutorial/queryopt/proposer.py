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

"""The proposer: suggests index candidates for the query.

Two implementations behind one protocol, so the optimizer graph never changes:

- HeuristicProposer (default): deterministic, offline. Proposes indexes on the
  columns that appear in the query's JOIN and WHERE clauses — the moves a DBA
  would try first. Reproducible, so CI and "run the chapter" always work.
- CodexProposer (opt-in): a local Codex agent reads the schema and query and
  proposes indexes in its own words. Enabled when AGENTFLOW_TUTORIAL_BACKEND is
  "codex" and `codex` is on PATH. Non-deterministic, needs the CLI — for your
  machine, not CI.

Pick one with ``get_proposer()``, which reads the env flag.
"""

from __future__ import annotations

import os
from typing import Protocol

from examples.tutorial.queryopt.scorer import Candidate, Problem

__all__ = ["Proposer", "HeuristicProposer", "CodexProposer", "get_proposer"]


class Proposer(Protocol):
    async def propose(self, problem: Problem, applied: tuple[str, ...]) -> list[Candidate]:
        """Suggest candidates that extend the already-``applied`` indexes."""
        ...


# The index moves a DBA would consider for this query: one per JOIN/WHERE
# column, plus a couple of compound indexes. Each is a full CREATE INDEX stmt
# so a candidate carries exactly what the scorer will run.
_MOVES = {
    "oi_order": "CREATE INDEX i_oi_order ON order_items(order_id)",
    "oi_product": "CREATE INDEX i_oi_product ON order_items(product_id)",
    "o_customer": "CREATE INDEX i_o_customer ON orders(customer_id)",
    "o_date": "CREATE INDEX i_o_date ON orders(order_date)",
    "c_segment": "CREATE INDEX i_c_segment ON customers(segment)",
    "o_cust_date": "CREATE INDEX i_o_cust_date ON orders(customer_id, order_date)",
}


class HeuristicProposer:
    """Deterministic proposer: each untried index move as a single-index candidate."""

    async def propose(self, problem: Problem, applied: tuple[str, ...]) -> list[Candidate]:
        applied_set = set(applied)
        return [
            Candidate(indexes=tuple(sorted(applied_set | {stmt})))
            for stmt in _MOVES.values()
            if stmt not in applied_set
        ]


class CodexProposer:
    """A local Codex agent proposes index moves. Opt-in; needs ``codex`` on PATH.

    The agent is given the schema and query and asked for a few CREATE INDEX
    statements. We validate each line against the known moves so a hallucinated
    column cannot reach the scorer; anything unrecognized is dropped. The graph
    is identical to the heuristic path — only this object differs.
    """

    def __init__(self, model: str | None = None) -> None:
        self.model = model

    async def propose(self, problem: Problem, applied: tuple[str, ...]) -> list[Candidate]:
        from agentflow.backends.base import AllowAll
        from agentflow.backends.codex import CodexBackend
        from agentflow.events import TurnEnd
        from examples.tutorial.queryopt.db import SCHEMA

        # Give the agent the *search state*, not just the query: which indexes
        # are already applied, so it proposes the next step on top of them
        # rather than re-suggesting the obvious ones every round. This is what
        # lets the agent search incrementally, like the heuristic does.
        already = (
            "Indexes already applied (do not repeat these):\n  " + "\n  ".join(applied) + "\n\n"
            if applied
            else "No indexes applied yet.\n\n"
        )
        prompt = (
            "You are tuning indexes for a slow SQLite query, one step at a time.\n\n"
            "Schema:\n"
            + SCHEMA
            + "\nQuery:\n"
            + problem.query
            + "\n\n"
            + already
            + "Suggest the SINGLE most useful additional CREATE INDEX statement to add "
            "next, on a column used in a JOIN or WHERE. Reply with just the statement, "
            "no prose. If no further index would help, reply 'none'."
        )
        backend = CodexBackend(permission=AllowAll(), model=self.model)
        text = ""
        async with backend:
            async for ev in backend.prompt(prompt):
                if isinstance(ev, TurnEnd):
                    text = ev.text

        # Validate against the vetted move set so a hallucinated column cannot
        # reach the scorer; map the agent's free text onto known-safe moves.
        applied_set = set(applied)
        low = text.lower()
        chosen = {
            stmt
            for stmt in _MOVES.values()
            if stmt not in applied_set and stmt.split("ON ")[1].rstrip(";").lower() in low
        }
        if chosen:
            # one candidate per suggested move, each extending applied by one
            return [Candidate(indexes=tuple(sorted(applied_set | {stmt}))) for stmt in chosen]
        if "none" in low:
            # the agent judged no further index worthwhile — let the search end
            return []
        # non-empty but unmappable (a column we don't vet): fall back to the
        # heuristic for this round so the search still progresses and the
        # scorer decides. The agent informs; it does not gate.
        return await HeuristicProposer().propose(problem, applied)


def get_proposer() -> Proposer:
    """Pick the proposer from AGENTFLOW_TUTORIAL_BACKEND (default: heuristic)."""
    backend = os.environ.get("AGENTFLOW_TUTORIAL_BACKEND", "heuristic").lower()
    if backend == "codex":
        return CodexProposer()
    return HeuristicProposer()
