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

"""Quality gates: a checked "good enough?" decision on a step of work.

A gate is not a new kind of node in the graph API and not a new backend. It is
a convention plus a thin helper over the primitives you already have:
conditional edges, a per-gate attempt counter, and ``ctx.emit`` for telemetry.
The rule it enforces is the one that keeps long autonomous runs debuggable: a
work node writes an artifact; a *gate* decides what happens next — accept it,
send it back for revision, or escalate to a human.

The contract is one dataclass and one helper. Mix :class:`GateState` into your
state schema so the gate has channels for its bookkeeping::

    class ReviewState(GateState):
        artifact: Annotated[str, last]
        feedback: Annotated[str, last]

    async def evaluate(state, ctx) -> GateResult:
        score = await grade(state["artifact"])
        return GateResult(passed=score >= 0.85, score=score,
                          feedback=None if score >= 0.85 else "add tests")

    g = Graph(ReviewState)
    g.add_node("revise", revise)
    add_quality_gate(
        g, "review", evaluate,
        on_pass=END, on_fail="revise", on_escalate="ask_human",
        max_attempts=3,
    )
    g.add_edge("revise", "review")

GateResult vs Candidate
-----------------------
:class:`GateResult` and :class:`~agentflow.prebuilt.Candidate` look similar but
answer different questions; pick by what you are building.

- :class:`~agentflow.prebuilt.Candidate` is the output of the *worker* inside
  :func:`~agentflow.prebuilt.iterate_until_converged`. ``Candidate.done`` means
  "the loop may stop now"; the loop owns the stop policy (best-so-far,
  patience, max iterations).
- :class:`GateResult` is the output of an *evaluator* consulted by
  :func:`add_quality_gate`. ``GateResult.passed`` means "this artifact meets
  the bar"; the gate owns the routing (pass / fail-and-revise / escalate).

Use ``iterate_until_converged`` for a self-contained optimize loop with a score
to climb. Use ``add_quality_gate`` for an explicit checkpoint in a larger graph
that routes three ways and can hand off to a human. They compose: a gate's
``on_fail`` can point back into a revise node, which is its own small loop.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Annotated, Any, Protocol, runtime_checkable

from agentflow.events import GateEvent
from agentflow.graph import Graph
from agentflow.state import State, last, merge

__all__ = ["GateResult", "GateState", "Evaluator", "add_quality_gate"]


@dataclass(frozen=True)
class GateResult:
    """An evaluator's verdict on an artifact.

    ``passed`` is the only field the router requires. ``score`` and ``reasons``
    are for observability and human review; ``feedback`` is the actionable text
    a revise node can consume to improve the artifact on the next attempt.
    """

    passed: bool
    score: float | None = None
    reasons: list[str] = field(default_factory=list)
    feedback: str | None = None


class GateState(State):
    """Mix into a state schema to add the channels quality gates need.

    One pair of channels serves any number of gates in the graph — the gate
    name is the dict key, so you never declare per-gate fields. Both use
    last-wins/merge semantics and hold only gate bookkeeping.
    """

    #: Attempt count per gate name.
    gate_attempts: Annotated[dict, merge]
    #: The most recent :class:`GateResult` per gate name.
    gate_results: Annotated[dict, last]


@runtime_checkable
class Evaluator(Protocol):
    """A gate's decision function: ``async (state, ctx) -> GateResult``.

    It may be pure code or call a backend (an ``LLMBackend`` scoring the
    artifact, an ``AgentBackend`` reviewing a diff). It is an ordinary node
    body, not a backend — do not confuse it with a token source. Keep it
    read-only with respect to the artifact: it judges, it does not fix.
    """

    async def __call__(self, state: Mapping[str, Any], ctx: Any) -> GateResult: ...


def add_quality_gate(
    g: Graph,
    name: str,
    evaluator: Evaluator | Callable[[Mapping[str, Any], Any], Awaitable[GateResult]],
    *,
    on_pass: str,
    on_fail: str,
    on_escalate: str,
    max_attempts: int = 3,
) -> Graph:
    """Add a quality-gate node ``name`` and its three-way routing to ``g``.

    The gate runs ``evaluator`` once per visit and routes on the verdict:

    - passed                              -> ``on_pass``
    - failed with attempts remaining      -> ``on_fail`` (typically a revise
      node that loops back into this gate)
    - failed at ``max_attempts``          -> ``on_escalate`` (often a
      human-in-the-loop node, or :data:`~agentflow.graph.END`)

    ``on_pass`` / ``on_fail`` / ``on_escalate`` are node names already in the
    graph, or :data:`~agentflow.graph.END`. All three are required so a gate
    never silently accepts a failing artifact once it runs out of attempts —
    for an autonomous run point ``on_escalate`` at a human gate or a
    park-and-report node.

    The gate emits :class:`~agentflow.events.GateEvent`s (``started`` then one
    of ``passed`` / ``failed`` / ``escalated``) via ``ctx.emit``, so telemetry
    and the run stream show each decision and how many times a process looped on
    quality. It keeps its attempt count in the ``gate_attempts`` channel and its
    last verdict in ``gate_results`` — mix :class:`GateState` into your schema
    to declare both. Returns ``g`` for chaining.
    """
    if name in g.nodes:
        raise ValueError(f"node {name!r} already exists")
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")

    async def gate(state: Mapping[str, Any], ctx: Any) -> dict[str, Any]:
        attempts = dict(state.get("gate_attempts") or {})
        attempt = int(attempts.get(name, 0)) + 1
        ctx.emit(GateEvent(gate=name, phase="started", attempt=attempt))

        result = await evaluator(state, ctx)
        reasons = tuple(result.reasons)

        if result.passed:
            phase = "passed"
        elif attempt >= max_attempts:
            phase = "escalated"
        else:
            phase = "failed"
        ctx.emit(
            GateEvent(gate=name, phase=phase, attempt=attempt, score=result.score, reasons=reasons)
        )

        results = dict(state.get("gate_results") or {})
        results[name] = result
        return {"gate_attempts": {name: attempt}, "gate_results": results}

    def route(state: Mapping[str, Any]) -> str:
        results = state.get("gate_results") or {}
        result: GateResult | None = results.get(name)
        attempts = state.get("gate_attempts") or {}
        attempt = int(attempts.get(name, 0))
        if result is not None and result.passed:
            return "pass"
        if attempt >= max_attempts:
            return "escalate"
        return "fail"

    g.add_node(name, gate)
    g.add_conditional_edges(
        name,
        route,
        {"pass": on_pass, "fail": on_fail, "escalate": on_escalate},
    )
    return g
