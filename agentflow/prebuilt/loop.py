"""Iterate-until-converged: the classic optimization loop as a graph.

This reproduces the behavior of the original ``AgentLoopRunner`` — a baseline
turn followed by iterations that stop on any of: the agent declaring done, a
perfect score, or no improvement for ``patience`` consecutive rounds — but
built entirely from core primitives (nodes, a conditional edge, reducer
channels). It is proof the engine subsumes the old fixed loop, and a handy
building block on its own.

You supply one async ``work`` function that performs a turn and returns a
:class:`Candidate` (a score plus whatever payload you carry). The loop owns
the bookkeeping: best-so-far, no-improvement streak, and the stop decision.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Annotated, Any

from agentflow.graph import END, START, Graph
from agentflow.state import State, add, append, last

__all__ = ["Candidate", "iterate_until_converged"]


@dataclass(frozen=True)
class Candidate:
    """One turn's result. ``done`` lets the worker end the loop itself."""

    score: float
    payload: Any = None
    done: bool = False


# The worker: (state) -> Candidate. It receives the loop state so it can read
# the current best and the last candidate if it wants to.
Worker = Callable[[dict], Awaitable[Candidate]]


class _LoopState(State):
    best_score: Annotated[float, last]
    best_payload: Annotated[Any, last]
    last_score: Annotated[float, last]
    streak: Annotated[int, last]
    iterations: Annotated[int, add]
    stop_reason: Annotated[str, last]
    history: Annotated[list, append]
    _done: Annotated[bool, last]


def iterate_until_converged(
    work: Worker,
    *,
    perfect_score: float = 100.0,
    patience: int = 4,
    max_iterations: int = 20,
    checkpointer: Any = None,
):
    """Compile a loop graph around ``work``.

    Stop conditions, in priority order:

    - the worker returns ``Candidate(done=True)`` -> ``stop_reason="done"``
    - best score reaches ``perfect_score`` -> ``stop_reason="optimal"``
    - no improvement for ``patience`` rounds -> ``stop_reason="converged"``
    - ``max_iterations`` reached -> ``stop_reason="exhausted"``

    The final state carries ``best_score``, ``best_payload``, ``stop_reason``,
    ``iterations``, and ``history`` (a list of per-turn scores).
    """

    async def step(state: dict, ctx) -> dict:
        candidate = await work(state)
        prev_best = state.get("best_score", float("-inf"))
        improved = candidate.score > prev_best

        update: dict[str, Any] = {
            "iterations": 1,
            "last_score": candidate.score,
            "history": candidate.score,
        }
        if improved:
            update["best_score"] = candidate.score
            update["best_payload"] = candidate.payload
            update["streak"] = 0
        else:
            update["streak"] = state.get("streak", 0) + 1

        best_now = candidate.score if improved else prev_best
        iters = state.get("iterations", 0) + 1

        if candidate.done:
            update["stop_reason"] = "done"
            update["_done"] = True
        elif best_now >= perfect_score:
            update["stop_reason"] = "optimal"
            update["_done"] = True
        elif update.get("streak", 0) >= patience:
            update["stop_reason"] = "converged"
            update["_done"] = True
        elif iters >= max_iterations:
            update["stop_reason"] = "exhausted"
            update["_done"] = True
        else:
            update["_done"] = False

        return update

    def route(state: Mapping[str, Any]) -> str:
        return "stop" if state.get("_done") else "again"

    g = Graph(_LoopState)
    g.add_node("step", step)
    g.add_edge(START, "step")
    g.add_conditional_edges("step", route, {"again": "step", "stop": END})
    # A generous engine-level guard well above max_iterations; the loop's own
    # counter is the real terminator.
    return g.compile(checkpointer=checkpointer, step_limit=max_iterations + 5)
