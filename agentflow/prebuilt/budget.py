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

"""Token / cost budget for an autonomous run.

An agentic loop that proposes with an LLM runs many rounds, each costing
tokens. Time has a guard (``with_timeout``), load has one (``max_concurrency``),
loop length has one (``step_limit``) — this is the guard for *spend*.

Two pieces, matching "how much did I spend?" and "stop at a cap":

- :class:`TokenMeter` is a :class:`~agentflow.observability.Hooks` listener that
  sums usage across a run from the backend events a node emits. Pure
  observation — it never affects the run (a hook cannot, by contract), so it
  answers "how much?" but cannot stop anything.

- To *stop*, budget is state. Mix :class:`BudgetState` into your schema, have
  the node that drives the LLM add its turn's usage with :func:`record_usage`,
  and have your router call :func:`over_budget` and route to a stop/escalate
  node when the cap is exceeded. Because the budget rides the checkpoint, it
  survives a resume (a resumed run does not forget what it already spent), and
  stopping is a routing decision — the same discipline as every other stop in
  the SDK: the work writes an artifact, a router decides what happens next.

    class SearchState(BudgetState):
        ...

    async def propose(state, ctx):
        turn = await call_llm(...)                 # yields a TurnEnd with .usage
        return {**result, **record_usage(turn.usage)}

    def route(state):
        if over_budget(state, max_tokens=100_000):
            return "stop"
        return "continue"

The stop takes effect at the next super-step boundary, not mid-turn, so a run
may overshoot the cap by at most the turn in flight. That is the deliberate
trade of the state-based design: durable and engine-free, at the cost of
turn-granular (not token-exact) enforcement.
"""

from __future__ import annotations

from typing import Annotated, Any

from agentflow.events import Usage
from agentflow.observability import Hooks
from agentflow.state import State, add, by_key

__all__ = ["TokenMeter", "BudgetState", "record_usage", "over_budget"]


class BudgetState(State):
    """Mix into a schema to add the ``budget`` channel the helpers accumulate.

    The channel is a dict whose numeric fields each sum via ``add``, so many
    nodes (or many rounds) fold their spend together. Fields mirror
    :class:`~agentflow.events.Usage`: ``input_tokens``, ``output_tokens``,
    ``total_tokens``, ``cost_usd``.
    """

    budget: Annotated[
        dict,
        by_key(
            {
                "input_tokens": add,
                "output_tokens": add,
                "total_tokens": add,
                "cost_usd": add,
            }
        ),
    ]


def record_usage(usage: Usage | None) -> dict[str, Any]:
    """Return a state update that adds ``usage`` into the ``budget`` channel.

    A ``None`` usage (a backend that reports no counts) contributes nothing —
    the returned update is empty, so a node can unconditionally
    ``return {**result, **record_usage(turn.usage)}``. Only the fields the
    backend actually reported are added; a missing field is treated as zero and
    omitted, so it does not fabricate a total.
    """
    if usage is None:
        return {}
    fields: dict[str, Any] = {}
    if usage.input_tokens is not None:
        fields["input_tokens"] = usage.input_tokens
    if usage.output_tokens is not None:
        fields["output_tokens"] = usage.output_tokens
    # total: prefer the reported total, else derive from input+output if present
    if usage.total_tokens is not None:
        fields["total_tokens"] = usage.total_tokens
    elif usage.input_tokens is not None or usage.output_tokens is not None:
        fields["total_tokens"] = (usage.input_tokens or 0) + (usage.output_tokens or 0)
    if usage.cost_usd is not None:
        fields["cost_usd"] = usage.cost_usd
    if not fields:
        return {}
    return {"budget": fields}


def over_budget(
    state: Any,
    *,
    max_tokens: int | None = None,
    max_cost_usd: float | None = None,
) -> bool:
    """True when accumulated spend has reached or passed a given cap.

    Checks whichever caps are provided: ``max_tokens`` against the summed
    ``total_tokens``, ``max_cost_usd`` against the summed ``cost_usd``. With no
    caps given it is always ``False`` (nothing to enforce). Spend not yet
    recorded (a backend that reports nothing) reads as zero, so a token cap can
    only fire when some backend actually surfaced counts.
    """
    budget = state.get("budget") or {}
    tokens_over = max_tokens is not None and budget.get("total_tokens", 0) >= max_tokens
    cost_over = max_cost_usd is not None and budget.get("cost_usd", 0.0) >= max_cost_usd
    return bool(tokens_over or cost_over)


class TokenMeter(Hooks):
    """A Hooks listener that accumulates token/cost usage across a run.

    Attach at ``compile(hooks=...)``. It sums the usage on every ``TurnEnd`` a
    node surfaces via ``ctx.emit``. Pure observation for logging or a metrics
    readout — it does not and cannot stop a run (hooks must never change
    behavior). To stop on a cap, use :func:`over_budget` in a router.
    """

    def __init__(self) -> None:
        self.input_tokens = 0
        self.output_tokens = 0
        self.total_tokens = 0
        self.cost_usd = 0.0
        self.turns = 0

    async def on_event(self, thread: str, step: int, node: str, event: Any) -> None:
        usage = getattr(event, "usage", None)
        if not isinstance(usage, Usage):
            return
        self.turns += 1
        if usage.input_tokens:
            self.input_tokens += usage.input_tokens
        if usage.output_tokens:
            self.output_tokens += usage.output_tokens
        if usage.total_tokens is not None:
            self.total_tokens += usage.total_tokens
        elif usage.input_tokens or usage.output_tokens:
            self.total_tokens += (usage.input_tokens or 0) + (usage.output_tokens or 0)
        if usage.cost_usd:
            self.cost_usd += usage.cost_usd

    def summary(self) -> dict[str, Any]:
        return {
            "turns": self.turns,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens,
            "cost_usd": round(self.cost_usd, 6),
        }
