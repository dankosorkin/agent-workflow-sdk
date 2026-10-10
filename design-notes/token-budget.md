# Design note: token / cost budget

Status: draft for review. A working design note, like `plan-executor.md` and
`eval-harness.md`. Target: event + prebuilt changes in `agentflow`.

## The gap this closes

Time has a guard (`with_timeout`), load has one (`max_concurrency`), loop
length has one (`step_limit`). Token/dollar spend has none. An autonomous
optimizer that proposes with an LLM runs many rounds, each costing real tokens
— and nothing stops "it ran all night and burned $500." When the proposer is a
local agent the cost is your machine; swap to a hosted LLM and the cost is
money, so the budget guard matters exactly at the "agents locally, LLM in
prod" switch the tutorial is about.

## Two problems, in order

1. Usage is not surfaced. `TurnEnd` carries `text`, `stop_reason`, `message` —
   no token counts. Backends see usage (Codex prints a `usage` block; OpenAI/
   Anthropic/Ollama return token counts) but drop it. Nothing downstream can
   budget what it cannot see. This must be fixed first.
2. There is no budget guard that can stop a run. `Hooks` can count but,
   by contract, must never raise or change behavior — so a hook cannot stop a
   run. Stopping must come from a place that affects control flow.

## Part 1: surface usage on TurnEnd

Add an optional `Usage` to `TurnEnd` (and to the backend events where a turn
ends). Keep it a small, backend-neutral value; each backend fills what it
knows, missing fields stay `None`.

```python
@dataclass(frozen=True, slots=True)
class Usage:
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None     # when a backend only reports a total
    cost_usd: float | None = None       # when a backend/pricing map provides it

@dataclass(frozen=True, slots=True)
class TurnEnd:
    text: str = ""
    stop_reason: str | None = None
    message: Message | None = None
    usage: Usage | None = None          # NEW, optional, backward-compatible
```

Backward-compatible: `usage` defaults to `None`, existing code and backends
that do not set it are unaffected. Backends are updated to populate it where
the underlying engine reports counts (Ollama, OpenAI-compatible, Anthropic,
Codex's `usage` block). A backend with no counts leaves it `None` — then a
budget by tokens cannot apply and the design says so plainly rather than
guessing.

## Part 2: accounting + a budget that can stop

Two layers, matching the two things a user wants ("how much did I spend?" and
"stop at a cap"):

### 2a. A Hooks listener that accumulates spend (observe)

`TokenMeter(Hooks)` sums usage across a run from the `on_event` callback (it
already receives every `ctx.emit`-ed backend event). Pure observation — totals
for logging and a `/metrics`-style readout. Like `RunMetrics`, it never affects
the run.

```python
meter = TokenMeter()
app = graph.compile(hooks=meter)
...
meter.total_tokens, meter.cost_usd      # after the run
```

But a meter cannot stop a run (Hooks must not raise). So:

### 2b. A budget the run can act on

The honest options for *stopping*, since hooks can't:

- Option S1 — a budget channel in state + a router. A node (or a small wrapper)
  adds each turn's usage into a `budget` channel; the optimizer's router reads
  it and routes to a stop/escalate node when the cap is exceeded. Durable
  (budget rides the checkpoint, survives resume), deterministic, and uses only
  existing primitives. The downside: the user must wire the router, and the
  stop happens at the next super-step boundary, not mid-turn.
- Option S2 — `ctx` enforces it: a `ctx.spend(usage)` that raises a
  `BudgetExceeded` when over the cap, caught by the runtime like an interrupt.
  Stops closer to the moment, but it is new engine surface, and raising from
  inside a node mid-turn is murkier for a durable resume (the half-done turn's
  effect/٭state).
- Option S3 — a `BudgetHook` that, unlike a plain Hooks, is allowed to request
  cancellation. This breaks the "hooks never change behavior" contract that is
  stated and relied on; I do not want to muddy it.

Recommendation: S1. A budget is *data* that survives a resume (exactly like the
`attempts` budget in the existing budget-aware-stopping recipe), and stopping
is a routing decision — the same discipline as every other stop in this SDK
("the work writes an artifact; a router decides what happens next"). It needs
no engine change and composes with `by_key`/gates/plan_executor. S2 can come
later if a mid-turn stop is ever needed; it is a bigger, riskier change.

### The prebuilt

A small helper so users do not hand-roll the accounting:

```python
class BudgetState(State):                     # mixin, like GateState/PlanState
    budget: Annotated[dict, by_key({
        "input_tokens": add, "output_tokens": add,
        "total_tokens": add, "cost_usd": add,
    })]

def record_usage(usage: Usage) -> dict:       # -> a {"budget": {...}} update
    ...

def over_budget(state, *, max_tokens=None, max_cost_usd=None) -> bool:
    ...                                        # the router's predicate
```

A proposer/LLM node returns `record_usage(turn.usage)` alongside its result;
the router calls `over_budget(state, max_tokens=...)` and routes to a stop node
when true. `TokenMeter` (2a) stays available for pure observation when you do
not need to stop.

## Determinism and the tutorial

For the heuristic proposer (default, offline) there is no token usage — budget
is simply never hit, and the tutorial runs reproducibly. With the Codex/LLM
proposer, `usage` is populated and the budget guard becomes live. So the budget
chapter demonstrates: with a cap set low, the LLM-backed optimizer stops early
and escalates rather than running away — the protection you asked for at the
prod switch. The heuristic path shows the same wiring with usage of zero.

## Open questions for review

1. S1 (budget-as-state + router) vs S2 (`ctx.spend` raising). I strongly prefer
   S1 (durable, no engine change, matches existing stop discipline). Agree?
2. `Usage` fields: input/output/total tokens + `cost_usd`. Cost needs a
   price map (tokens -> $). Do we ship a tiny per-model price table, or leave
   `cost_usd` to the caller and budget on tokens by default? I lean: budget on
   tokens (always available), leave `cost_usd` optional and caller-provided —
   a bundled price table goes stale and is a maintenance trap.
3. Scope of surfacing usage now: all backends, or just the ones the tutorial
   uses (Codex + one LLM)? I lean: define `Usage` + wire it on `TurnEnd` for
   all, but only populate the backends we can test, leaving others `None` with
   a note. Avoids shipping unverified parsing for backends we can't exercise.
4. Naming: `TokenMeter` / `BudgetState` / `record_usage` / `over_budget`.
   Reasonable, or prefer `CostMeter` / `UsageState`? I lean `TokenMeter` +
   `BudgetState` (tokens are the always-present unit; "budget" is the concept).
