# Long-running loops

Long autonomous processes — the kind you kick off and let run for minutes or hours — tend to reuse the same handful of skeletons. This chapter documents them as recipes built from the primitives, not as new engine features. The through-line is one discipline: a work node writes an artifact, and a separate gate or router decides what happens next. Keep those apart and your run stays debuggable, and a resume after a crash stays predictable.

## Recipe: optimize / improve

Draft, score, redraft until good enough. This is exactly what [`iterate_until_converged`](prebuilt.md) does — one `work` function returning a `Candidate`, and the loop owns best-so-far, patience, and the stop decision.

```mermaid
flowchart LR
    START --> step
    step -- again --> step
    step -- stop --> END
```

Reach for this when the whole task is "keep improving one artifact against one score". When you instead need an explicit accept / revise / escalate decision inside a bigger graph, use a [quality gate](../durability/quality-gates.md).

## Recipe: research → plan → implement → review

The workhorse shape for a gated pipeline. Two [quality gates](../durability/quality-gates.md) guard the risky transitions: a plan gate makes sure the plan is sound before any work happens, and a review gate checks the result.

```mermaid
flowchart LR
    research --> plan --> plan_gate{plan ok?}
    plan_gate -- no --> replan --> plan
    plan_gate -- yes --> implement --> review_gate{review ok?}
    review_gate -- no --> revise --> review_gate
    review_gate -- yes --> done
    review_gate -- exhausted --> escalate
```

Two rules make this safe to run unattended:

- `plan` and `review` are the natural human-gate or hard-evaluator points. Escalate there, not from the middle of `implement`.
- `implement` should be narrow and idempotent, so a resume after a crash re-runs it safely. Wrap it with [`skip_if_done`](#recipe-idempotency-skip-work-already-done) so an identical plan does not rebuild.

`examples/research_plan_implement_review.py` is a runnable version.

## Recipe: critic / adversarial loop

Two roles, one artifact. A builder produces work; a critic returns structured findings but never edits. The builder fixes; the critic re-checks. Stop when the critic signs off or a step budget is spent.

```mermaid
flowchart LR
    build --> critic{findings?}
    critic -- issues --> build
    critic -- lgtm --> END
```

Model the critic as an evaluator returning a `GateResult` whose `reasons` carry the findings and whose `feedback` tells the builder what to fix. The builder reads `state["gate_results"][...]` on the next pass. This keeps "judge" and "fix" in separate nodes — the same discipline as every other recipe here.

## Recipe: fan-out / map-reduce with a gate on the aggregate

Run several independent branches — different hypotheses, files, or test suites — then merge and gate the combined result.

```mermaid
flowchart TD
    START --> a & b & c
    a --> merge
    b --> merge
    c --> merge
    merge --> gate{aggregate ok?}
```

Two things matter here. First, reducers do the merging: give the aggregate channel an `append` or `merge` reducer so the parallel updates fold together deterministically instead of racing. Second, a super-step is all-or-nothing — if any branch raises, the whole step is discarded and no checkpoint is written, so a resume never sees a half-merged aggregate. Bound the fan-out with `compile(max_node_concurrency=...)` if the branches hit a shared resource.

## Recipe: budget-aware stopping

An autonomous run needs a ceiling — a router that stops not only on quality but on cost. Keep the budget where it belongs: `attempts` is a decision input that must survive a resume, so it lives in state; wall-clock time and token counts are non-deterministic, so keep them in [`RunMetrics`](../durability/observability.md) or your own hook rather than in the checkpoint. Putting a timestamp in state means a resume would "replay" a time from the past and mislead the router.

```python
from typing import Annotated
from agentflow import add, last

class BudgetState(GateState):
    attempts: Annotated[int, add]      # survives resume — safe to route on

def route(state):
    result = state["gate_results"].get("review")
    if result and result.passed:
        return "done"
    if state.get("attempts", 0) >= 10:   # hard budget ceiling
        return "escalate"
    return "revise"
```

A quality gate already enforces a per-gate `max_attempts`; this recipe is for a budget that spans several nodes or the whole run.

## Recipe: idempotency, skip work already done

Long loops re-run the same steps — after a crash, on resume, or on another iteration. Expensive, deterministic work should run once per distinct input. [`skip_if_done`](../reference/api/gate.md) is a recipe over the [Store](../durability/store.md): hash the node's input, serve a cached result if one exists under that hash, otherwise run and cache.

```python
from agentflow import MemoryStore
from agentflow.prebuilt import artifact_key, skip_if_done

store = MemoryStore()   # or PostgresStore for a durable, shared cache

async def expensive(state, ctx):
    return {"result": await do_costly_thing(state["input"])}

node = skip_if_done(
    expensive, store,
    namespace=("cache", "expensive"),
    key_from=lambda s: artifact_key(s["input"]),
)
g.add_node("expensive", node)
```

`artifact_key` is a stable SHA-256 of any JSON-serializable value (dict order does not matter), so it keys by *what* the input is rather than when it ran. Because the cache is a `Store`, a `PostgresStore` shares it across processes and a `ttl` expires stale entries. Only wrap deterministic work whose result depends solely on the hashed input — a node with side effects you always want is a poor fit.

## Side effects and delivery semantics

There is a boundary worth stating plainly, because it is easy to misread "atomic super-step" as more than it is. A super-step is atomic **over state**: if any node in the step raises, the step's state updates are discarded and no checkpoint is written. That transaction covers the checkpointer and nothing else. A side effect a node already performed — a file written, an HTTP `POST` sent, a row inserted in another system — is *not* rolled back, because the engine has no transaction over it. On a resume the node runs again and the effect can happen twice.

This is not a defect to fix. It is the delivery contract of every durable executor that does not wrap your external systems in a distributed transaction: **checkpoints give at-least-once execution; exactly-once holds only if the effect itself is idempotent.** There is an irreducible window between "the effect happened" and "the checkpoint recorded it" — a crash in that gap always risks a repeat.

So the fix lives at the effect, and AgentFlow gives you `IdempotentOp` to make it explicit. Unlike `skip_if_done`, which wraps a whole node to avoid recomputing harmless work, `IdempotentOp` guards *one specific effect inside a node* with a claim → run → commit marker in a [Store](../durability/store.md):

```python
from agentflow.prebuilt import IdempotentOp, effect_key

op = IdempotentOp(store, ("effects", "send_email"))

async def notify(state, ctx):
    key = effect_key("send_email", state["message_id"])

    async def send():
        # pass the SAME key downstream as the provider's idempotency token
        return await email_api.send(state["to"], state["body"], idempotency_key=key)

    receipt = await op.run(key, send, on_incomplete="error")
    return {"receipt": receipt}
```

The state machine per key is held in one store entry so a plain `get` reads it in one shot:

| Prior state | `op.run` does |
| --- | --- |
| absent | write `in_flight`, run the effect, write `done(result)`, return it |
| `done` | return the stored result — the effect does not run again |
| `in_flight` | a prior attempt did not reach `done`; dispatch on `on_incomplete` |

`on_incomplete` is the honest part: the SDK cannot know whether an interrupted effect actually landed, so you choose per effect.

| `on_incomplete` | Meaning | Use when |
| --- | --- | --- |
| `"error"` (default) | raise `IncompleteEffectError` | the effect is not safe to repeat — let a human or gate decide |
| `"rerun"` | run the effect again | the effect is genuinely idempotent |
| `"skip"` | assume it completed, return `None` | a duplicate is worse than a miss |

A `done` marker only appears when the effect returned cleanly, so this reliably catches a hard crash mid-effect. But an exception is trickier — and it is a second, separate window.

When `fn` raises, the effect may still have happened: a `TimeoutError` reading the response of a `POST` that already went through is an exception *after* delivery. `on_error` decides what the marker is left as:

| `on_error` | Meaning | Use when |
| --- | --- | --- |
| `"keep"` (default) | leave `in_flight` so the next attempt hits `on_incomplete` | the exception does not prove the effect was skipped (the safe default) |
| `"release"` | drop the marker; the retry re-runs the effect | the exception reliably means "not done" (e.g. a connect error before any request left) |

The consequence to internalize: `on_incomplete="error"` protects the crash window, but with `on_error="release"` it does **not** protect the lost-response window — the marker is already gone, so the retry simply runs the effect again.

One more boundary: the claim is a `get` then a `put`, **not** an atomic compare-and-set. Two workers can both read `absent` for the same key and both run the effect. `IdempotentOp`'s guarantee holds for a single writer per key (one run, retried sequentially over time); under real concurrency, correctness must come from idempotency at the effect.

So `IdempotentOp` narrows the duplicate window and makes every ambiguous case an explicit decision — but it does not, and cannot, close it at the orchestrator layer. What actually collapses duplicates is idempotency at the effect itself; use the key `IdempotentOp` gives you as the token that carries it there:

| Effect | Idempotency technique |
| --- | --- |
| HTTP API | send an idempotency key / dedup token the server honors |
| File write | write a temp file, then atomic `rename` to a *stable* destination path; the retry must produce the same path and the same content for the replace to be idempotent |
| Database | `INSERT ... ON CONFLICT DO NOTHING`, or a unique constraint |
| Message/queue | a dedup id the broker or consumer deduplicates on |

Reach for `skip_if_done` to save work; reach for `IdempotentOp` to protect an effect. `examples/idempotent_effect.py` is a runnable walk-through: an effect whose response is lost, the retry that `IdempotentOp` stops, and a downstream store that deduplicates on the same key.

## The one rule behind all of these

Do not mix "the work" and "the decision to continue" in one node. The work node writes an artifact; a gate or router reads state and decides. That separation is what makes each super-step atomic, each checkpoint meaningful, and each resume predictable — the properties a run needs to survive being left alone for an hour.

Next: the [quality gates](../durability/quality-gates.md) chapter for the gate contract in full, or the [tutorial](tutorial-qa-agent.md) for an end-to-end agent.
