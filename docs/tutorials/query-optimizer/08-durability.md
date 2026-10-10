# 8. Surviving restarts, and a token budget

Two concerns for a run that lasts: it should survive a restart, and when an LLM is proposing, it should not burn tokens without a ceiling.

Runnable file: `examples/tutorial/queryopt/ch08_durability.py`.

## Durable search

With a checkpointer, the search is persisted every super-step and a fresh process can read it. The state is all JSON-serializable — index statements are strings, candidate summaries are dicts — so a durable checkpointer can store it (the same rule the mechanics tutorial hit: no sets, no custom objects in state).

```python
app_a = build(problem).compile(checkpointer=SqliteCheckpointer(db), step_limit=50)
await app_a.invoke(init, thread="run-1")      # process A optimizes

app_b = build(problem).compile(checkpointer=SqliteCheckpointer(db))
reloaded = await app_b.get_state("run-1")     # process B reads the result
```

## A token budget for the LLM path

When the proposer is an LLM, each round costs tokens — and a round count does not capture that a few expensive turns can blow the budget. The [`budget`](../../reference/api/budget.md) helpers make spend a durable, routable value. Mix `BudgetState` in, record each round's usage, and have the router stop at a cap:

```python
usage = Usage(total_tokens=...)              # from the LLM turn; 0 for the heuristic
updates = {**record_usage(usage)}
...
def route(state):
    if max_tokens is not None and over_budget(state, max_tokens=max_tokens):
        return "stop"
    return "again" if state.get("improved") else "stop"
```

Because budget rides the checkpoint, a resumed run continues from what it already spent rather than forgetting. The stop is a routing decision at the next round boundary, so a run can overshoot by at most one round — the deliberate trade for a budget that needs no engine support.

```text
process A: optimizing (durable to sqlite):
  done: value -8.0 with 2 indexes
process B: read the checkpoint -> value -8.0
  budget total_tokens recorded: 0
```

With the heuristic proposer the usage is zero, so the cap is never tripped and the offline run is unaffected. Switch to the LLM proposer and the same wiring stops a runaway search before it runs up a bill — the protection that matters exactly at the production switch.

## Applying indexes exactly once

Creating indexes is a side effect; a resume could re-run the apply node. Guard it with `IdempotentOp` keyed on the index set (as the mechanics tutorial shows) so a retried apply is a no-op — `CREATE INDEX IF NOT EXISTS` plus the idempotency marker make it safe to repeat.

## What you learned

- A checkpointer makes the search resumable in a fresh process; keep state JSON-serializable.
- `BudgetState` + `record_usage` + `over_budget` cap token (or cost) spend, durably across a resume.
- The budget stops at a round boundary — turn-granular, not token-exact — which is the price of needing no engine change.
- With no LLM, usage is zero and the budget is inert; it protects the LLM path without affecting the offline one.

In [chapter 9](09-watch.md) the optimizer parks on a full benchmark before trusting a plan.
