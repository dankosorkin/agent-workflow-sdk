# 8. Surviving restarts

An optimizer that runs for minutes or hours will eventually meet a restart — a deploy, a crash, a preemption. This chapter makes the search durable: a run that stops mid-flight resumes from its last super-step in a fresh process, and applying the final plan happens exactly once even if the apply step runs twice.

Runnable file: `examples/tutorial/08_durability.py`.

## State must be JSON-serializable

A durable checkpointer (`FileCheckpointer`, `SqliteCheckpointer`, `PostgresCheckpointer`, `RedisCheckpointer`) stores each super-step's state as JSON. JSON has no set type and no custom classes — so state must be JSON-serializable. This is why the tutorial has represented a plan as a sorted tuple since chapter 1, not a `frozenset`:

```python
def with_strategy(plan, strategy):
    return tuple(sorted(set(plan) | {strategy}))   # a tuple round-trips as a list
```

A tuple round-trips through JSON as a list; a set would raise. The same rule is why `gate_results` holds plain dicts, not `GateResult` objects. If you ever see "Object of type X is not JSON serializable" from a checkpointer, this is the cause: something in state is not a JSON value. Keep state to dicts, lists, strings, numbers, and booleans.

## Resuming in a different process

A checkpointer persists every super-step keyed by `(thread, step)`. Resuming is just loading the last checkpoint and continuing from its frontier — and nothing requires the same process, or even the same `CompiledGraph` object. The example proves it by using two separate graph instances over one SQLite file:

```python
# process A: optimize until the approval pause, then "crash" (stop using it)
app_a = build().compile(checkpointer=SqliteCheckpointer(db), step_limit=100)
await app_a.invoke(init, thread="run-1")
assert (await app_a.get_state("run-1")).interrupted   # parked for approval
del app_a                                              # the process is gone

# process B: a brand-new graph, same db file, no shared memory with A
app_b = build().compile(checkpointer=SqliteCheckpointer(db), step_limit=100)
out = await app_b.resume("run-1", value="yes")         # picks up from the checkpoint
```

`process B` never saw the search happen. Everything it needs — the incumbent, the candidates, the frontier to resume from — is in the checkpoint `process A` wrote. That is durable execution: the run outlives the process.

```text
process A: optimizing (durable to sqlite):
  parked for approval with plan ['add_index', 'push_filter', 'reorder_stages'] — process A ends here
process B: resuming from the checkpoint:
  resumed and finished: plan ['add_index', 'push_filter', 'reorder_stages'] cost 21.6
```

## Applying a plan exactly once

Resuming is safe for the search because reducers fold deterministically. But applying a plan is a side effect — writing a config, rebuilding an index — and a side effect is not rolled back when a step is retried. The checkpoint contract is at-least-once: if a crash lands between performing the effect and recording that it happened, a resume runs the node again, and the effect could happen twice.

The fix lives at the effect. `IdempotentOp` guards one effect with a claim → run → commit marker in a `Store`, keyed by something stable — here, the plan itself:

```python
op = IdempotentOp(APPLY_STORE, ("effects", "apply_plan"))

async def apply_plan(state, ctx) -> dict:
    plan = state["search"]["incumbent"]["plan"]
    async def do_apply():
        return await actually_apply(plan)          # the real side effect
    receipt = await op.run("|".join(plan), do_apply, on_incomplete="rerun")
    return {"applied": [receipt]}
```

Run `apply_plan` a second time for the same plan and the effect does not fire again — `op.run` finds the stored `done` marker and returns the receipt. The example proves it by counting real effect calls across a redundant re-apply:

```text
  apply effect fired 1 time(s)
  after a redundant re-apply, effect STILL fired only 1 time(s)
```

The key is `task`-stable: the same plan always produces the same key, so a retry recognizes the effect as already done. Pass that key downstream too — as an API idempotency token, a stable file path, a unique DB constraint — so duplicates collapse at the effect itself, not just in the marker.

## What you learned

- Durable checkpointers store state as JSON; keep state to JSON values (this is why plans are tuples and `gate_results` are dicts).
- A run resumes from its checkpoint in any process — `resume(thread, ...)` on a fresh graph over the same store continues from the last super-step.
- The execution contract is at-least-once; a retried node can repeat a side effect.
- `IdempotentOp` makes an effect exactly-once, keyed on something stable, and the key is the token that collapses duplicates downstream.

The optimizer now survives restarts. But some steps wait on the outside world — a benchmark run that takes minutes. Blocking a worker that whole time is waste. In [chapter 9](09-watch.md) we park on an external result without holding a worker.
