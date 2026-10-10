# 7. Human approval

The optimizer finds the best plan on its own. But finding and applying are different acts: applying a plan might rebuild an index, change a production config, or cost real money. That step deserves a human's sign-off. This chapter pauses the run for approval before applying, using the same interrupt machinery the framework uses for every human-in-the-loop case.

Runnable file: `examples/tutorial/07_human.py`.

## Suspending for a human

A node asks for human input by calling `ctx.interrupt(payload)`. On a fresh run this raises inside the engine, which catches it, writes an interrupted checkpoint, and stops. The run is now parked — no worker held, nothing spinning. Later, a resume supplies the answer and the same `ctx.interrupt` call returns it.

```python
async def approve(state, ctx) -> dict:
    incumbent = state["search"]["incumbent"]
    answer = await ctx.interrupt({
        "question": "apply this plan?",
        "plan": sorted(incumbent["plan"]),
        "cost": incumbent["cost"],
    })
    decision = "apply" if str(answer).lower() in ("y", "yes", "apply") else "skip"
    return {"decision": decision}
```

The payload is yours to shape — it is what the human (or the UI calling `resume`) sees to make the decision. Here it carries the plan and its cost.

## Driving the pause and resume

Interrupting requires a checkpointer, because the paused state must be persisted somewhere to resume from. The run suspends inside `invoke`; you inspect the pending request and resume with an answer.

```python
app = build().compile(checkpointer=MemoryCheckpointer(), step_limit=100)

await app.invoke(init, thread="run-1")      # runs until the interrupt, then returns

state = await app.get_state("run-1")
assert state.interrupted                    # parked at approve
req = state.interrupt_payload               # the payload we passed to ctx.interrupt
# ... show req to a human, get their answer ...

out = await app.resume("run-1", value="yes")  # the answer becomes ctx.interrupt's return
```

`get_state` reads the checkpoint without running anything; `interrupt_payload` is exactly what the node passed. `resume` feeds the value back into the waiting `ctx.interrupt` and continues the run to `END`.

## Run it

```text
optimizing until the approval gate:
  PAUSED for approval: apply ['add_index', 'push_filter', 'reorder_stages'] (cost 21.6)?
  applied plan {'add_index', 'push_filter', 'reorder_stages'}
applied=True final plan {'add_index', 'push_filter', 'reorder_stages'}
```

The search ran automatically, parked for approval, and only applied the plan after we said yes. Resume with `"no"` instead and `apply_plan` would record `applied=False` and change nothing.

## What is actually saved

The interrupted checkpoint holds the full state and the frontier to resume from — the node that interrupted will run again on resume, and its `ctx.interrupt` returns the resumed value instead of raising. Everything the optimizer discovered (the incumbent, the candidates) is in that checkpoint, so the pause can last milliseconds or days; a resume picks up exactly where it stopped. That durability is the subject of the next chapter.

## Agent backends get this for free

We drove the interrupt by hand here. When a node uses an agent backend with the `Interactive` permission policy, the same mechanism fires automatically: every tool the agent wants to run becomes a `ctx.interrupt` the human approves or denies via `resume`. The approval pattern is identical whether you write the interrupt yourself or an agent's permission policy writes it for you.

## What you learned

- `ctx.interrupt(payload)` suspends a run and checkpoints it; `resume(thread, value=...)` feeds the answer back and the call returns it.
- Interrupting requires a checkpointer — the paused state has to live somewhere.
- `get_state` inspects a parked run; `interrupt_payload` is what the node asked.
- The `Interactive` permission policy turns an agent's tool requests into the same interrupt automatically.

A parked run is only as durable as its checkpointer. In [chapter 8](08-durability.md) we make the whole optimizer survive a process crash — and make applying a plan safe to retry.
