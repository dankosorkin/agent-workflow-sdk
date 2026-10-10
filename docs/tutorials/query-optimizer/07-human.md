# 7. Human approval

Finding the best index set is automatic. Creating those indexes on a production table is not free — it takes space and slows writes. So before applying them, the optimizer pauses for a human to approve.

Runnable file: `examples/tutorial/queryopt/ch07_human.py`.

## Pause with ctx.interrupt

```python
async def approve(state, ctx) -> dict:
    inc = state["search"]["incumbent"]
    answer = await ctx.interrupt({
        "question": "apply these indexes?",
        "indexes": list(inc["indexes"]),
        "value": inc["value"],
    })
    return {"applied": str(answer).lower() in ("y", "yes", "apply")}
```

On a fresh run `ctx.interrupt` suspends and checkpoints; a later `resume` feeds the answer back and the call returns it. Interrupting needs a checkpointer, because the paused state must live somewhere.

```python
await app.invoke(init, thread="run-1")       # runs until the pause
state = await app.get_state("run-1")          # state.interrupted is True
req = state.interrupt_payload                 # the indexes awaiting approval
out = await app.resume("run-1", value="yes")  # apply them
```

```text
optimizing until the approval pause:
  PAUSED: apply 2 indexes (value -8.0)?
applied=True
```

## The same mechanism the agent uses

In chapter 5 the Codex agent ran with `AllowAll()`. Switch its policy to `Interactive` and every index the agent wants to create becomes a `ctx.interrupt` a human approves or denies — the identical pause-and-resume machinery, now driven by the agent's permission policy instead of a node you wrote. Approval is one mechanism whether you write the interrupt or an agent's policy writes it for you.

## What you learned

- `ctx.interrupt(payload)` pauses the run for a human; `resume(thread, value=...)` continues it with the answer.
- Interrupting requires a checkpointer — the paused state has to persist.
- An agent backend's `Interactive` policy turns tool/index requests into the same interrupt automatically.

In [chapter 8](08-durability.md) the search survives a restart, and a token budget guards an LLM-backed run.
