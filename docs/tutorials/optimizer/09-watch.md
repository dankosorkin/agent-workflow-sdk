# 9. Waiting on a benchmark

The cost model is a stand-in. In a real optimizer you would confirm a promising plan with an actual benchmark before trusting it — and a benchmark takes real time. Blocking a worker while it runs is waste. This chapter parks the optimizer on an external benchmark: it kicks the run off, polls for the result, and suspends between polls without holding anything.

Runnable file: `examples/tutorial/09_watch.py`.

## Parking vs blocking

A node that waits by sleeping holds its worker the whole time. A node that waits by parking suspends the run — it writes a checkpoint marked to wake at a future time and hands control back. Nothing spins while parked. This is `ctx.wait`: a timer-based sibling of the human interrupt from chapter 7, resolved by the clock instead of a person.

A watch node wraps that in a poll loop. Each poll reports one of three outcomes:

- idle — nothing yet; park and poll again later.
- activity — something changed; hand it to the graph.
- terminal — the external thing finished; exit the loop.

## A watcher is any command that prints the contract

`CommandWatcher` runs a command that prints one JSON object — `{"outcome", "payload", "cursor"}` — and maps it to a poll result. The benchmark here is a shell script that reports `idle` a couple of times then `terminal` with the measured cost; a real `gh run view ... --json` or a CI poll drops straight in.

```python
watcher = CommandWatcher(f"sh {script} {count} {measured}")

g.add_node("start", start_benchmark)         # kick off the benchmark
g.add_node("wait", watch_node(watcher, poll_interval=0.2))
g.add_node("promote", promote)               # reads state["watch_result"]["payload"]
g.add_edge("start", "wait")
g.add_conditional_edges("wait", route_watch, {"activity": "wait", "terminal": "promote"})
g.add_edge("promote", END)
```

`watch_node` polls, and on an idle result calls `ctx.wait(now + poll_interval, cursor)` to park until the next poll. It only returns to the router on activity or terminal, so `route_watch` always sees a fresh, non-idle result. The cursor rides the park, so a stateless poll script can resume where it left off.

## Driving the park inline

`run_until_done` drives timer waits inline: it runs until the graph parks, sleeps until the checkpoint's `wake_at`, resumes, and repeats until the run finishes. It needs a checkpointer, because each park is a checkpoint.

```python
out = await app.run_until_done(
    {"plan": plan, "modeled_cost": modeled}, thread="bench-1", max_sleep=0.2
)
```

```text
optimizing with a benchmark gate (parks between polls):
  kicking off benchmark for plan ['add_index', 'push_filter']
  benchmark done: measured cost 25.0 (modeled 24.0)
final: plan ['add_index', 'push_filter'] modeled 24.0 measured 25.0
the benchmark was polled 3 times
```

Note the measured cost (25.0) differs from the modeled cost (24.0) — the whole point of benchmarking: the model is a guide, the benchmark is truth, exactly the propose/verify split from chapter 5 taken to the real world.

## Inline vs parked — the honest difference

`run_until_done` keeps the process alive for the whole wait. That is fine for development and short waits, but it is not free the way true parking is:

- Inline (`run_until_done`): no worker to release, so the method just sleeps until `wake_at`. The process must stay up.
- Control plane (chapter 10): a parked run is `WAITING` in the queue with a `wake_at`, and the worker is released — zero cost while parked, re-claimed when the time comes, surviving a restart.

Two honest limits either way: polling is not push (an event is seen at the next poll, so `poll_interval` trades freshness for cost), and a watcher must resume from its cursor, not from in-memory state, because the park is a checkpoint boundary.

## What you learned

- `ctx.wait(wake_at)` parks a run on a timer, the clock's version of an interrupt; nothing spins while parked.
- `watch_node` + `CommandWatcher` + `route_watch` build a poll loop over anything a command can check.
- The watcher's cursor survives the park; resume from it, not from memory.
- Inline `run_until_done` sleeps through waits (process stays up); the control plane releases the worker entirely.

Everything so far runs in one process. In [chapter 10](10-plan-executor.md) the optimizer becomes a background job: the planner emits a dependency graph of experiments, `plan_executor` runs and re-optimizes it, and the control plane plus an HTTP server let you submit a problem and watch it progress.
