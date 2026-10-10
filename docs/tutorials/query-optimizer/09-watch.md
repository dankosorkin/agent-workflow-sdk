# 9. Waiting on a benchmark

The `EXPLAIN`-based score is cheap, deterministic, and perfect as an in-loop guide. But before committing an index set to production you want to confirm it on a full benchmark against realistic data — which takes minutes. The optimizer kicks that off and parks on a watch node until it reports back, instead of holding a worker busy.

Runnable file: `examples/tutorial/queryopt/ch09_watch.py`.

## Park, do not block

A watch node polls an external thing and parks between polls with `ctx.wait`, so nothing spins while the benchmark runs. The watcher reports idle (park and poll again), activity, or terminal (done).

```python
watcher = CommandWatcher(f"sh {script} {count} {latency}")
g.add_node("wait", watch_node(watcher, poll_interval=0.2))
g.add_conditional_edges("wait", route_watch, {"activity": "wait", "terminal": "record"})
```

`CommandWatcher` runs any command that prints the small contract JSON (`{"outcome", "payload", "cursor"}`). Here it is a stand-in script reporting `idle` twice then `terminal` with a measured latency; a real benchmark — run the query against a large dataset and print the result — drops straight in.

```text
confirming the chosen plan on a benchmark (parks between polls):
  submitted benchmark for 1 indexes
  benchmark reported 42.0 ms
confirmed: 1 indexes, benchmark 42.0 ms
the benchmark was polled 3 times
```

## Two measures, two purposes

This chapter pairs with the propose/verify split from chapter 5 in a new way: the `EXPLAIN` plan cost is the fast, deterministic measure that drives the search; the benchmark latency is the slow, authoritative measure that confirms the final choice before it ships. Cheap guide in the loop, real confirmation at the boundary — the same discipline, two scales.

Driven inline here with `run_until_done` (the process stays alive through the wait); under the control plane in chapter 10, a parked run releases its worker entirely and is re-claimed when the benchmark is due.

## What you learned

- `ctx.wait` / `watch_node` park a run on an external result without holding a worker.
- `CommandWatcher` watches anything a command can check; the cursor survives the park.
- A fast in-loop measure (`EXPLAIN`) and a slow confirming measure (a benchmark) play complementary roles.

In [chapter 10](10-plan-executor.md) the planner emits a dependency graph of experiments and the whole optimizer runs on the control plane.
