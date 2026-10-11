# Building an optimizer

This tutorial builds one system across ten chapters: an optimizer that is handed a task and improves its solution as it runs. By the end it proposes candidate solutions, verifies them deterministically, keeps the best so far, re-plans based on what it learned, survives a crash, waits on an external benchmark without burning a worker, and runs as a background job behind an HTTP API.

We build it one layer at a time. Each chapter adds a single idea and ends with a runnable program; the next chapter grows that same program. Nothing is thrown away — the file you have at the end of chapter 3 is the starting point for chapter 4.

## The problem we optimize

To keep every chapter runnable offline and perfectly reproducible, the thing being optimized is a toy *query plan* scored by a deterministic cost model — not a real database. A plan is a set of applied strategies (add an index, reorder stages, push a filter down); each strategy changes the plan's modeled cost. Lower cost is better. There is no network and no model in the early chapters, so you can run and re-run and always get the same numbers.

This is a stand-in, chosen for determinism. The shape — propose, verify, keep the best, re-plan — is exactly the shape of a real optimizer (a query planner, a config tuner, a scheduler). In chapter 5 we swap the hand-written proposer for a real model, and in chapter 10 the proposer becomes a planner emitting a dependency graph of experiments. The cost model stays a stand-in throughout so results stay reproducible; only the intelligence proposing moves changes.

## What each chapter adds

1. Your first graph — state, a node, a reducer, one run. Score a single plan.
2. Branching and loops — a router picks which strategy to try; the graph loops.
3. Parallel experiments — try several strategies at once in one super-step; merge the results.
4. Dual-state with `by_key` — separate the fixed problem facts from the moving search state.
5. A real planner backend — the proposer calls a model; the verifier stays deterministic.
6. The promotion gate — accept a new best only when it is verified better.
7. Human approval — pause for sign-off before applying an expensive plan.
8. Surviving restarts — a checkpointer makes the search resumable; applying a plan is idempotent.
9. Waiting on a benchmark — park on an external run without holding a worker.
10. Plan-as-data at scale — the planner emits a dependency graph of experiments the engine runs and re-optimizes, served over the control plane and HTTP.

Each chapter links to a runnable file under `examples/tutorial/`. Start with [chapter 1](01-first-graph.md).

Requires `agent-workflow-sdk >= 0.6.0` (`by_key`, `plan_executor`, and the token budget used in the later chapters land in 0.6.0).
