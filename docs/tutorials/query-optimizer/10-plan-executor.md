# 10. Plan-as-data at scale

The finale. The planner stops proposing one index at a time and emits a dependency graph of experiments as data: measure every single index first, then — building on the best single — measure the pairs that extend it. `add_plan_executor` runs them by readiness, scoring each with the real SQLite scorer, and the whole optimizer runs as a background job on the control plane.

Runnable file: `examples/tutorial/queryopt/ch10_plan_executor.py`.

## The planner emits a DAG of experiments

```python
async def planner(state, ctx):
    waves = state.get("waves", 0)
    moves = list(_MOVES.values())
    if waves == 0:
        tasks = [Task(f"single:{i}", "measure", input={"indexes": [m]})
                 for i, m in enumerate(moves)]
        return {"plan": {"tasks": tasks}, "waves": 1}
    if waves == 1:
        best_single = max(singles, key=singles.get)         # from wave-1 results
        pairs = [Task(f"pair:{best_idx}+{i}", "measure",
                      input={"indexes": sorted([best_single[0], m])},
                      prerequisites=(f"single:{best_idx}", f"single:{i}"))
                 for i, m in enumerate(moves) if m != best_single[0]]
        return {"plan": {"tasks": pairs}, "waves": 2}
    return {"waves": waves + 1}
```

Each pair task declares prerequisites on its singles — so the executor will not run a pair until both its singles are measured. The handler scores one experiment with the real scorer:

```python
async def measure(task, state, ctx) -> TaskResult:
    indexes = tuple(task.input["indexes"])
    score = await scorer(Candidate(indexes=indexes), problem)
    return TaskResult(task.id, output={"indexes": list(indexes), "value": score.value})
```

```python
add_plan_executor(g, handlers={"measure": measure}, on_idle="planner", max_fanout=6)
g.add_conditional_edges("planner", planner_route, {"run": "dispatch", "stop": "summarize"})
```

## Running on the control plane

The optimizer is now a graph like any other, so it runs as a background job: register it by name, enqueue a run, let a worker drain it.

```python
registry = GraphRegistry()
registry.register("optimize", lambda: build(problem, checkpointer))
queue = MemoryRunQueue()
run = await queue.enqueue("optimize", {"waves": 0})
worker = Worker(queue, registry)
while await worker.run_once():
    pass
```

```text
enqueued run e452aceb on the control plane
planner wave 1: 6 single-index experiments
planner wave 2: 5 pair experiments extending the best single
best measured: 2 indexes, value -8.0 (from 11 experiments)
run status: succeeded
```

In production two things change and nothing else: the queue becomes a `PostgresRunQueue` shared by a pool of workers (a crashed worker's run is re-claimed; because the plan is checkpointed data, a resumed worker recomputes the runnable frontier and loses no experiment), and an HTTP server (`agentflow.server.create_app`) accepts the submission over the network — `POST /runs` to enqueue, `GET /runs/{id}` to watch progress. See the [control-plane HTTP guide](../../durability/control-plane.md#over-http).

## What you built

Across ten chapters, one system grew from a single node scoring one candidate into a durable, distributed query optimizer that:

- proposes index sets with a local Codex agent (or an LLM) but verifies every one with a deterministic SQLite measure (chapter 5), so a change of backend never changes the score;
- scores candidates concurrently (chapter 3) over a structured state separating the fixed problem from the moving search (chapter 4);
- gates promotion (chapter 6), pauses for human approval before applying indexes (chapter 7), survives a restart and caps token spend (chapter 8), and confirms the winner on a real benchmark without holding a worker (chapter 9);
- and finally emits its own dependency graph of experiments, run and re-planned on the control plane (chapter 10).

This is combinatorial optimization with a real measure: known objective (a cheaper plan returning the same rows), unknown path (which of exponentially many index sets and rewrites), expensive-to-evaluate candidates — exactly where an agent that reads the schema and proposes meaningful moves earns its place over blind enumeration, and exactly where the SDK's durability, concurrency, and backend-independent measure matter.

## Where to go next

- The [scoring](../../reference/api/scoring.md) and [plan executor](../../reference/api/plan-executor.md) references for the primitives this leans on.
- The [token budget](../../reference/api/budget.md) reference for capping LLM spend.
- The [control plane](../../durability/control-plane.md) chapter for running this across a worker pool and over HTTP.
