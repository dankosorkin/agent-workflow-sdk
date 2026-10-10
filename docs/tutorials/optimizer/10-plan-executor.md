# 10. Plan-as-data at scale

The finale. Until now the optimizer proposed one move at a time. Real search is a graph of experiments with dependencies: measure the singles first, then the pairs that build on the best single, then triples, and so on. The engine graph is static, so we cannot add a node per experiment at runtime. Instead the planner emits the dependency graph as data, and `add_plan_executor` runs it by readiness — then the whole optimizer runs as a background job on the control plane.

Runnable file: `examples/tutorial/10_plan_executor.py`.

## The planner emits a dependency DAG

A plan is a set of `Task`s. Each task has an `id`, a `kind` (which handler runs it), an `input`, and `prerequisites` — the ids that must finish first. The planner writes tasks into the `plan` channel (from the `PlanState` mixin) and re-plans from results:

```python
async def planner(state, ctx):
    results = state.get("plan", {}).get("results", [])
    waves = state.get("waves", 0)
    if waves == 0:
        tasks = [Task(f"single:{s}", "measure", input={"plan": [s]}) for s in STRATEGY_FACTORS]
        return {"plan": {"tasks": tasks}, "waves": 1}
    if waves == 1:
        singles = {r["output"]["plan"][0]: r["output"]["cost"] for r in results}
        best = min(singles, key=singles.get)
        pairs = [
            Task(f"pair:{best}+{s}", "measure",
                 input={"plan": sorted([best, s])},
                 prerequisites=(f"single:{best}", f"single:{s}"))
            for s in STRATEGY_FACTORS if s != best
        ]
        return {"plan": {"tasks": pairs}, "waves": 2}
    return {"waves": waves + 1}
```

Wave 2's pair tasks declare prerequisites on their singles — so the executor will not run a pair until both its singles are measured.

## The executor runs it by readiness

```python
add_plan_executor(g, handlers={"measure": measure}, on_idle="planner", max_fanout=4)
g.add_conditional_edges("planner", planner_route, {"run": "dispatch", "stop": "summarize"})
g.add_edge(START, "planner")
```

A handler is `async (task, state, ctx) -> TaskResult`, selected by the task's `kind`. The executor computes the runnable frontier — tasks whose prerequisites are all done — assigns up to `max_fanout` of them to run slots that execute concurrently, collects their results, and loops. When nothing is runnable it routes to `on_idle` (here the planner), which observes results and proposes the next wave or stops. The planner owns the stop decision; the executor owns the running.

Read the three properties worth knowing in the [plan-as-data recipe](../../patterns/long-running-loops.md#recipe-plan-as-data-the-planner-emits-a-task-dag): the frontier is derived from state (so resume is free), a failed task blocks its dependents (`plan_stalled` tells you), and the plan is stored as JSON-serializable dicts (so a planner appends `Task(...)` objects but reads results back as dicts).

## Running it as a background job

The optimizer is now a graph like any other, so it runs on the control plane: register it by name, enqueue a run, and let a worker drain it.

```python
registry = GraphRegistry()
registry.register("optimize", lambda: make_graph(checkpointer))

queue = MemoryRunQueue()
run = await queue.enqueue("optimize", {"waves": 0})

worker = Worker(queue, registry)
while await worker.run_once():
    pass
```

```text
enqueued run 4ac5a3cc on the control plane
planner wave 1: 3 single-strategy experiments
planner wave 2: 2 pair experiments extending 'add_index'
best measured plan: ['add_index', 'push_filter'] cost 24.0 (from 5 experiments)
run status: succeeded
```

In production two things change, and nothing else:

- The queue is a `PostgresRunQueue`, shared by a pool of workers across machines. The same run can be picked up by whichever worker is free; a crashed worker's run is re-claimed once its lease expires. Because the plan is checkpointed data, a worker that resumes mid-plan recomputes the runnable frontier and continues — no experiment is lost or repeated.
- An HTTP server accepts the submission over the network. `agentflow.server.create_app(queue)` returns a FastAPI app exposing `POST /runs` to enqueue, `GET /runs/{id}` to watch progress, and `GET /stats` for queue depth. Submit a problem with a `curl`, poll for the result, and the optimizer runs as a service — see the [control-plane HTTP guide](../../durability/control-plane.md#over-http).

## What you built

Across ten chapters, one system grew from a single node scoring one plan into a durable, distributed optimizer that:

- proposes moves with a model but verifies them with deterministic code (chapters 5);
- runs independent experiments concurrently (chapter 3) over a structured state that separates fixed facts from the moving search (chapter 4);
- gates promotion with an attempt budget (chapter 6) and pauses for human approval (chapter 7);
- survives a restart and applies its result exactly once (chapter 8);
- parks on an external benchmark without holding a worker (chapter 9);
- and finally emits its own dependency graph of experiments, re-optimizing as results arrive, running as a background job on the control plane (chapter 10).

Every layer was a primitive the engine already had, composed — not a framework feature bolted on. That is the point of the SDK: a small set of composable parts (state and reducers, super-steps, checkpoints, interrupts, backends, and a handful of prebuilt patterns) is enough to build a system that would otherwise be a bespoke orchestration project.

## Where to go next

- The [control plane](../../durability/control-plane.md) chapter for running this across a worker pool and over HTTP.
- The [plan executor reference](../../reference/api/plan-executor.md) for the full `add_plan_executor` contract.
- The [patterns](../../patterns/long-running-loops.md) chapter for the other long-running shapes that compose the same way.
