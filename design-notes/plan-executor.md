# Design note: `plan_executor` (plan-as-data DAG executor)

Status: draft for review. Not published docs — a working design note, like
`DESIGN.md`. Target: a new prebuilt in `agentflow/prebuilt/`.

## The gap this closes

The engine runs a static graph: nodes and edges are fixed at `compile`, and a
`conditional_edges` router may only fan out to targets already declared in its
mapping. That is a deliberate contract (it buys compile-time validation and a
durable resume from a known frontier).

What it does not give, out of the box, is a planner that emits a *dependency
graph of tasks* which an executor then runs by readiness — the "engine builds
and runs the execution plan as it goes" shape. Today a consumer writes that by
hand (see `examples/dual_state_search.py`): a static pool of worker nodes plus
a dispatch router that picks a runnable subset. `plan_executor` is that loop,
generalized and made durable, as a reusable prebuilt — path (a): plan-as-data,
no engine change.

## Non-goals (stated first, on purpose)

- Not dynamic graph mutation. The SDK graph stays static. The *task plan* is
  data in state; the executor is a fixed loop (dispatch -> run -> collect ->
  route) over a static pool of handlers. No node is added to the graph at
  runtime.
- Not a `Send`-style dynamic spawn primitive. That would touch the engine and
  raise checkpoint-serialization questions; explicitly out of scope here.
- Not a general scheduler. No priorities, no preemption, no cost-aware
  ordering beyond "dependencies satisfied". A consumer can layer those on by
  choosing which runnable tasks to dispatch.
- Not retrying/attempt policy per task beyond idempotency. Retries compose from
  the existing `with_retry` / `IdempotentOp`, not reimplemented here.

## The data model

Two frozen dataclasses, in the style of `Candidate` / `GateResult`.

```python
@dataclass(frozen=True)
class Task:
    id: str                              # unique within a plan; the dedup key
    kind: str                            # selects the handler
    input: Any = None                    # payload the handler reads
    prerequisites: tuple[str, ...] = ()  # task ids that must finish first

@dataclass(frozen=True)
class TaskResult:
    task_id: str
    output: Any = None
    failed: bool = False
    error: str | None = None
```

Why `id` is a plain string, not a UUID: it must be stable across a resume so a
re-dispatched task is recognized as the same one. The planner chooses it (e.g.
`"try_index:collection=orders"`), which doubles as a natural idempotency key.
A UUID generated at propose-time would differ on a replay and break dedup.

Why `kind` is separate from `id`: the executor routes a task to a handler by
`kind`, so the handler pool is a `dict[str, Handler]` fixed at build time. This
is what keeps the graph static — a new task is new *data*, never a new node.

## State: a `by_key` channel

The plan lives in one structured channel, folded by `by_key` — this is the
primitive `plan_executor` is built to exercise.

```python
class PlanState(State):
    plan: Annotated[dict, by_key({
        "tasks":    append,   # the planner's proposals accumulate
        "results":  append,   # handlers' TaskResults accumulate
        "done_ids": union,    # set of finished task ids (fast readiness check)
    })]
```

- `tasks` grows as the planner proposes; it is the backlog, not the frontier.
- `results` grows as handlers finish; one `TaskResult` per completed task.
- `done_ids` is a `union` set of completed ids, so "are prerequisites met?" is
  a set-subset check without scanning `results` each time.

The runnable frontier is a *derived* value, never stored: tasks whose
prerequisites are all in `done_ids` and whose own id is not yet in `done_ids`.
Deriving it (not storing it) is what makes resume trivial — the frontier is
recomputed from durable state every super-step.

## Shape: builder into your graph, not a sealed compile

Decision: `plan_executor` is a *builder* that adds nodes to a graph you own
(like `add_quality_gate`), not a function that returns a sealed compiled graph
(like `iterate_until_converged`).

Rationale: the whole point is that the plan is re-optimized by *your* planner,
which reads results and proposes more tasks. A sealed loop would hide the
planner. The consumer needs to wire their planner node and their routing (keep
going vs stop) around the executor. So the prebuilt supplies the mechanical
middle — dispatch, run, collect — and the consumer supplies planner + stop.

```python
add_plan_executor(
    g: Graph,
    *,
    handlers: Mapping[str, Handler],   # kind -> async (task, state, ctx) -> TaskResult
    dispatch: str = "dispatch",        # node name it creates
    collect: str = "collect",          # node name it creates
    on_idle: str,                      # where to go when no task is runnable
    max_fanout: int | None = None,     # cap tasks dispatched per super-step
) -> Graph
```

It creates two nodes and the wiring between them:

- `dispatch` reads state, computes the runnable frontier, and fans out — one
  branch per runnable task — onto an internal run step, bounded by `max_fanout`.
- the run step invokes `handlers[task.kind](task, state, ctx)` and returns the
  `TaskResult` into `plan.results` (+ the id into `plan.done_ids`).
- `collect` is the fan-in point after a batch; it routes back to `dispatch`
  while tasks remain runnable, or to `on_idle` when the frontier is empty.

The consumer's planner writes `plan.tasks`; `on_idle` is typically the planner
(propose more, given results so far) or a stop/END. That closes the
propose -> run -> observe -> re-plan loop, with the executor owning only the
run mechanics.

### The static-pool constraint, made honest

Fan-out in the engine goes to targets in a static mapping. So the executor
cannot literally create one graph node per task. Instead it fans out onto a
*single* run node that appears in the next frontier once, and that node
handles one task per invocation — but several runnable tasks must run in the
*same* super-step to be concurrent.

This is the one real design tension. Two ways to resolve it:

- Option R1 — N copies of the run node, fixed pool width. Declare `run_0 ..
  run_{W-1}` (W = `max_fanout`, default a small number like 8). `dispatch`
  assigns up to W runnable tasks to those slots this super-step via a router
  returning the slot keys; each slot reads its assigned task from state. True
  concurrency up to W, bounded and static. Width is a compile-time ceiling.
- Option R2 — one run node, sequential within a super-step. Simpler wiring, but
  the "batch" runs tasks one at a time, losing the concurrency that is the
  whole reason to fan out.

Recommendation: R1. The concurrency is the point (independent experiments run
in parallel), and a fixed slot ceiling is a reasonable, static, resumable
bound — it mirrors `max_node_concurrency` in spirit. R2 would make
`plan_executor` little more than a for-loop and not worth a primitive.

### How slots read their task (R1 detail)

`dispatch` returns a list of slot keys for the runnable tasks it is placing
this super-step, and writes the assignment (slot -> task id) into a small
`plan` field, e.g. `assigned: Annotated[dict, last]` (replaced each dispatch).
Each `run_i` reads `assigned[str(i)]`, looks the task up in `tasks` by id,
runs the handler, and clears nothing (the assignment is overwritten next
dispatch via `last`). Unused slots this round are simply not in the router's
returned key list, so they do not run.

Trade-off to accept: a slot assignment is transient state. It must be derived
fresh each dispatch and never trusted across a park/resume boundary — on
resume, `dispatch` recomputes from `done_ids`, so a crash mid-batch simply
re-derives the frontier and re-assigns. Combined with idempotent handlers (see
below), a task that was mid-flight at the crash is safely re-run.

## Durability and resume

The executor stores only durable facts (`tasks`, `results`, `done_ids`) in the
checkpointed `plan` channel. Everything operational (the frontier, slot
assignment) is derived. So resume is: load checkpoint, `dispatch` recomputes
the runnable frontier from `done_ids`, continue. No special resume path.

At-least-once, not exactly-once: if a worker crashes after a handler's side
effect but before the `collect` checkpoint, the task's id is not yet in
`done_ids`, so on resume it is dispatched again. This is the same contract as
the rest of the SDK. The executor does not add idempotency; it *enables* it by
making `task.id` the stable key a handler passes to `IdempotentOp` or a
downstream dedup token. Documented, not hidden.

## Termination and failure

- Plan exhausted: no runnable task and no task in flight -> `collect` routes to
  `on_idle`. The consumer's planner decides whether to propose more or stop.
- A task fails (`TaskResult.failed=True`): its id still goes into `done_ids`
  (it is "finished", just unsuccessfully), so dependents become... a decision:
  should a failed prerequisite block its dependents, or release them? Proposed
  default: a failed task blocks its dependents (they never become runnable),
  and `plan_executor` exposes `blocked_ids` as a derived value the planner can
  inspect. Alternative: release dependents and let them see the failed result.
  Needs a decision — see open questions.
- Deadlock guard: if there are unfinished tasks but none runnable (a cycle, or
  all remaining tasks depend on a failed one), `dispatch` detects "unfinished
  but empty frontier" and routes to `on_idle` with a derived `stalled=True`
  flag rather than spinning. The engine `step_limit` is the backstop.

## Open questions for review

1. R1 vs R2 for concurrency (recommend R1, fixed slot pool). Agree?
2. Failed-prerequisite policy: block dependents (default) vs release them.
3. Default `max_fanout` / slot width (propose 8).
4. Does `plan_executor` own a `by_key` `PlanState` mixin it exports (like
   `GateState`), or does the consumer declare the `plan` channel themselves?
   Proposed: export a `PlanState` mixin for ergonomics, mirroring `GateState`.
5. Handler signature: `(task, state, ctx)` vs `(task, ctx)`. Proposed include
   `state` so a handler can read problem facts (the deterministic domain
   graph) without threading them through every task input.
