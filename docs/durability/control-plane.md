# Control plane

`invoke`, `stream`, and `resume` run a graph inline in your code. The control plane lets you manage runs from outside that process: enqueue a run, let a pool of workers execute it, and list, cancel, or resume it from anywhere sharing the queue. Runs survive a process restart and scale across workers. This chapter covers the pieces and how they fit.

## What it is, and is not

The control plane is a library layer first. What you get is the durable machinery: a queue, a registry, and workers, all as plain objects you drive. An optional HTTP layer (the `server` extra) wraps the queue so you can drive it over the network — covered at the end of this chapter. There is still no auth or tenancy anywhere in it; those are properties of a service you put in front, and stay out of scope.

## The pieces

- `GraphRegistry` maps a graph name to a compiled-graph factory. Graphs are code, not data, so a run references a graph by name; the enqueuer and its workers share an equivalent registry.
- `RunQueue` holds run requests and hands them to workers under a lease.
- `Worker` / `WorkerPool` claim requests and execute them.

They share one checkpointer so that whichever worker picks up a run sees its state.

## Wiring it up

```python
from agentflow import GraphRegistry, MemoryRunQueue, Worker, FileCheckpointer

cp = FileCheckpointer(".runs")

registry = GraphRegistry()
registry.register("summarize", lambda: build_graph().compile(checkpointer=cp))

queue = MemoryRunQueue()

# Enqueue a run (from anywhere sharing the queue).
run = await queue.enqueue("summarize", {"text": "..."})

# A worker — usually a separate long-running process — drains the queue.
worker = Worker(queue, registry)
await worker.run_once()        # claim and execute one run
# or: await worker.run_forever()

record = await queue.get(run.run_id)   # status, error, timestamps
```

The registry maps the name `"summarize"` to a factory that compiles the graph with the shared checkpointer. The enqueuer only needs the name and the input; the worker resolves the name to real code.

## Run lifecycle

A run moves through these statuses:

```mermaid
stateDiagram-v2
    [*] --> queued
    queued --> running: worker claims
    running --> succeeded
    running --> failed
    running --> interrupted
    queued --> cancelled
    running --> cancelled
    interrupted --> queued: enqueue_resume(value)
```

- `queued` → `running` when a worker claims it.
- `running` → `succeeded` / `failed` / `interrupted` when the graph finishes, errors, or hits `ctx.interrupt`.
- An `interrupted` run returns to `queued` via `enqueue_resume`.
- A `queued` or `running` run can be `cancelled`.

## Resuming an interrupted run

A graph that interrupts for a human suspends the run as `interrupted`. Re-queue it with the answer and a worker resumes it:

```python
await queue.enqueue_resume(run.run_id, value="approved")   # back to queued
```

The worker detects that the thread sits on an interrupted checkpoint and resumes rather than starting fresh, feeding your value to the waiting node.

## Cancellation

Cancellation is cooperative. `request_cancel` stops a running graph at the next super-step boundary, leaving a consistent checkpoint — never mid-step.

```python
cancelled = await queue.request_cancel(run.run_id)   # True unless already terminal
```

A queued run is cancelled immediately; a running one is flagged, and the worker stops it cleanly at the next boundary.

## Leases and crash recovery

A worker claims a run under a time-bounded lease and renews it with a heartbeat while working. If the worker crashes, its lease expires and another worker re-claims the run. No run is stranded by a dead worker for longer than a lease.

`PostgresRunQueue` claims via `FOR UPDATE SKIP LOCKED`, so under many concurrent workers each run goes to exactly one worker. `MemoryRunQueue` is for a single process and tests.

## Monitoring

Two snapshots let you build a `/healthz` and `/metrics` without any bundled server.

```python
stats = await queue.stats()          # QueueStats
stats.queued, stats.running          # depth by status
stats.expired_leases                 # running runs past their lease — stranded by a crash

pool = WorkerPool(queue, registry, concurrency=4)
await pool.start()
health = pool.health()               # PoolHealth
health.workers, health.alive, health.busy, health.idle
if not health.healthy:
    ...                              # a worker loop died — pool is degraded
```

A rising `expired_leases` is the signal that workers are dying or falling behind their heartbeat. `health.healthy` is False when any configured worker loop is not alive — a readiness signal you can expose directly.

## Over HTTP

Everything so far drives the queue from Python. The optional `server` extra wraps the same queue in a FastAPI app, so you can enqueue and observe runs over the network — from a different language, a web UI, or a `curl` in a shell.

```bash
pip install 'agent-workflow-sdk[server]'
```

`create_app` takes a `RunQueue` and returns an ASGI app. It adds no behavior of its own; each route forwards to one queue method.

```python
from agentflow import PostgresRunQueue
from agentflow.server import create_app

queue = PostgresRunQueue(dsn)
app = create_app(queue)        # an ASGI app
```

Serve it with any ASGI server. `uvicorn` is a common choice and you install it yourself:

```bash
uvicorn myapp:app --port 8000
```

The routes map 1:1 onto the queue methods from earlier in this chapter:

- `POST /runs` enqueues a run. Body: `{"graph": "...", "input": {...}, "thread": "..."}` (thread optional). Returns `201` with the run record.
- `GET /runs/{id}` returns one run record, or `404` if there is no such run.
- `GET /runs` lists runs. Query params: `status`, `graph`, `limit` (1–1000), `offset`.
- `POST /runs/{id}/resume` re-queues an interrupted run. Body: `{"value": ...}`. `404` if there is no such run.
- `POST /runs/{id}/cancel` requests cancellation. Returns `{"cancelled": true}` unless the run was unknown or already terminal.
- `GET /stats` returns the `QueueStats` snapshot (`total`, `by_status`, `expired_leases`).
- `GET /healthz` returns `{"status": "ok"}`, plus a `pool` block when you pass `pool_health`.

Because the models are typed, FastAPI serves interactive OpenAPI docs at `/docs` and the raw schema at `/openapi.json` with no extra work.

A quick enqueue over HTTP:

```bash
curl -X POST localhost:8000/runs \
  -H 'content-type: application/json' \
  -d '{"graph": "summarize", "input": {"text": "..."}}'
```

Wire the pool's health into `/healthz` by passing the callable:

```python
pool = WorkerPool(queue, registry, concurrency=4)
app = create_app(queue, pool_health=lambda: pool.health())
```

### What it does not expose

The worker protocol — `claim`, `heartbeat`, `complete` — is deliberately absent from the HTTP surface. Those methods mutate run state and belong to workers, which talk to the queue directly (in-process, or via the same `PostgresRunQueue`), not over this API. The HTTP layer is the enqueue-and-observe plane only.

### Trust boundary

There is no authentication, authorization, rate limiting, or multi-tenancy in this app. It is an adapter over the queue, not a hardened public endpoint. Run it inside your own trust boundary, or behind a reverse proxy or gateway that supplies those. Anyone who can reach it can enqueue, cancel, and resume any run.

See the [HTTP server API reference](../reference/api/server.md) for the request and response models.

## When to use the control plane

- Use inline `invoke`/`stream` when the run belongs to the calling process — a request handler, a script, a test.
- Use the control plane when runs must outlive the enqueuer, scale across worker processes, or be managed (listed, cancelled, resumed) from elsewhere.

Next: [observability](observability.md).
