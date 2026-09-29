# Design

This document records the architecture and the contracts that the rest of the
codebase depends on. The README shows how to use the SDK; this explains what
must stay true as it changes. If a change would break an invariant here, that
is a design decision, not a refactor.

The import root is `agentflow`. The distribution name is `agent-workflow-sdk`.

## Dependency direction

The one hard rule: `state` to `engine` to `backends`, and never the reverse.

- The core (`agentflow/` minus `backends/`) is dependency-free — standard
  library only. Importing `agentflow` must not pull in `httpx`, `redis`,
  `asyncpg`, `prometheus_client`, or `opentelemetry`.
- The engine (`graph`, `runtime`, `compiled`, `state`) never imports a backend.
- A backend imports only `events` and `errors` from the core, never `runtime`
  or `graph`.
- Every provider library lives behind an optional extra and is imported lazily,
  so the class is only resolved when you actually reference it.

This boundary is what lets the core run with zero third-party packages and lets
a backend be swapped without touching workflow code.

## State and reducers

State is a set of named channels declared as a `TypedDict` subclass of `State`.
Each channel has a reducer — a pure function `(current, update) -> new` — that
folds a node's partial update into the current value.

- A field annotated `Annotated[type, reducer]` uses that reducer. A bare field
  uses `last` (last-value-wins), the default.
- Built-in reducers: `last`, `append`, `add`, `merge`, `union`. A reducer must
  be pure and must treat a `None` current as its empty value (empty list, 0,
  empty dict/set).
- Reducers are what make concurrent updates in one super-step merge
  deterministically instead of racing on last-writer-wins.
- Channel names starting with `__` and the names in `RESERVED_NAMES`
  (`__step`, `__next`) belong to the engine. A schema or a node update that
  writes one is rejected.
- Nodes may only write declared channels. Writing an undeclared channel raises
  rather than sitting silently in state — a typo fails fast.

`State` is pure data: no async, no I/O, no engine imports. It can be understood
and tested in isolation.

## Execution model

A run advances in super-steps. Each super-step runs the current frontier of
nodes, folds their updates into state, computes the next frontier from the
edges, and checkpoints — then the next super-step begins.

Invariants that must hold:

- Super-steps are all-or-nothing. If any node in a step raises, the whole
  step's updates are discarded and no checkpoint is written for that step. The
  last good checkpoint is the previous step, so a resume never sees a partial
  step.
- Updates within a step are folded in a deterministic node order, so
  non-commutative reducers (`append`, `last`) are well-defined. Commutative
  reducers (`add`, `union`) do not depend on it.
- `step_limit` bounds total super-steps to catch a graph that never reaches
  `END`.
- A whole-run timeout cancels the in-flight step and raises `RunTimeout`; the
  last completed step's checkpoint is intact, so a checkpointed run resumes.

### State isolation

`compile(isolate_state=...)` controls defensive copying of a node's input:

- `"fanout"` (default) deep-copies a node's input only when more than one node
  runs in the same super-step, so an in-place mutation of a shared nested
  container cannot race a sibling. Linear graphs pay no copy cost.
- `"always"` copies on every node; `"never"` never copies (fastest, but the
  caller promises nodes do not mutate shared input in place).

## Backends

Two kinds of backend share one event vocabulary, so a node consumes either the
same way — a stream of events ending in `TurnEnd`.

- Agent backends (`AgentBackend`) drive a session that runs its own tools and
  asks permission. Lifecycle is `start()` / `prompt(text)` / `close()`, also
  usable as an async context manager. `KiroBackend` holds a long-lived
  JSON-RPC (ACP) session; `CodexBackend` and `ClaudeCodeBackend` run a fresh
  subprocess per turn and carry a resumable session id between turns.
- LLM backends (`LLMBackend`) are stateless: messages in, token stream out.
  They never run tools themselves — a tool call is a request the graph
  fulfils.

Contracts a backend must honor:

- Import only `events` and `errors` from the core.
- Subprocess backends use argv lists (never `shell=True`), so a prompt can
  never be interpreted as a shell command.
- Transport failures raise typed errors (`BackendTransportError`, and
  `BackendRateLimitError` for a 429 that outlives retries). Retries cover the
  connect / initial-response phase only, never a partially received stream.

## Permissions

Authorization is explicit. Agent backends require a `PermissionPolicy` — there
is no auto-approve default. `AllowAll` is for a trusted local sandbox only;
other policies are `DenyAll`, `Interactive`, and `ToolAllowlist`. Only Kiro
routes tool requests through the policy; the one-shot CLI agents rely on their
own sandbox flags (see the capability matrix in the README).

## Checkpointing and durability

A `Checkpointer` persists the execution state of one thread so a run can
resume, be inspected, or fork (time-travel). It is keyed by `(thread, step)`.

Contract:

- `put(cp, if_revision=...)` — `Checkpoint` carries a `revision` (the write
  count for its `(thread, step)`, set on read). Passing `if_revision` makes the
  write a compare-and-set; a mismatch raises `CheckpointConflict`. `None`
  (default) is an unconditional upsert, so plain `put(cp)` is unchanged. This
  gives optimistic concurrency for two resumes racing the same super-step.
- `get` / `history` — read one step or all steps of a thread.
- `delete_thread` / `prune(before_step=, older_than=)` — retention.
- `list_threads` — enumerate threads with a `ThreadInfo` summary, the
  foundation the control plane builds on.

Implementations: `MemoryCheckpointer`, `FileCheckpointer` (single-writer,
atomic per-step files), `SqliteCheckpointer` (transactional, WAL),
`RedisCheckpointer`, `PostgresCheckpointer`. Serialization is shared in
`checkpoint/_serde.py` so the persistent backends never drift.

Durability note: Postgres is the recommended production default — a committed
row survives a crash without extra tuning. Redis is not crash-durable unless
AOF/RDB persistence is configured.

## Human-in-the-loop

A node calls `await ctx.interrupt(payload)` to suspend the run for a human. The
runtime writes an interrupted checkpoint and stops. `resume(thread, value=...)`
continues the run, and the same `interrupt` call returns `value`. Because the
frontier is persisted, this works across process restarts.

## Store

A `Store` is the other axis from a checkpointer: durable key-value data shared
across threads (profiles, facts, long-term memory), addressed by a `namespace`
tuple and a string `key`, holding any JSON value, with an optional per-item
TTL. Expired items never surface from `get` or `search`. Implementations:
`MemoryStore` and `PostgresStore`.

Keep this distinct from a checkpointer: a checkpointer is one thread's
execution state; a store is cross-thread application data.

## Control plane

The control plane manages runs from outside the process that created them. It
is a library layer, not a service — there is no HTTP server (that is planned as
a thin adapter over these methods).

- `GraphRegistry` maps a graph name to a compiled-graph factory. Graphs are
  code and cannot be serialized, so a run request references a graph by name;
  the enqueuer and its workers must share an equivalent registry and the same
  checkpointer.
- `RunQueue` holds run requests. Lifecycle: `queued` to `running` to
  `succeeded` / `interrupted` / `failed` / `cancelled`; an interrupted run
  returns to `queued` via `enqueue_resume`. Cancellation is cooperative —
  `request_cancel` stops a running graph at a super-step boundary, leaving a
  consistent checkpoint.
- `Worker` / `WorkerPool` claim requests under a time-bounded lease and execute
  them; a heartbeat renews the lease, and a crashed worker's run is re-claimed
  once its lease expires. `PostgresRunQueue` claims via
  `FOR UPDATE SKIP LOCKED`, so each run goes to exactly one worker under
  concurrency.
- Monitoring: `RunQueue.stats()` and `WorkerPool.health()` return plain values
  a caller can expose through its own `/healthz` and `/metrics`.

## Observability

`Hooks` are lifecycle callbacks attached at `compile(hooks=...)`. They are
awaited but must never raise — an exception in a hook is swallowed so
instrumentation cannot break a run. `RunMetrics`, `JsonlTelemetry`, `OtelHooks`
(otel extra), and `PrometheusHooks` (prometheus extra) are all `Hooks`
listeners; compose several with `MultiHooks`.

## Security posture

- Persistence is plaintext by default. Checkpoints and JSONL telemetry contain
  prompts, model output, and tool arguments. `RedactKeys` (via `redact=`) masks
  sensitive keys; files and directories are created owner-only (0o600/0o700)
  where the filesystem supports it. Redacted checkpoints are not resumable to
  the exact original state — masked values are lost.
- Postgres backends validate the table name with `str.isidentifier()` before
  interpolating it into SQL.
- Treat model and tool output and fetched content as untrusted. The SDK does
  not execute model output; nodes decide what to run.

See `SECURITY.md` for the reporting process and the operator-facing summary.

## What is intentionally out of scope

- An HTTP/gRPC API over the control plane, and the auth/multi-tenancy that a
  service layer implies. These are properties of a service, not this SDK, and
  are deferred to a future `server` layer that adapts the existing queue
  methods.
- Rate limiting and per-tenant quotas. `max_concurrency` /
  `max_node_concurrency` bound pressure within one process; per-tenant
  isolation is the caller's responsibility until the service layer exists.
