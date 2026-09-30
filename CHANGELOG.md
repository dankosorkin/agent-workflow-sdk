# Changelog

All notable changes to `agent-workflow-sdk` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
project aims to follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Quality gates (`agentflow.prebuilt`): `add_quality_gate(g, name, evaluator,
  *, on_pass, on_fail, on_escalate, max_attempts=3)` adds a checked
  accept/revise/escalate checkpoint built from a conditional edge and a
  per-gate attempt counter. An evaluator returns a `GateResult(passed, score,
  reasons, feedback)`; mix `GateState` into a schema for the `gate_attempts`
  and `gate_results` channels. All three routes are required so a gate never
  silently ships a failing artifact once attempts run out.
- `GateEvent` (`agentflow.events`): a gate emits `started` / `passed` /
  `failed` / `escalated` via `ctx.emit`, so `RunMetrics`, `JsonlTelemetry`, and
  the run stream show every gate decision. `Context.emit` now accepts a
  backend event or a `GateEvent`.
- Idempotency helpers (`agentflow.prebuilt`): `skip_if_done(node, store, *,
  namespace, key_from, ttl=None)` runs a node once per distinct key and serves
  the cached update from a `Store` thereafter; `artifact_key(value)` is a
  stable content hash to key it by.
- Structured interrupt-payload convention (`agentflow.interrupts`):
  `QualityGateReview` and `ClarificationRequest` (each with `.to_payload()`
  carrying a `type` discriminator) plus `interrupt_type(payload)`, generalizing
  the `PermissionRequest` shape so a UI or control plane can render any pause.
- Docs: "Quality gates" and "Long-running loops" chapters, a generated API
  reference (mkdocstrings), and the `examples/quality_gate.py` and
  `examples/research_plan_implement_review.py` examples.

- Effect-level idempotency (`agentflow.prebuilt`): `IdempotentOp(store,
  namespace)` protects one side effect inside a node with a claim → run →
  commit marker in a `Store`, so a resume or retry does not repeat it. Its
  `on_incomplete` policy (`"error"` default, `"rerun"`, `"skip"`) handles a
  prior attempt left `in_flight`, and `on_error` (`"keep"` default,
  `"release"`) controls the marker when `fn` raises — because an exception does
  not prove the effect was skipped (a lost response after a successful `POST`).
  `effect_key(*parts)` builds a stable token to pass downstream as the
  provider's idempotency key. New `IncompleteEffectError`. This narrows the
  duplicate window and makes every ambiguous case explicit, but does not close
  it: exactly-once ultimately needs idempotency at the effect (the claim is a
  marker, not an atomic lock; concurrent writers must rely on downstream
  keying). Checkpoints alone give only at-least-once, since a super-step is
  atomic over state only.

### Fixed

- `import agentflow.prebuilt` no longer requires the `ollama`/`httpx` extra.
  `httpx` is now imported lazily inside `open_stream`, so the `backends`
  package (and `RetryPolicy`) and the prebuilt patterns load dependency-free;
  `httpx` is pulled in only when an HTTP stream is actually opened by a
  concrete LLM backend.

## [0.1.1] - 2026-09-29

### Added

- `Callback(async_fn)` permission policy: asks a human (or any resolver) inline
  within the same turn and answers the agent's request immediately, without an
  engine interrupt/resume. This is the correct policy for a persistent-session
  backend like Kiro — `Interactive` suspends the run and resumes a new turn,
  which deadlocks a live ACP session that is holding the original turn open
  awaiting the decision. `Interactive` remains right for one-shot backends.
- Permission policy is now translated to CLI launch flags for the one-shot
  agents (best-effort upfront enforcement, since they have no per-tool runtime
  gate): `CodexBackend` derives `--sandbox` (`AllowAll` → `workspace-write`,
  `DenyAll`/`ToolAllowlist` → `read-only`); `ClaudeCodeBackend` derives
  `--allowed-tools` from a `ToolAllowlist` and `--permission-mode plan` from
  `DenyAll`. An explicit `sandbox=`/`allowed_tools=`/`permission_mode=` you pass
  always wins over the derived value.

### Changed

- `CodexBackend(sandbox=...)` now defaults to `None` (derive from the policy)
  instead of `"read-only"`. With `permission=AllowAll()` this means Codex now
  launches `workspace-write` rather than `read-only`; pass `sandbox="read-only"`
  explicitly to keep the old behavior.

### Fixed

- `KiroBackend` now extracts the tool name from the v3 permission request
  (`_meta.kiro.toolId` / `consent.capability` / `toolCall.title`), so a
  `PermissionRequest` no longer arrives with an empty `tool`.
- `KiroBackend.close()` returns promptly (short per-phase grace, configurable
  via `close_grace`) and signals the child's whole process group, so the v3
  CLI's helper process (`kiro-cli-chat`) is reaped instead of stranding the
  terminal.

## [0.1.0] - 2026-09-29

First public release: an async-first, LangGraph-style SDK for agent workflows
with pluggable agent and LLM backends.

### Added

- Agent backends require an explicit `permission` policy — there is no
  auto-approve default. Construct with `permission=AllowAll()` (trusted local
  sandbox only), `DenyAll()`, `Interactive()`, or `ToolAllowlist(...)`.
- Control-plane health/monitoring surface: `RunQueue.stats()` returns a
  `QueueStats` (queue depth by status plus `expired_leases`, the running runs
  whose lease is past due and await re-claim), and `WorkerPool.health()` returns
  a `PoolHealth` (`workers`/`alive`/`busy`/`idle`, with `healthy` True when every
  worker loop is alive). `Worker.busy` reports whether a worker is mid-run. Plain
  values for the caller to expose via `/healthz` / `/metrics` (no HTTP layer
  imposed). Implemented on both `MemoryRunQueue` and `PostgresRunQueue`.
- `PrometheusHooks` (in the `prometheus` extra): a `Hooks` listener that records
  run/node/step counters, a node-duration histogram, and backend-event counts as
  Prometheus metrics, for operators who scrape Prometheus rather than run an OTel
  collector. Uses a private `CollectorRegistry` by default; `exposition()`
  renders the text format for a `/metrics` endpoint (the HTTP layer is the
  caller's).
- `ToolAllowlist` permission policy for least-privilege tool scoping, with a
  configurable fallback (`DenyAll` by default).
- Capability matrix in the README documenting which backends route tool
  requests through `PermissionPolicy` (Kiro does; the one-shot CLI agents use
  their own sandbox/flags).
- Sensitive-data controls for persistence: `RedactKeys` redactor (mask values
  by key fragment). `JsonlTelemetry(redact=...)` and `FileCheckpointer(
  redact=...)` apply it before writing. Both create files/dirs owner-only
  (0o600/0o700) by default (`secure_permissions=`). Checkpoint redaction is
  opt-in and documented as non-resumable (masked values are lost).
- `SqliteCheckpointer`: transactional durable checkpointer with atomic
  per-`(thread, step)` revisions (WAL mode, upsert-with-revision-bump), safe
  for concurrent/multi-process runners. `FileCheckpointer` is now documented as
  single-writer/single-process.
- Provider resilience: typed `BackendTransportError` now carries `status` and
  `headers`; a 429 that outlives retries raises `BackendRateLimitError` with
  `retry_after`. HTTP LLM backends accept `max_concurrency` to bound in-flight
  requests, and `compile(max_node_concurrency=...)` bounds how many nodes run
  concurrently within a super-step (limits fan-out request pressure).
- State isolation: `compile(isolate_state="fanout"|"always"|"never")` (default
  `"fanout"`) deep-copies a node's input on concurrent super-steps so an
  in-place mutation of a shared nested container cannot race a sibling. Linear
  graphs pay no copy cost.
- `tool_loop` now sets a terminal `status` on the final state: `"completed"`
  (model answered tool-free) or `"tool_calls_unresolved"` (hit `max_turns` with
  pending tool calls). Callers must check it rather than assume the last
  message is a final answer.
- Tooling: Ruff (lint + format), mypy (type check, `agentflow` clean), and
  pip-audit added to the `dev` extra and CI. Codebase formatted and typed to
  zero findings.
- Release artifacts: `LICENSE` (Apache-2.0) and `NOTICE`, SPDX metadata,
  shipped in the wheel; real
  project URLs, `CONTRIBUTING.md` (with a release process), and `SECURITY.md`
  (reporting + security-relevant design notes).
- `DESIGN.md`: architecture and the invariants/contracts a change must
  preserve (dependency direction, execution model, checkpointer/store/control-
  plane contracts, security posture, out-of-scope). Referenced from
  `CONTRIBUTING.md` and the README layout.
- `OtelHooks` (in the `otel` extra): an OpenTelemetry `Hooks` exporter emitting
  a span per run and per node, recording errors and backend events, with a
  versioned attribute schema (`EVENT_SCHEMA_VERSION`).
- Backends are async context managers (`async with backend: ...`) so
  `start()`/`close()` can't be skipped and `close()` runs on error. Kiro's
  stdin writes now apply backpressure via `drain()`.
- Synchronous facade on `CompiledGraph`: `invoke_sync`, `resume_sync`, and
  `stream_sync` wrap `asyncio.run` for non-async callers, and refuse to run
  inside an existing event loop rather than deadlock.
- Control plane for managing runs outside the process that created them
  (`agentflow.controlplane`). A `RunQueue` holds run requests; a `GraphRegistry`
  maps a graph name to a compiled-graph factory (graphs are code, not data);
  and a `Worker`/`WorkerPool` claims requests and executes them, so runs
  survive a process restart and scale across workers. Run lifecycle:
  `queued -> running -> succeeded | interrupted | failed | cancelled`, with
  interrupted runs resumed via `enqueue_resume(run_id, value)` and cooperative
  `request_cancel` (stops at a super-step boundary). Ships `MemoryRunQueue`
  (single process) and `PostgresRunQueue` (in the `postgres` extra; `claim` uses
  `FOR UPDATE SKIP LOCKED` for safe concurrent workers, with a lease/heartbeat
  so a crashed worker's run is re-claimed). Also adds `Checkpointer.list_threads`
  (+ `ThreadInfo`) to enumerate runs across all backends. An HTTP layer over
  the queue is planned but not included.
- `Store` protocol for durable cross-thread memory (distinct from a
  checkpointer, which persists one thread's execution state). Items are
  addressed by a `namespace` tuple and a string `key`, hold any JSON value, and
  support an optional per-item TTL. Methods: `get`, `put`, `delete`, `search`
  (by namespace prefix) and `list_namespaces`. Ships `MemoryStore` (ephemeral)
  and `PostgresStore` (in the `postgres` extra; `text[]` namespace column for
  native prefix search, `jsonb` value, `timestamptz` expiry filtered from every
  read). Lazily exported so the core stays dependency-free.
- `PostgresCheckpointer` (in the `postgres` extra, via `asyncpg`): durable
  checkpoints in a Postgres table keyed by `(thread, step)` with a `revision`
  column and `jsonb` payload. Transactional upsert bumps the revision
  atomically; conditional writes check the stored revision under `FOR UPDATE`.
  Postgres is the recommended production-durability default (a committed row
  survives a crash without extra tuning). Lazily exported so the core stays
  dependency-free.
- Optimistic concurrency for checkpointers: `Checkpoint` gains a `revision`
  field (write count per `(thread, step)`, set by the store on read), and
  `Checkpointer.put(cp, if_revision=...)` performs a compare-and-set — a
  mismatch raises the new `CheckpointConflict`. `None` (default) stays an
  unconditional upsert, so existing `put(cp)` calls are unchanged. This lets
  two resumes racing the same super-step fail loudly instead of silently
  clobbering each other. Implemented across the Memory, Sqlite, File, Redis and
  Postgres backends.
- Retention on the `Checkpointer` protocol: `delete_thread(thread)` removes a
  whole thread, and `prune(thread, before_step=..., older_than=...)` deletes
  old checkpoints and returns the count removed. Implemented across all
  backends.
- `RedisCheckpointer` (in the `redis` extra): durable checkpoints on a Redis
  server for distributed / multi-process runners — JSON value per step plus a
  per-thread sorted-set step index, atomic pipeline writes. Shared checkpoint
  serialization extracted to `checkpoint/_serde.py`. Lazily exported so the
  core stays redis-free. Note: Redis is not crash-durable without AOF/RDB
  persistence configured; prefer `PostgresCheckpointer` when durability matters.

- HTTP retry/backoff for the httpx LLM backends (`OllamaBackend`,
  `OpenAIBackend`, `AnthropicBackend`) via a shared `RetryPolicy` and
  `open_stream` helper. Retries the connect/initial-response phase only (never
  mid-stream), honors `Retry-After`, and retries transient statuses
  (408/409/425/429/500/502/503/504) and transport errors. Configurable per
  backend through the `retry=` argument.
- Persistent telemetry: `JsonlTelemetry` (durable JSON Lines per run) and
  `MultiHooks` (compose several listeners); `Hooks.on_event` surfaces backend
  events emitted via `ctx.emit` to telemetry.
- Coverage: `pytest-cov` dev dependency and `[tool.coverage]` config; opt-in
  via `pytest --cov`. CI reports coverage.
- `CHANGELOG.md`.

Core engine (the foundation the above builds on):

- Graph engine: `Graph` builder (`add_node`, `add_edge`,
  `add_conditional_edges`), `START`/`END` sentinels, compile-time validation
  (reachability, unknown targets, dead ends), and a `CompiledGraph` runnable
  with `invoke` / `stream` / `resume` / `get_state` / `history`.
- Typed state with per-key reducers (`last`, `append`, `add`, `merge`,
  `union`) over a `TypedDict` schema; deterministic update merge.
- Super-step execution: concurrent fan-out, deterministic reducer folding,
  all-or-nothing step semantics, and a `step_limit` guard.
- Whole-run timeout (`invoke(..., timeout=...)` → `RunTimeout`) with the last
  checkpoint preserved for resume.
- Input validation at run start (undeclared/reserved channels rejected).
- Subgraph-as-node: `add_node(compiled_graph)` and `add_subgraph(...,
  input_map=, output_map=)` with isolated checkpoint sub-threads.
- Checkpointing: `Checkpointer` protocol, `MemoryCheckpointer`, durable
  `FileCheckpointer`; resume and time-travel.
- Human-in-the-loop: `ctx.interrupt(...)` suspends a run; `resume(value=...)`
  continues it. `Interactive` permission policy drives real interrupts.
- Backends behind one event stream. Agent: `KiroBackend` (persistent ACP
  session), `CodexBackend`, `ClaudeCodeBackend` (one-shot CLI per turn). LLM:
  `OllamaBackend`, `OpenAIBackend` (any OpenAI-compatible endpoint),
  `AnthropicBackend` (Messages API). Permission policies `AllowAll`/`DenyAll`/
  `Interactive`.
- Prebuilt patterns: `iterate_until_converged`, `tool_loop` (graph-native LLM
  function-calling), and `with_retry` / `with_timeout` node wrappers.
- Observability: `Hooks` lifecycle callbacks and `RunMetrics`.
- Packaging: `py.typed`, dependency-free core with optional `[ollama]` extra,
  GitHub Actions CI (offline suite + wheel build), and a `live` pytest marker
  separating opt-in real-backend smoke tests from the hermetic default run.

[0.1.1]: https://github.com/dankosorkin/agent-workflow-sdk/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/dankosorkin/agent-workflow-sdk/releases/tag/v0.1.0
