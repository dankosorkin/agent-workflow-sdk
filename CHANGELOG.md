# Changelog

All notable changes to `agentic-workflow-sdk` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
project aims to follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed (breaking)

- Agent backends now require an explicit `permission` policy — `AllowAll` is no
  longer the default. Construct with `permission=AllowAll()` (trusted local
  sandbox), `DenyAll()`, `Interactive()`, or `ToolAllowlist(...)`. Migration:
  add `permission=...` to every `KiroBackend`/`CodexBackend`/`ClaudeCodeBackend`
  call.

### Added

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
- Release artifacts: `LICENSE` (MIT, SPDX metadata, shipped in the wheel), real
  project URLs, `CONTRIBUTING.md` (with a release process), and `SECURITY.md`
  (reporting + security-relevant design notes).
- `OtelHooks` (in the `otel` extra): an OpenTelemetry `Hooks` exporter emitting
  a span per run and per node, recording errors and backend events, with a
  versioned attribute schema (`EVENT_SCHEMA_VERSION`).
- Backends are async context managers (`async with backend: ...`) so
  `start()`/`close()` can't be skipped and `close()` runs on error. Kiro's
  stdin writes now apply backpressure via `drain()`.
- Synchronous facade on `CompiledGraph`: `invoke_sync`, `resume_sync`, and
  `stream_sync` wrap `asyncio.run` for non-async callers, and refuse to run
  inside an existing event loop rather than deadlock.
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

## [0.1.0]

First working version: an async-first, LangGraph-style SDK for agent
workflows with pluggable agent and LLM backends.

### Added

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

[Unreleased]: https://example.com/agentic-workflow-sdk/compare/v0.1.0...HEAD
[0.1.0]: https://example.com/agentic-workflow-sdk/releases/tag/v0.1.0
