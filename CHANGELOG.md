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
