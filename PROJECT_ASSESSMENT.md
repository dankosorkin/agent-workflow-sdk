# Project Assessment: `agentic-workflow-sdk` / `agentflow`

**Assessment date:** 2026-09-29
**Scope:** source, tests, examples, packaging metadata, CI configuration, and
Git history currently present in this checkout. This is a source review, not a
claim of production readiness or a current live-provider certification.

## Executive conclusion

`agentflow` is a well-focused early SDK with an unusually solid workflow-core
for its size. Its strongest differentiators are deterministic bulk-synchronous
graph execution, all-or-nothing super-steps, resumable interrupts, and a clean
separation between the dependency-free engine and provider adapters. The test
suite deliberately tests those invariants rather than only happy paths.

The project is ready for controlled local development and internal prototypes.
It is **not yet ready to be presented as a production-grade or public SDK**.
The main reasons are operational rather than algorithmic: permissive agent
authorization by default, unprotected persisted data, no cross-process
checkpoint coordination, incomplete provider resilience, and release/project
hygiene gaps.

## What the project is

The distribution is `agentic-workflow-sdk` (version `0.1.0`); the import
package is `agentflow`. It provides:

- typed state channels with reducer-driven update merging;
- graph nodes, static/conditional routing, fan-out, and subgraphs;
- an async bulk-synchronous scheduler with a step limit;
- memory and atomic-file checkpoints, interrupt/resume HITL flows;
- structured events, hooks, metrics, JSONL telemetry, and prebuilt loops;
- adapters for Kiro ACP, Codex CLI, Claude Code CLI, Ollama, OpenAI-compatible
  Chat Completions, and Anthropic Messages.

The core has no mandatory third-party runtime dependency. Provider HTTP support
is an optional `httpx` extra.

## Verified evidence

| Check | Result | Boundary |
| --- | --- | --- |
| Offline test suite | `92 passed, 8 deselected` | Verified locally with the existing Python 3.13 virtual environment. Live tests were intentionally not run. |
| Syntax compilation | `compileall` completed for `agentflow`, `examples`, and `tests` | Not type checking. |
| Test inventory | 100 tests collected; 8 marked `live` | The default configuration deselects live tests. |
| CI definition | Python 3.11 and 3.12, offline tests, isolated wheel/sdist build | Workflow was reviewed, not executed on GitHub in this assessment. |
| Local clean build | Not verified | Build isolation could not download `hatchling` because DNS/network access is unavailable; the local venv also lacks `hatchling`. A pre-existing wheel contains `agentflow/py.typed`, but it was not rebuilt now. |

## Strengths

### 1. Coherent architecture and dependency direction

The architecture described in `DESIGN.md` matches the implementation. State
and reducers are pure (`state.py`); graph construction and execution live in
`graph.py`, `compiled.py`, and `runtime.py`; backends are adapters rather than
engine dependencies; checkpointing and observability are optional services.
Importing `agentflow` does not import HTTP clients or spawn subprocesses.
That is an excellent library boundary and makes the core cheap to adopt and
easy to test.

### 2. Correctly defined super-step semantics

All frontier nodes receive the same pre-step state and run concurrently through
`asyncio.gather`. Their updates are folded in deterministic frontier order.
If any node fails, the entire step is discarded before state merging and before
checkpointing. Tests cover actual concurrent overlap, deterministic reducers,
failed fan-out atomicity, timeout behavior, and step limits. This is the
project's most valuable technical foundation.

### 3. Durable-resume model is designed into the runtime

Checkpoint records include state, pending frontier, parent, step, and interrupt
metadata. `FileCheckpointer` writes a temp file then `os.replace`, preventing
readers from seeing a partial individual JSON file. HITL re-runs the interrupted
node with the supplied answer, preserving a simple programming model. The
Kiro permission integration using `contextvars` is thoughtful and tested
end-to-end against a scripted ACP process.

### 4. Useful backend taxonomy

The distinction between session-owning coding agents and stateless LLM chat
models is technically sound. `AgentBackend` and `LLMBackend` share events but
do not pretend their responsibilities are identical. The tool loop keeps tool
execution in the graph instead of giving arbitrary tool execution to an LLM.
Provider-specific wire parsing is isolated in individual adapter modules.

### 5. Good test and documentation discipline

The repository has targeted tests for graph validation, input validation,
reducers, concurrency, checkpoint history, interrupts, subgraphs, retries,
telemetry, and each transport parser. Offline tests are hermetic; live tests
are opt-in. `README.md` provides runnable usage paths and `DESIGN.md` makes
v1 non-goals explicit. Recent commits show incremental hardening rather than
an unvalidated bulk rewrite.

### 6. Sensible subprocess hygiene

CLI backends use `asyncio.create_subprocess_exec` with argument vectors rather
than a shell. The Claude adapter deliberately strips inherited `ANTHROPIC_*`
variables unless key-based mode is requested. These are strong, concrete
safety choices.

## Risks, weaknesses, and improvements

Priorities are based on likely impact if users run real agents or persist real
workflow data.

| Priority | Finding | Why it matters | Recommended action |
| --- | --- | --- | --- |
| P0 | `AllowAll` is the default `PermissionPolicy` for agent backends. | An application that creates a Kiro backend without explicit policy can approve agent-requested tools automatically. This is unsafe for a general SDK default. | Make explicit policy selection mandatory, or default to deny/interactive. Add allowlists scoped by tool, path, network host, and run. Document a migration path. |
| P0 | Checkpoints and JSONL telemetry persist full states/events without redaction, encryption, retention, or restrictive permissions enforced by the library. | Prompts, model outputs, tool arguments/results, tokens, and user data can be written in plaintext. Telemetry serializes dataclasses and only truncates long `text`; it does not remove secrets. | Add redaction hooks/policies before serialization, a documented sensitive-data contract, configurable retention/rotation, and a secure-storage adapter. Create directories/files with owner-only permissions where supported. |
| P1 | `FileCheckpointer` has atomic replacement for a single file but no lock, optimistic version, or conflict detection across processes. | Concurrent runners using the same thread can overwrite the same step or create an invalid history. Atomic write is not a multi-writer consistency guarantee. | Define single-writer semantics now; reject concurrent runs per thread. Then add a transactional SQLite/Postgres implementation with revision/CAS behavior. |
| P1 | Provider adapters have no built-in retry classification, exponential backoff, `Retry-After` handling, or rate/concurrency limits. | Real API 429/5xx/network transients fail the node. `with_retry` wraps a whole node and cannot safely distinguish retryable provider errors or coordinate request pressure. | Add typed provider errors with status/headers, configurable retry policy, bounded exponential backoff/jitter, cancellation-aware retries, and per-backend/global semaphores. |
| P1 | Mutable state is a convention, not an enforced guarantee. Nodes receive the same shallow state object within a concurrent super-step. | A node can mutate a nested list/dict/set in place and race with a sibling, bypassing reducers and violating the documented immutable-state model. | Document this sharply immediately; then provide immutable snapshots or defensive copying/frozen containers, with a benchmarked opt-out for large state. Add a race-regression test. |
| P1 | The permission abstraction is not uniformly implemented by all agent adapters. | Kiro forwards ACP permission requests through `PermissionPolicy`; one-shot Codex/Claude adapters expose the policy field but rely on CLI flags/output and do not route permission prompts through that policy. The README wording can lead users to assume one security control covers all agents. | State the capability matrix explicitly. Either implement adapter-specific policy enforcement or remove the uniform-policy implication and require backend-specific security configuration. |
| P1 | `tool_loop` silently terminates at `max_turns` while the last assistant message may still contain unresolved tool calls. | Its documentation promises a final tool-free answer, which is not guaranteed at the cap. A caller may treat an incomplete tool plan as a completed response. | Return an explicit terminal status such as `completed`, `max_turns`, or `tool_calls_unresolved`; optionally raise a dedicated exception. Add an assertion to the existing cap test. |
| P2 | No static analysis, formatter/linter, coverage threshold, or dependency-security audit is configured. | Runtime tests alone will miss typing drift, untested parser/error branches, and supply-chain issues. | Add Ruff, Pyright or mypy, pytest-cov with a ratcheting threshold, and Dependabot/Renovate plus `pip-audit`/OSV scanning in CI. |
| P2 | The project metadata is not publication-ready. | `pyproject.toml` declares MIT but the repository has no `LICENSE`; homepage is `example.com`; no changelog, contributor guide, security policy, support policy, or release process exists. | Add those artifacts before publishing. Use a real project URL and test package metadata as part of release CI. |
| P2 | No production observability semantics. | Hooks provide useful local events, but there are no correlation conventions, traces/spans, metrics exporter, queue/backpressure policy, or failure-safe log rotation. | Implement the already planned OpenTelemetry `Hooks` adapter, define event schema/versioning, and make sink backpressure and failure behavior explicit. |
| P2 | Lifecycle/resource boundaries need stronger contracts. | Backends require manual `start()`/`close()`; skipped close can leak HTTP clients or subprocesses. Kiro `_write` does not await stream backpressure. | Provide async context managers, document ownership rules for shared backends, add bounded output/backpressure handling, and test cancellation/early consumer termination. |

## Missing capabilities

### Required before public or production use

1. A secure-by-default authorization model and a clearly documented capability
   matrix for each agent backend.
2. Sensitive-data controls for checkpoints and telemetry: redaction,
   permissions, retention, and pluggable secure storage.
3. A transactional persistent checkpointer (SQLite is a practical first step;
   Postgres/Redis may follow), plus single-writer/versioning semantics.
4. Provider-aware resilience: retry classification, rate-limit handling,
   deadlines, and concurrency budgets.
5. Release basics: `LICENSE`, non-placeholder metadata, changelog, security
   reporting policy, versioning/release procedure, and reproducible build
   verification.

### Valuable next product increments

1. Evaluation API (`Evaluator`, deterministic checks, JSON-schema checks,
   datasets, batch report, and trace links), already identified in `TODO.md`.
2. OpenTelemetry exporter over the existing `Hooks` interface.
3. Run-management tooling: checkpoint inspection, thread listing, deletion
   with retention safeguards, and a minimal CLI if a library-only API becomes
   burdensome.
4. Explicit graph run outcomes and cancellation/recovery semantics; at present
   callers infer too much from final state and exceptions.
5. Schema validation for initial state and node updates beyond channel names;
   reducers currently receive values without runtime type/schema checks.
6. More real integration coverage: cancellation, process crash/restart,
   corrupt checkpoint recovery, provider rate limits, multi-process checkpoint
   contention, and live test reporting that distinguishes skipped from passed.

## Recommended delivery sequence

1. **Secure the defaults:** change authorization behavior, document backend
   capability differences, redact persistence, and add tests proving no secret
   reaches default telemetry/checkpoints.
2. **Make one-machine durability honest:** define single-writer ownership and
   implement SQLite checkpointing with atomic revisions. Do not advertise
   multi-process durability before this exists.
3. **Harden providers:** typed failures, retry/rate policies, cancellation,
   and bounded concurrency. Make the tool-loop cap an explicit incomplete
   outcome.
4. **Establish release quality:** license, metadata, changelog, Ruff/type
   checking/coverage/security checks, and a clean build on the supported
   Python matrix.
5. **Expand the platform only after the above:** evaluator, OTel exporter,
   CLI/run management, and additional provider adapters.

## Final assessment

The core engine is the right thing to preserve: compact, composable,
well-documented, and tested against the difficult semantics that many workflow
projects postpone. The largest gap is the difference between *correct local
execution* and *safe, observable, multi-user operation with real agents and
real data*. Addressing the P0/P1 items will turn this from a strong technical
prototype into a credible foundation for production workflows; adding more
backends or higher-level features before that would increase surface area
without resolving the principal operating risks.
