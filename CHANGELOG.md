# Changelog

All notable changes to `agent-workflow-sdk` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
project aims to follow [Semantic Versioning](https://semver.org/).

## [0.7.0] - unreleased

### Added

- `agentflow.process.ManagedProcess`: one supervisor for every agent CLI. It owns spawn,
  stopping the whole process tree (a process group on POSIX; a kill-on-close Job Object on
  Windows, with `taskkill /F /T` as the fallback), the stop ladder (close stdin, terminate,
  kill) and a registry of live processes with `kill_all()`, also run at interpreter exit.
  `KiroBackend` and `CLIExecBackend` (Codex, Claude Code) run on it and lose their own process
  code, so a one-shot CLI turn that times out now kills its whole tree too.
- `PersistentClaudeBackend`: Claude Code as one long-lived process
  (`--input-format stream-json`) serving many turns; `reset_session()` clears the
  conversation in place with `/clear`. The agent, model and permissions are process flags,
  so they are fixed for the process. On a live `claude` the first turn took 8.5s and later
  ones 4.2s, i.e. the start-up is paid once.
- `agentflow.backends.pool.BackendPool` with `BackendSpec(kind, cwd, permission, agent, model)`:
  one way to get a backend for a step. Kiro v3 shares one process across agents
  (`use_agent`), Claude Code keeps a persistent process per (directory, agent, model,
  permissions), Codex is a fresh backend per lease. `KiroPool` stays as the Kiro-only
  wrapper. Workflow runs open `agent_pool()` around the plan.
- CI runs the offline suite on Windows too (advisory until it has passed once).
- `KiroBackend.use_agent(name)` (engine v3): switch the running process to another
  agent with a fresh session. v3 selects the agent as a session mode, so the process
  and the MCP servers it already connected are kept.
- `agentflow.backends.pool.KiroPool`: reuses Kiro processes across steps and agents
  (a fresh session and the step's own permission per lease; a backend whose lease
  failed is closed, never reused). Workflow runs open `agent_pool()` around the plan,
  so Kiro steps no longer start a process each. On a live `kiro-cli` with three custom
  agents and MCP servers, six steps went from 28s to 15s (about 1.9x), and each MCP
  server started once instead of once per step.

### Changed (breaking)

- `RunQueue.heartbeat` and `RunQueue.complete` now take a required positional
  `owner` (the `attempt` number stamped on the record `claim` returns) and
  return `bool` instead of `None`. Both are owner-checked: once a run is
  re-claimed by a newer attempt, a stale worker's `heartbeat`/`complete`
  matches nothing and returns `False` instead of silently clobbering the live
  attempt. They no longer raise `RunNotFound` for an unknown/stale run — they
  return `False`. Update any custom worker loop or `RunQueue` implementation to
  pass `owner=` and honor the returned bool.
- `Checkpoint` gained a required-at-rest `invocations` map (node name → highest
  invocation ordinal on this thread), changing the persisted checkpoint format.
  There is no migration (pre-1.0): a run checkpointed by an earlier version is
  not resumable under this version; a legacy payload without the key loads with
  an empty map. Node names + ints only, so it is never redacted.
- `RunQueue` gained a required `async def is_owner(self, run_id, owner) -> bool`
  (True iff the run exists, is `RUNNING`, and `attempt == owner`). An out-of-tree
  `RunQueue` implementation must add it; `MemoryRunQueue` and `PostgresRunQueue`
  implement it in-tree. The running graph calls it to owner-check every
  checkpoint write.
- A subgraph node reached at run time now requires its embedded graph to have
  its own checkpointer (compile it with `checkpointer=...`). A suspend inside a
  subgraph is unobservable without a child checkpoint, so a checkpointer-less
  subgraph — even one that would have finished cleanly — now raises `GraphError`
  naming the subgraph instead of risking a swallowed interrupt.

### Added

- Provider-neutral plans. A plan names roles (`researcher`, `analyst`, `writer`, `coder`,
  `reviewer`; overridable by `.agentflow/roles/*.md`) and the permission each step needs, never
  a backend. The three system agents (orchestrator, designer, critic) are defined in the engine
  and run on a *system backend* (`Environment(system_backend=...)`, default the first installed)
  in an empty directory with their whole instructions in the prompt. The executing backend is
  chosen when a run starts: `start_run(plan_id, params, backend=, extensions=, mode=)`, bound to
  that backend's generic agent, which applies the user's own steering, skills and
  `AGENTS.md` / `CLAUDE.md` itself. The same approved plan runs on Kiro, Claude Code or Codex.
  `inventory_digest` of a neutral inventory covers only the roles (description, permissions,
  preamble), so an approval does not depend on the backend or on bound files. A plan that the
  chosen backend cannot run (for example `allowlist` on Codex) is refused at launch with reasons;
  `GET /plans/{id}/binding?backend=&ext=kind:name&mode=` previews the binding.
- Worktree cleanup. When a run finishes, what the agents wrote is committed to the run's branch
  (the copied extension files are not counted), and a worktree with no changes is removed together
  with its branch; one with work is kept, with the commit and the number of changed files on the
  run. A run that is held or waiting keeps its worktree. `remove_workspace(run_id, branch="keep" |
  "delete" | "force")`, `list_workspaces()` and `sweep_workspaces("clean" | "finished")`, over
  `DELETE /runs/{id}/workspace`, `GET /workspaces` and `POST /workspaces/sweep`; buttons on the Run
  and Runs screens. Removal never touches a worktree outside the service's own folder, refuses
  while changes are uncommitted, and drops only its own git record (not `git worktree prune`).
- Run mode `strict` / `track` / `auto` for the files a run relies on: strict holds the run when
  one changes between steps (resume accepts the new state), track warns once. Each run gets its own
  git worktree on `agentflow/<run_id>` with the project's uncommitted extension files copied in; a
  non-git project is used in place and the run says so.
- `agentflow.workflow.extensions`: read-only discovery of a backend's steering, skills and
  `AGENTS.md` / `CLAUDE.md` (names, descriptions, hashes, never content), `available_backends()`,
  and `probe_backend()`, a canary check that runs the real agent in a temporary directory and
  proves whether it applies those files. Endpoints `GET /backends`,
  `GET /backends/{name}/extensions`, `POST`/`GET /backends/{name}/probe`, `GET /environment`;
  a Backends screen in the UI. Verified on Kiro, Claude Code and Codex.
- Live process: an activity log per design and run (which role or step started, finished,
  failed, how long), a once-a-second heartbeat on the SSE streams, and every event inside an
  agent call (text, tool calls and results, permission requests, usage) stored under
  `GET /designs/{id}/agent-stream` and `GET /runs/{id}/agent-stream`. `collect_text` reports to
  an observer set with `observing(...)`; the observer cannot change the call and its errors are
  ignored. Create and Run show "Working: ... N s", the heartbeat and an Agent output panel.
- Create can reopen a design from the URL (`?design=<id>`) and lists earlier designs; Plans shows
  an empty state instead of a dead Approve button.

- `agentflow.server.workflow_app` now serves the bundled UI as static files from
  the same FastAPI app: `GET /` redirects to `/ui/index.html` and the mockup
  under `ui-mockup-demo/` is mounted at `/ui` (mounted last so it never shadows
  the API). `create_workflow_app` and `serve` gained a `ui_dir` parameter
  (defaults to the repo's `ui-mockup-demo/`). Two Server-Sent Events endpoints,
  `GET /designs/{id}/events` and `GET /runs/{id}/events`, stream the full record
  on every change and close at a terminal/paused status; `GET` records remain the
  source of truth and fallback, and an unknown id still yields 404.
- The five `ui-mockup-demo/` screens (New plan, Plans, Run, Runs, Inventory) are
  now wired to the live API over `fetch` and SSE instead of hardcoded mock data,
  with no build step: a small `window.AF` helper (`get`/`post`/`stream`) was added
  to `support.js`, and each screen's `DCLogic` component drives its state from real
  `WorkflowService` responses — Create designs a plan end to end and streams design
  progress, Plans lists plans and can approve and launch, Run shows live run progress
  over SSE (with a GET-polling fallback) and can answer a waiting run and resume a held
  one, Runs lists runs attention-first, and Inventory renders real agents and backends.
  The "fictional mock data" banners were removed from every screen and the launcher.
- `agentflow.workflow`: the building blocks of a workflow-from-request layer, kept
  out of the engine (nothing in `agentflow` imports it; a test enforces that, and
  that it needs no third-party package). Import it explicitly:
  `from agentflow.workflow import validate_plan`.
  - `validate_plan(plan, inventory=...)`: deterministic validator for `plan.json`
    (schema version, structure, unique ids, acyclic dependencies, data bindings,
    `final_outputs`, agents, permissions and backend capabilities). Returns every
    problem at once as `Issue` objects with stable codes; unknown keys are errors.
  - Agent inventory (`validate_inventory`, `load_inventory`, `resolve_backend`,
    `inventory_digest`) and the backend capability catalog, pinned to the real
    backend classes by a drift test.
  - Approval helpers: `plan_hash`, `check_approval` (a plan or inventory change
    revokes an approval), `auto_approval_blockers`.
  - `plan.schema.json` (`load_schema()`): the structural JSON Schema, shipped in the
    wheel.
  - `run_plan(plan, inventory, runner, params=...)` / `build_workflow_graph(...)`: run
    an approved plan as data through `add_plan_executor`. Each step resolves its input
    bindings, reuses a stored result for identical (agent, prompt, inputs, contract,
    checks), and otherwise runs the agent inside a quality-gate sub-graph on a
    deterministic sub-thread, so the attempt counter survives a crash and resume. A
    `side_effects` step runs under `IdempotentOp`. The agent's answer must end with a
    JSON object matching the step's `outputs`. The result is a `RunOutcome`
    (`completed | partial | failed`, with failed and blocked steps listed).
    `backend_runner(cwd=...)` runs each attempt on a fresh Kiro / Claude Code / Codex
    backend whose policy comes from the step's `permission`.
  - `may_ask`: a step may answer with a clarifying question instead of a result. It is
    answered by `default_answer`, by a human (`ask_mode="interrupt"`: the run returns
    `waiting` with the question, continue with `resume_plan(..., answer=...)`), or it
    stops the step with `needs_clarification` (`ask_mode="stop"`, the default). A
    stopped run continues as a NEW run: `run_plan(..., answers={step: answer})` reuses
    the finished steps from the cache, the answer is part of the cache key, and the
    approval stays valid. A resume does not repeat the agent's question: the finished
    attempt is read back from its durable sub-thread.
  - `resume_plan(...)` continues a run that is waiting, held, or was interrupted by a
    crash, after re-checking the approval. With `inventory_source=...`, the approval is
    re-checked against the CURRENT inventory before every step; a revoked approval
    HOLDS the run (`RunOutcome.status == "held"`, finished steps kept) instead of
    failing it.
  - `design_plan(request, inventory, designer, max_rounds=3)`: the design phase. Three
    roles, one agent call each: `orchestrator` (brief), `creator` (plan), `verifier`
    (logic review). A bounded loop: the deterministic checks (`validate_plan`,
    `unsupported_features`, and a minimum-evidence rule that sends back steps whose
    checks could never fail) run BEFORE the verifier, so an invalid plan never costs a
    verifier call; verifier `blocker` remarks go back to the creator, `warning` remarks
    travel with the plan. It ends as `converged`, `round_limit` (a human decides, with
    the open remarks), `not_executable` (cannot be approved) or `failed`; it never
    raises on bad agent output. Designers see only agent names, descriptions and allowed
    permissions, never backends or paths. Fields the creator may not set (approval,
    verification, provenance) are dropped. `backend_designer(make)` runs each call on a
    fresh backend.
  - Design roles as agent definitions. The roles' instructions are split into a static part
    (`role_system`) and the data (`*_task`); the full prompts (`*_prompt`) are their
    concatenation and work on any backend with nothing installed. For backends with named
    agents, `python -m agentflow.workflow install claude|kiro --to DIR` copies ready-made
    definitions (Claude Code: Markdown with YAML frontmatter; Kiro: JSON) into a directory
    YOU name: there is no default location, nothing is overwritten without `--force`,
    `--dry-run` reports only. The shipped files live in `agentflow/workflow/agent_files/`,
    are generated from the prompts, and a test keeps them identical. `design_plan(...,
    named_agents=True)` then sends only the data; `claude_designer(cwd)` runs the roles on
    Claude Code with `--agents` inline, so nothing has to be installed. Codex has custom
    agents (TOML) too, but the SDK cannot select one by name yet, so Codex designers get the
    full prompt.
  - `design_plan(..., ask=...)`: the orchestrator can talk to the user before it writes the
    brief. It sends up to five questions (each with the answer it would assume), `ask`
    returns the reply, and this repeats for at most `max_question_rounds` (3). With no `ask`,
    after the budget, or on an empty reply it must write the brief and list gaps as
    ambiguities. The talk is kept in `DesignResult.dialogue`.
  - Check primitives: `count`, `compare` (12 operators, value or another field),
    `unique`, `sorted`, on top of `not_empty` and `has_fields`. A target may be a path
    (`items[*].level`, `items[0].name`); a missing path part fails, an empty `[*]` match
    fails unless `allow_empty`. Parameters are validated in `validate_plan` (`bad_check`).
    No regex and no `command_succeeds` on purpose.
  - Local service for the UI: `WorkflowService` (`agentflow.workflow.service`, stdlib only)
    designs a plan (the orchestrator's questions arrive as `asking` and are answered over
    HTTP), records a human approval, and starts, answers and resumes runs. State lives in a
    data directory (`records.db`, `store.db`, `checkpoints.db`). HTTP adapter:
    `agentflow.server.workflow_app.create_workflow_app` (the `server` extra now includes
    uvicorn); `python -m agentflow.workflow serve --inventory ... --data-dir ... --cwd ...`
    listens on `127.0.0.1` only, with no authentication. A restart marks unfinished designs
    and runs `interrupted`; a run is continued with `resume`. Clients poll; no event stream yet.
  - `approve_plan(plan, inventory, by=...)` records a human approval bound to the plan
    hash and the inventory digest (refuses a plan that is invalid or not executable);
    `try_auto_approve` approves only when the safety threshold holds and no verifier
    blocker is open.
  - Refused up front (`PlanRefused`), never ignored: an invalid, unapproved or
    stale-approved plan, missing or unknown run parameters, an answer for a step that
    cannot take one, and features not executable yet: `semantic_check`, budgets,
    timeouts, artifacts, `may_ask` together with `side_effects`, and check kinds other
    than `not_empty` and `has_fields`.

- `Checkpoint.interrupt_answers`: an ordered tuple of the human answers already
  delivered to a node's successive `interrupt` calls. A node that interrupts
  more than once is now driven across resumes so the Nth `interrupt` receives
  the Nth answer, and the sequence survives a crash replay. Round-tripped by
  every checkpoint backend and redacted alongside `state`/`interrupt_payload`.

- `SqliteStore` (`agentflow.store`, also exported from `agentflow`): a durable
  `Store` in a single SQLite file, with no server and no extra. Same contract as
  `MemoryStore` and `PostgresStore`, including TTL and the atomic `put(...,
  if_absent=True)` / `StoreConflict` claim (the write lock is taken before the
  existence check, so independent store objects or processes sharing the file get
  exactly one winner). With it a single machine keeps `IdempotentOp` markers,
  `skip_if_done` caches and `ScoreCache` across a restart without PostgreSQL.
  Namespace segments may not contain `NUL`; a non-JSON value raises `StoreError`.
  `tests/test_store.py` now runs the shared contract against both offline backends.
- `reset_session()` on agent backends: forget the conversation so the next turn
  starts fresh without creating a new backend object. `CLIExecBackend` (Codex,
  Claude Code) clears the remembered session id, which they otherwise resume on the
  next turn, so a reused instance leaked one task's context into the next; a turn
  already running when it is called does not bring the old session back. `KiroBackend`
  opens a new ACP session on the same process (reapplying the agent mode or model);
  it raises `BackendError` before `start()` or during a turn. By default the previous
  session is left alone: kiro-cli keeps each session as a record on disk, so a
  pipeline that resets per step accumulates them (no memory or file descriptors are
  held). `reset_session(delete_previous=True)` deletes the previous session with ACP
  `session/delete` once the new one is ready, so a failure to create the new session
  leaves the old one intact; if the delete itself fails the error propagates but the
  new session is already active. `BaseAgentBackend` provides a no-op default; the method
  is not part of the `AgentBackend` protocol, so custom backends without it still conform.

### Changed

- The step cache key includes the executor (backend, agent, role text): the same plan on another
  backend is a new run, not a replay of the first one's results.
- Front matter in skill and steering files may use block scalars (`>-`, `|`) for descriptions.
- Kiro steps may grant `disclose_context` (the permission Kiro asks before loading a skill) even
  under `read_only`; without it a skill the agent found could not be used.
- The UI no longer carries leftover mock data or controls the server ignores (approval mode and
  design rounds on Create, "cap at read_only" on Plans), and the Run screen stops polling once a
  run has finished or paused.

### Fixed

- Worker crash recovery resumes any unfinished run from its saved checkpoint
  instead of re-running the graph from START, so a step completed before the
  crash (and its side effects) is not repeated (bug 1).
- A human interrupt inside a subgraph now pauses the parent run: the child
  suspend propagates up as a parent interrupt and the child resumes correctly on
  continuation, rather than the parent treating the subgraph as done and running
  downstream nodes without an answer (bug 2).
- Two `interrupt()` calls in one node now receive their answers in order across
  resumes; previously the single stored answer always went to the first call and
  the second question reappeared (bug 3).
- A stale worker whose lease expired can no longer `complete` or `heartbeat` a
  run that was re-claimed by a newer attempt (owner-checked lease) (bug 4).
- Cancelling a worker task now cancels and awaits the in-flight graph work
  rather than leaving it running detached in the background, where it could
  re-run concurrently after lease expiry (bug 5).
- Cancelling a suspended run (interrupted / waiting / queued) now transitions it
  straight to `cancelled`; a worker never re-claims such a run, so the old
  cooperative flag left it suspended forever (bug 6).
- `run_until_done` with a capped `max_sleep` now loops until the real `wake_at`
  deadline instead of resuming after a single shortened nap, so a timer no longer
  fires early (bug 7).
- `MemoryCheckpointer` now deep-isolates checkpoint state at both the write and
  read boundaries, so mutating a nested container after `invoke`/`get` no longer
  rewrites a stored checkpoint (bug 8).
- A worker that loses its lease mid-run now cancels and awaits the in-flight
  graph task and owner-checks every checkpoint write (parent thread and child
  sub-threads alike), so a stale attempt can no longer run further nodes, fire
  side effects, or persist a checkpoint after a newer attempt has re-claimed the
  run; the stale worker relinquishes the run without writing a terminal status
  (invocation-ownership bug 1).
- A loop that re-enters the same subgraph node no longer collides across
  iterations: each entry derives a per-invocation sub-thread
  (`{thread}::{name}#{inv}`), so iteration 1 no longer reads iteration 0's
  finished child checkpoint and the parent correctly observes a later-iteration
  interrupt (invocation-ownership bug 3).
- An ordinary (non-interrupt) child checkpoint left after a crash now resumes
  the child from its own checkpoint instead of re-invoking it from START, so the
  child's already-completed first step is not repeated
  (invocation-ownership bug 5).
- `KiroBackend` is released at `TurnEnd`, not when the event stream is finalised. A
  consumer that stopped reading at `TurnEnd` (`return`/`break` inside `async for`)
  left the suspended generator holding the turn, so the next `prompt()` or
  `reset_session()` failed with "a turn is already in progress". A generator that
  is finalised late (garbage collection) can no longer take a later turn's event
  routing or cancel its pending permission answers: cleanup now acts only on the
  turn the generator owns. Stopping BEFORE `TurnEnd` still leaves the turn in
  progress, because the agent is still working.
- Closing a `KiroBackend` event stream before `TurnEnd` (`aclose()`, an exception in
  the consumer, task cancellation) now sends the ACP `session/cancel` notification
  for that turn's session. Previously the agent kept running an unread turn (editing
  files, spending tokens) until the next prompt. Best-effort: nothing is sent if the
  process is gone, after a normal `TurnEnd`, or from a late-finalised generator whose
  turn was already released.
- `ClaudeCodeBackend(permission=AllowAll())` now actually allows edits: with no
  explicit `allowed_tools` / `permission_mode` it launches with
  `--permission-mode acceptEdits` (headless `claude -p` refused every write with no
  flag at all). This is not `bypassPermissions`: shell commands still need an
  explicit `allowed_tools`. Explicit settings still win; `Interactive`/`Callback`
  add no flag.
- `KiroBackend.close()` no longer raises `AttributeError` on Windows, where
  `signal.SIGKILL`, `os.killpg` and `os.getpgid` do not exist. Shutdown now
  escalates by intent (`_terminate`, then `_kill`) instead of by signal number:
  POSIX still signals the whole process group; on Windows it falls back to
  `proc.terminate()` / `proc.kill()`. Known limitation: on Windows only the direct
  child is killed, so a grandchild such as `kiro-cli-chat` may outlive it.
  (Superseded: on Windows the child now runs in a kill-on-close Job Object, so the whole tree is reaped on a hard kill and by the OS if the host process dies; `taskkill /F /T` is the fallback.)

The interrupt answer-delivery path (`interrupt_answers`) is unchanged this
release.

## [0.6.0] - 2026-10-11

### Changed (breaking)

- Quality-gate results and plan-executor tasks are now stored in state as plain
  dicts, not dataclasses, so they are JSON-serializable and survive a durable
  checkpointer. Read `state["gate_results"][name]` as a mapping
  (`result["passed"]`, not `result.passed`), and read `plan_executor` results
  as dicts (`r["output"]`, not `r.output`). The evaluator you pass to
  `add_quality_gate` still returns a `GateResult`, and a handler still returns a
  `TaskResult` — only the *stored* form changed. This fixes a real bug: either
  prebuilt combined with a file/sqlite/postgres/redis checkpointer previously
  raised "not JSON serializable" on the first checkpoint.

### Added

- `plan_executor` (`agentflow.prebuilt`): `add_plan_executor(g, handlers=...,
  on_idle=...)` runs a dependency graph of tasks a planner emits as data — a
  task runs once its prerequisites are done, independent tasks fan out
  concurrently into a fixed slot pool, and the plan is checkpointed so a resume
  recomputes the runnable frontier. Ships `Task`, `TaskResult`, the `PlanState`
  mixin, and `plan_complete` / `plan_stalled`.
- `scoring` (`agentflow.prebuilt`): a deterministic, backend-independent measure
  for a candidate — `Score`, the `Scorer` contract, `score_candidates` (batch,
  bounded concurrency), `best_of` (promote the best feasible, keep the incumbent
  unless strictly beaten), and `ScoreCache`. The verifier half of an optimizer
  loop, so swapping the proposer (a local agent, a hosted LLM) never changes the
  score.
- Token / cost budget: `Usage` (input/output/total tokens, optional `cost_usd`)
  is now an optional field on `TurnEnd`, populated by the Ollama and Codex
  backends. New `agentflow.prebuilt` helpers: `TokenMeter` (a `Hooks` listener
  that sums usage for observation), the `BudgetState` mixin, `record_usage`, and
  `over_budget` — budget lives in state, survives a resume, and a router stops
  the run at a token or cost cap.

### Documentation

- A unified Tutorials section: the Q&A agent quick start, a ten-chapter
  "Mechanics: building an optimizer" tutorial on a toy cost model, and a
  ten-chapter "Applied: a query optimizer with agents" tutorial that measures
  real index/rewrite candidates against a seeded SQLite database, with a local
  Codex agent as the opt-in proposer.

## [0.5.0] - 2026-10-04

### Added

- `by_key(policy)` reducer (`agentflow.state`, re-exported from `agentflow`):
  a reducer for a dict channel whose fields each fold with their own reducer.
  Where `merge` applies one last-wins policy to a whole dict, `by_key` lets a
  single structured channel hold fields that accumulate differently — e.g. one
  `search` channel where `intents` / `candidates` / `verified` grow via
  `append` while `events` / `incumbent` are replaced via `last`. This is the
  core primitive a dual-state search/optimization layer (single planner,
  concurrent workers, a deterministic promotion boundary) builds on; the
  fan-out, state isolation, checkpoints, Store, and quality gates it also needs
  already exist. See the dual-state search recipe and
  `examples/dual_state_search.py`.

### Notes

- `by_key` folding is one level of structure, not a recursive deep-merge: each
  field's value is combined whole by its reducer (so `last` on a field replaces
  the whole object, it does not patch one of its keys). Writing a field absent
  from the policy raises `KeyError`, mirroring how the engine rejects writes to
  undeclared top-level channels. Use it for distinct fields that accumulate
  differently, not for concurrent partial patches of the same nested object.

## [0.4.0] - 2026-10-03

### Added

- Optional HTTP control-plane server (`server` extra): `agentflow.server.
  create_app(queue)` returns a FastAPI ASGI app that maps 1:1 onto a
  `RunQueue`, so runs can be enqueued and observed over the network. Routes:
  `POST /runs`, `GET /runs/{id}`, `GET /runs`, `POST /runs/{id}/resume`,
  `POST /runs/{id}/cancel`, `GET /stats`, `GET /healthz`. Request/response
  bodies are Pydantic models, so FastAPI serves interactive OpenAPI docs at
  `/docs`. `pool_health=` wires a `WorkerPool`'s health into `/healthz`. The
  app adds no behavior of its own — each route forwards to one queue method.
  See `examples/serve_control_plane.py` and the control-plane docs.
- `server` install extra: `pip install 'agent-workflow-sdk[server]'`
  (`fastapi>=0.110`). The core remains dependency-free; serve the returned
  ASGI app with any ASGI server (e.g. `uvicorn`), installed separately.

### Changed

- The worker protocol (`claim` / `heartbeat` / `complete`) is deliberately not
  exposed over HTTP; workers talk to the queue directly. The HTTP layer ships
  no authentication, authorization, rate limiting, or multi-tenancy — those
  stay out of scope and are the caller's responsibility (trust boundary /
  reverse proxy). `DESIGN.md` updated accordingly.

## [0.3.0] - 2026-10-02

### Added

- Watch nodes: park a run on an external system without burning turns while
  nothing changes. `ctx.wait(wake_at, payload)` is a timer-based sibling of the
  human interrupt — it suspends the run until a time, then re-runs the node.
  `Checkpoint` gains `wake_at` and a derived `suspend_kind` (`"human"` |
  `"timer"`); the human-interrupt path is unchanged.
- Durable parked execution: `RunStatus.WAITING` + `RunRecord.wake_at`. A run
  parked on `ctx.wait` is `WAITING` in the queue and does not hold a worker;
  `claim` re-takes it once `wake_at` passes (symmetric to lease expiry), in both
  `MemoryRunQueue` and `PostgresRunQueue`. The worker frees itself on a timer
  suspend and resumes on re-claim.
- `CompiledGraph.run_until_done(...)` drives timer waits inline (sleeps until
  `wake_at` and resumes), for running a watch loop without the control plane —
  the process must stay alive for the wait, unlike the parked control-plane
  path.
- `agentflow.prebuilt.watch`: `WatchResult` (idle/activity/terminal), the
  `Watcher` protocol, `CommandWatcher` (poll any command that prints the
  contract JSON), `watch_node` (polls, parks on idle, loops until non-idle), and
  `route_watch`. GitHub/CI/etc. are examples over `CommandWatcher`, not core.
  See `examples/watch_until_done.py`.

### Fixed

- First-use schema creation on PostgreSQL is now concurrency-safe. `CREATE TABLE
  IF NOT EXISTS` can collide on `pg_type` when two sessions bootstrap the same
  table at once; the Postgres backends now run that DDL under a
  `pg_advisory_xact_lock` (plus an in-process lock), so a pool of workers
  starting together no longer races.

### Changed

- Internal: the Postgres backends (`PostgresStore`, `PostgresCheckpointer`,
  `PostgresRunQueue`) now have a real-server contract test suite, run in CI
  against a `postgres` service container. The tests are marked `pg` and skipped
  by the default hermetic run (they need `POSTGRES_TEST_DSN`); they cover the
  SQL-only paths the in-memory suites can't — the `if_absent` conditional write
  and `StoreConflict`, the `if_revision` compare-and-set and `CheckpointConflict`,
  and concurrent `claim` under `FOR UPDATE SKIP LOCKED`.

## [0.2.0] - 2026-10-01

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

- Atomic conditional write on `Store`: `put(..., if_absent=True)` creates only
  if no live item exists at the key, else raises the new `StoreConflict` (an
  expired item counts as absent and can be reclaimed). Implemented for
  `MemoryStore` (atomic within a process) and `PostgresStore` (atomic across
  processes via a single conditional `INSERT`). This is the store-level
  primitive for a single-flight claim.
- Effect-level idempotency (`agentflow.prebuilt`): `IdempotentOp(store,
  namespace)` protects one side effect inside a node with an atomic claim → run
  → commit in a `Store`, so a resume or retry does not repeat it, and two
  workers racing the same key give one winner and one conflict (the
  duplicate-starter race is closed within a backend). Its `on_incomplete` policy
  (`"error"` default, `"rerun"`, `"skip"`) handles a prior attempt left
  `in_flight`, and `on_error` (`"keep"` default, `"release"`) controls the
  marker when `fn` raises — because an exception does not prove the effect was
  skipped (a lost response after a successful `POST`). `effect_key(*parts)`
  builds a stable token to pass downstream as the provider's idempotency key.
  New `IncompleteEffectError`. This narrows the duplicate window and makes every
  ambiguous case explicit, but does not by itself guarantee exactly-once at the
  orchestrator layer — the winner can still crash or lose its response inside
  the effect, so exactly-once ultimately needs idempotency at the effect.
  Checkpoints alone give only at-least-once, since a super-step is atomic over
  state only.

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

First public release: an async-first SDK for agent workflows with pluggable
agent and LLM backends.

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
