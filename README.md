# agent-workflow-sdk

An async-first, LangGraph-style SDK for building agent workflows from
composable pieces. You describe a workflow as a graph of nodes over a typed,
reducer-based state, and run it against a pluggable backend — a coding agent
(Kiro, Codex, or Claude Code) or a plain LLM (Ollama, any OpenAI-compatible
endpoint, or Anthropic).

The import root is `agentflow`. The distribution name is
`agent-workflow-sdk`.

## Why

- Build any workflow from primitives: nodes, edges, conditional routing, and
  a shared state. Not a fixed loop, not a linear chain.
- Swap the backend without touching workflow code. Agents and LLMs share one
  event vocabulary.
- Async everywhere: the engine, backends, checkpointing, and streaming.
- Durable by default: every super-step is checkpointed, runs resume after a
  crash, and human-in-the-loop interrupts suspend and resume a run.


## Install

The core is dependency-free. Backends that need extra libraries are optional
extras.

```bash
pip install -e .            # core only
pip install -e '.[ollama]'  # + httpx, for the Ollama LLM backend
pip install -e '.[dev]'     # + pytest, pytest-asyncio
```

Python 3.11+ is required. The Kiro backend needs `kiro-cli` on PATH.

## Quickstart

A graph that loops until a counter reaches a target, then stops.

```python
import asyncio
from typing import Annotated
from agentflow import Graph, START, END, State, add, append

class CountState(State):
    n: Annotated[int, add]        # updates are summed
    log: Annotated[list, append]  # updates are concatenated

async def tick(state, ctx):
    return {"n": 1, "log": f"tick {state.get('n', 0) + 1}"}

def route(state):
    return "again" if state["n"] < 5 else "done"

g = Graph(CountState)
g.add_node("tick", tick)
g.add_edge(START, "tick")
g.add_conditional_edges("tick", route, {"again": "tick", "done": END})
app = g.compile()

print(asyncio.run(app.invoke({"n": 0, "log": []})))
```

See `examples/hello_graph.py` and `examples/optimize_loop.py` for runnable
versions.

## Core concepts

### State and reducers

State is a `TypedDict` subclass of `State`. Each field is a channel; annotate
it with a reducer that folds each node's update into the current value. A
field without a reducer uses `last` (last-value-wins). Built-in reducers:
`last`, `append`, `add`, `merge`, `union`. A node returns a partial update; it
never mutates the state it was given.

### Nodes and edges

A node is an async callable `(state, ctx) -> update`. Edges are either static
(`add_edge`) or conditional (`add_conditional_edges` with a `router(state) ->
key`). `START` and `END` are sentinels. A conditional router may return a list
of keys to fan out to several nodes at once.

### Execution

A run proceeds in super-steps. All nodes on the current frontier run
concurrently against the same immutable state, their updates are folded
through the reducers in a deterministic order, and the next frontier is
computed from the edges. A `step_limit` guards against runaway loops.

Super-steps are all-or-nothing: if any node in a step raises, that step's
updates are discarded before they touch the state and no checkpoint is written
for it, so the run stays at the last committed checkpoint. Input to a run is
validated at start — an undeclared channel key is rejected with a clear error
rather than sitting unused in the state.

`CompiledGraph` gives you:

- `await app.invoke(input, thread=..., timeout=...)` — run to completion,
  return state. `timeout` bounds the whole run; on expiry it raises
  `RunTimeout` and the last checkpoint is preserved for resume.
- `app.stream(input, thread=...)` — async-iterate `StreamEvent`s as they occur.
- `await app.resume(thread, value=...)` — continue a suspended run.
- `await app.get_state(thread)` / `app.history(thread)` — inspect checkpoints.

For non-async callers there are blocking wrappers — `app.invoke_sync(...)`,
`app.resume_sync(...)`, `app.stream_sync(...)` — which run the coroutine via
`asyncio.run` and refuse to run inside an existing event loop.

### Subgraphs

A compiled graph composes as a node in another graph:

```python
parent.add_node("sub", compiled_subgraph)              # channels shared by name
parent.add_subgraph("sub", compiled_subgraph,          # or map explicitly
                    input_map={"query": "q"}, output_map={"answer": "result"})
```

A subgraph runs on an isolated checkpoint sub-thread and merges its result
back through the parent's reducers. Use `output_map` to route a result into a
distinct parent channel and avoid double-counting accumulator channels.

### Observability

Pass a `Hooks` implementation to `compile(hooks=...)` for lifecycle callbacks
(`on_run_start`, `on_node_start/end/error`, `on_step_end`, `on_run_end`). A
hook raising never breaks a run. `RunMetrics` is a ready-made `Hooks` that
records per-node call counts, errors, and durations:

```python
from agentflow import RunMetrics
m = RunMetrics()
await g.compile(hooks=m).invoke({...})
print(m.summary())   # {"steps": 3, "completed": True, "nodes": {...}}
```

For durable telemetry, `JsonlTelemetry` is a `Hooks` that writes one JSON line
per event (`run_start`, `node_start/end/error`, `event`, `step`, `run_end`) —
backend events surfaced via `ctx.emit` are logged too. Compose several
listeners with `MultiHooks`; a failing listener never breaks the run or the
others:

```python
from agentflow import MultiHooks, RunMetrics, JsonlTelemetry
metrics = RunMetrics()
telemetry = JsonlTelemetry(".runs")           # dir -> one file per thread
app = g.compile(hooks=MultiHooks(metrics, telemetry))
```

See `examples/telemetry_demo.py` for a runnable version.

For distributed tracing, `agentflow.otel.OtelHooks` (install the `otel` extra)
is a `Hooks` that emits an OpenTelemetry span per run and per node:

```python
from agentflow.otel import OtelHooks
app = g.compile(hooks=OtelHooks())   # uses the global tracer/provider
```

For operators who scrape Prometheus instead of running an OTel collector,
`agentflow.prometheus.PrometheusHooks` (install the `prometheus` extra) is a
`Hooks` that records run/node/step counters, a node-duration histogram, and
backend-event counts. It uses a private registry by default; call
`exposition()` to render the text format for a `/metrics` endpoint (you own the
HTTP layer).

```python
from agentflow.prometheus import PrometheusHooks

metrics = PrometheusHooks()
app = g.compile(hooks=metrics)
# ... inside your /metrics handler:
body, content_type = metrics.exposition()
```

Compose several listeners with `MultiHooks(RunMetrics(), OtelHooks(),
PrometheusHooks())`.

## Backends

Two kinds of backend share one event stream, so a node calls either the same
way.

- Agent backends (`AgentBackend`) drive a session that runs its own tools and
  asks permission. Available: `KiroBackend` (persistent ACP session over
  `kiro-cli`), `CodexBackend` (`codex exec --json`), `ClaudeCodeBackend`
  (`claude -p --output-format stream-json`).
- LLM backends (`LLMBackend`) are stateless: messages in, token stream out.
  They never run tools themselves — a tool call is a request the graph
  fulfils. Available: `OllamaBackend` (local `/api/chat`), `OpenAIBackend`
  (any `/v1/chat/completions` endpoint — OpenAI, Groq, Together, vLLM, LM
  Studio, and Ollama's own `/v1` shim), and `AnthropicBackend` (Messages API).

```python
from agentflow.backends.kiro import KiroBackend
from agentflow.backends.codex import CodexBackend
from agentflow.backends.claude_code import ClaudeCodeBackend
from agentflow.backends.ollama import OllamaBackend
from agentflow.backends.openai import OpenAIBackend
from agentflow.backends.anthropic import AnthropicBackend

agent = KiroBackend("vibe", engine="v3")          # persistent session
codex = CodexBackend(sandbox="read-only")          # one-shot per turn
claude = ClaudeCodeBackend(model="sonnet")         # one-shot per turn
llm = OllamaBackend("llama3.2")                     # local HTTP
openai = OpenAIBackend("gpt-4o-mini", api_key="...")  # any OpenAI-compatible API
anthropic = AnthropicBackend("claude-sonnet-4", api_key="...")  # Messages API

await agent.start()
async for event in agent.prompt("summarize the repo"):
    ...  # TextChunk, ToolCall, ToolResult, ..., TurnEnd
await agent.close()
```

The three agent backends have different lifecycles under one interface. Kiro
holds a long-lived JSON-RPC session; Codex and Claude Code run a fresh
subprocess per turn and carry a resumable session id between turns. Either way
you call `start()`, `prompt(text)`, `close()` and consume the same event
stream. Backends are constructed by you and passed into your nodes. The core
never imports a backend, so importing `agentflow` pulls in no subprocess or
HTTP dependency.

The HTTP LLM backends retry transient failures (connection errors, timeouts,
429/5xx) on the connect/initial-response phase only — never mid-stream, so a
partial token stream is never replayed. `Retry-After` is honored. Tune it with
a `RetryPolicy`:

```python
from agentflow.backends import RetryPolicy
from agentflow.backends.openai import OpenAIBackend

llm = OpenAIBackend("gpt-4o-mini", api_key="...",
                    retry=RetryPolicy(max_retries=4, backoff=0.5))
```

Run `python examples/agents_demo.py` to smoke every backend installed on your
machine (set `KIRO_AGENT` to a valid agent id, e.g. `vibe`, to include Kiro).

### Permission policies

A `PermissionPolicy` is **required** when constructing an agent backend — there
is no default, because auto-approving an agent's tool use is a security choice
the caller must make explicitly. Options: `AllowAll` (trusted local sandbox
only), `DenyAll`, `Interactive` (escalate to a human via an engine interrupt),
or `ToolAllowlist({"read_file", ...}, fallback=DenyAll())` for least-privilege
scoping.

```python
from agentflow.backends import AllowAll, ToolAllowlist, DenyAll
KiroBackend("vibe", permission=ToolAllowlist({"read_file"}, fallback=DenyAll()))
```

Capability matrix — where the policy actually applies:

| Backend | Routes tool requests through `PermissionPolicy`? |
| --- | --- |
| `KiroBackend` | Yes — ACP `session/request_permission` is resolved by the policy (and `Interactive` drives a real HITL interrupt). |
| `CodexBackend` | No — one-shot CLI; permission is governed by its `--sandbox` mode. The policy field is still required but does not intercept prompts. |
| `ClaudeCodeBackend` | No — one-shot CLI; permission is governed by CLI flags (`--permission-mode`, `--allowed-tools`). |

For the one-shot CLI agents, configure their own controls (sandbox, allowed
tools) — the SDK policy alone does not sandbox them.

## Checkpointing and human-in-the-loop

Pass a checkpointer to `compile` to make runs durable.

```python
from agentflow import FileCheckpointer
app = g.compile(checkpointer=FileCheckpointer(".runs"))
```

`MemoryCheckpointer` is for tests; `FileCheckpointer` writes one atomic JSON
file per super-step under `.runs/<thread>/` (single-writer / single-process);
`SqliteCheckpointer` stores checkpoints transactionally with atomic per-step
revisions; `RedisCheckpointer` (install the `redis` extra) persists to a Redis
server for distributed / multi-process runners; and `PostgresCheckpointer`
(install the `postgres` extra) persists to a Postgres table. All implement the
same `Checkpointer` protocol, so they are interchangeable at
`compile(checkpointer=...)`.

```python
from agentflow.checkpoint import PostgresCheckpointer
app = g.compile(checkpointer=PostgresCheckpointer("postgresql://localhost/app"))
```

For production durability prefer `PostgresCheckpointer`: a committed row
survives a crash by design. Redis is only crash-durable when you configure
AOF/RDB persistence.

### Optimistic concurrency and retention

Each checkpoint carries a `revision` (the write count for its `(thread, step)`,
set when you read it back). To guard a read-modify-write against a concurrent
resume, pass the revision you read as `if_revision`; a stale value raises
`CheckpointConflict` instead of silently overwriting.

```python
cp = await checkpointer.get(thread, step)
# ... resume work off cp ...
await checkpointer.put(new_cp, if_revision=cp.revision)  # raises on conflict
```

Two retention methods trim old state: `delete_thread(thread)` drops a whole
thread, and `prune(thread, before_step=..., older_than=...)` deletes older
checkpoints and returns how many it removed.

A node calls `await ctx.interrupt(payload)` to suspend the run for a human.
The runtime writes an interrupted checkpoint and stops. Later, `await
app.resume(thread, value=answer)` continues the run, and the same `interrupt`
call returns `answer`. This works across process restarts, since the frontier
is persisted.

## Cross-thread memory (Store)

A checkpointer persists one thread's execution state so it can resume. A
`Store` is the other axis: durable key-value data shared across threads such as
user profiles, learned facts, or long-term agent memory. Items live under a
`namespace` (a tuple of path segments) and a string `key`; the value is any
JSON-serializable object.

```python
from agentflow import MemoryStore

store = MemoryStore()
await store.put(("users", "u1", "memories"), "favorite_color", "blue")
item = await store.get(("users", "u1", "memories"), "favorite_color")
recent = await store.search(("users", "u1"))  # everything under that prefix
```

`MemoryStore` is ephemeral (tests, single process); `PostgresStore` (install
the `postgres` extra) is durable and shared across processes. Both implement
the same `Store` protocol. `put(..., ttl=seconds)` expires an item; expired
items never surface from `get` or `search`.

```python
from agentflow.store import PostgresStore

store = PostgresStore("postgresql://localhost/app")
await store.put(("cache",), "doc-42", payload, ttl=3600)
```

## Control plane (run queue + workers)

`invoke`/`stream`/`resume` run a graph inline in your code. The control plane
lets you manage runs from *outside* that process: enqueue a run, let a pool of
workers execute it, and list, cancel, or resume it from anywhere sharing the
same queue. Runs survive a process restart and scale across workers.

A run request references a graph by name (graphs are code, not data), so an
enqueuer and its workers share a `GraphRegistry` mapping names to compiled
graphs, and the same checkpointer so workers see the run's state.

```python
from agentflow import GraphRegistry, MemoryRunQueue, Worker

registry = GraphRegistry()
registry.register("summarize", lambda: build_graph().compile(checkpointer=cp))

queue = MemoryRunQueue()
run = await queue.enqueue("summarize", {"text": "..."})

# A worker (usually a separate long-running process) drains the queue.
worker = Worker(queue, registry)
await worker.run_once()          # or: await worker.run_forever()

record = await queue.get(run.run_id)   # status: succeeded / interrupted / ...
```

Run lifecycle: `queued -> running -> succeeded | interrupted | failed |
cancelled`. An interrupted run (a graph that hit `ctx.interrupt`) is resumed by
re-queuing it with the human answer:

```python
await queue.enqueue_resume(run.run_id, value="approved")  # back to queued
```

Cancellation is cooperative — `await queue.request_cancel(run_id)` stops a
running graph at the next super-step boundary, leaving a consistent checkpoint.

`MemoryRunQueue` is for a single process/tests. `PostgresRunQueue` (install the
`postgres` extra) is durable and safe for many concurrent workers: `claim`
hands each run to exactly one worker via `FOR UPDATE SKIP LOCKED`, and a
time-bounded lease means a crashed worker's run is re-claimed once the lease
expires.

For monitoring, `await queue.stats()` returns a `QueueStats` snapshot (depth by
status plus `expired_leases` — running runs whose lease is past due, i.e. work
stranded by a crashed worker awaiting re-claim), and `pool.health()` returns a
`PoolHealth` snapshot (`workers`/`alive`/`busy`/`idle`, and `healthy` when every
worker loop is alive). Both are plain values you can expose through your own
`/healthz` and `/metrics` handlers.

```python
stats = await queue.stats()          # stats.queued, stats.running, stats.expired_leases
if not pool.health().healthy:
    ...                              # a worker loop died — pool is degraded
```

An HTTP layer (each queue method maps 1:1 to an endpoint) is planned and not
part of this release.

## Prebuilt patterns

`agentflow.prebuilt.iterate_until_converged(work, ...)` compiles the classic
baseline-then-iterate optimization loop as a graph. You supply an async
`work(state) -> Candidate`; the loop owns the best-so-far, the
no-improvement streak, and the stop decision (`done`, `optimal`, `converged`,
`exhausted`).

```python
from agentflow.prebuilt import Candidate, iterate_until_converged

async def work(state):
    best = state.get("best_score", 0.0)
    return Candidate(score=min(best + 25.0, 100.0))

app = iterate_until_converged(work, perfect_score=100.0, patience=4)
result = await app.invoke({})
```

`agentflow.prebuilt.tool_loop(llm, tools)` compiles the function-calling agent
loop as a two-node graph: an `agent` node calls the `LLMBackend`, a `tools`
node runs any tool the model requested and appends the result to the
conversation, and a conditional edge repeats until the model answers without a
tool call. The LLM never executes a tool itself — the graph does.

```python
from agentflow.prebuilt import Tool, tool_loop
from agentflow.events import Message
from agentflow.backends.ollama import OllamaBackend

async def multiply(a: float, b: float) -> str:
    return str(a * b)

tools = [Tool("multiply", multiply, description="Multiply two numbers",
              schema={"type": "object",
                      "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
                      "required": ["a", "b"]})]

llm = OllamaBackend("gpt-oss")
await llm.start()
app = tool_loop(llm, tools, max_turns=6)
out = await app.invoke({"messages": [Message("user", "What is 23 * 19?")]})
print(out["messages"][-1].content)   # -> the model's final answer
await llm.close()
```

See `examples/tool_loop_ollama.py` for a runnable version, and
`examples/mixed_backends.py` for a single workflow that combines an agent
backend (Codex/Claude/Kiro) with an LLM tool loop, composing a compiled
sub-graph inside a node.

`agentflow.prebuilt` also ships `with_retry(node, retries=..., on=...)` and
`with_timeout(node, seconds=...)` — composable wrappers that add retry with
exponential backoff and per-node timeouts. Both let an `InterruptError`
through untouched, so human-in-the-loop suspends are never retried or timed
out.

## Project layout

```text
agentflow/
  state.py            channels, reducers, update merge
  graph.py            Graph builder + validation
  runtime.py          super-step scheduler, Context, interrupts
  compiled.py         CompiledGraph runnable
  events.py           messages, requests, streaming events
  errors.py           exception hierarchy
  observability.py    Hooks + RunMetrics
  telemetry.py        MultiHooks + JsonlTelemetry (durable JSONL)
  backends/           base protocols + kiro/codex/claude_code/ollama/openai/anthropic
  checkpoint/         Checkpointer protocol + memory/file/sqlite/redis/postgres
  store/              Store protocol (cross-thread memory) + memory/postgres
  controlplane/       RunQueue + GraphRegistry + Worker (memory/postgres)
  prebuilt/           iterate_until_converged, tool_loop, with_retry/with_timeout
examples/             runnable examples
tests/                pytest suite (async; offline by default, `-m live` for real backends)
DESIGN.md             architecture and contracts
```

## Development

```bash
pip install -e '.[ollama,dev]'
pytest -q                 # offline suite only (hermetic, fast)
pytest --cov              # with coverage (source=agentflow, branch)
```

Tests are async and run under `pytest-asyncio` in `auto` mode, so no
per-test decorator is needed. Coverage is opt-in via `--cov` to keep the
default run fast.

Lint, format, and type checks (run in CI):

```bash
ruff check agentflow tests examples
ruff format --check agentflow tests examples
mypy agentflow
pip-audit
```

See `CHANGELOG.md` for the release history.

The suite is split by a `live` marker. The default run skips live tests
(`addopts = -m 'not live'`) so CI stays hermetic — everything mocks its
transport. Live smoke tests spawn the real agent CLIs and hit a real Ollama
server; run them explicitly:

```bash
pytest -m live            # runs only backends that are installed/reachable
KIRO_AGENT=vibe pytest -m live   # include the Kiro backend
```

Each live test skips itself when its backend is absent, so `pytest -m live`
never fails on a missing CLI.

## License

This project is licensed under the Apache License, Version 2.0 (Apache-2.0).
See the [LICENSE](LICENSE) and [NOTICE](NOTICE) files for details.

Copyright 2026 Daniel Sorkin
