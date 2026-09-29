# AgentFlow — Design

An async-first, LangGraph-style library for building production-grade agent
workflows from composable primitives. Agent backends (Kiro, Codex, Claude
Code) are pluggable behind one structured, async interface.

This document is the contract we agree on before implementation. It defines
the layers, the public API surface, and the execution and persistence
semantics. It is not a tutorial.

## Goals

- Build arbitrary workflows as graphs: nodes, edges, conditional routing,
  shared typed state. Not a fixed loop, not a linear chain.
- Swap the agent backend without touching workflow code. Kiro first; Codex
  and Claude Code as additional adapters over the same protocol.
- Async-first: the engine, backends, checkpointer, and streaming are all
  `async`. No blocking calls on the hot path.
- Structured state with per-key reducers (channels), so concurrent node
  updates merge deterministically.
- Fully structured backend streaming: typed events (text, tool call, tool
  result, permission request, turn end, error), never `print`.
- Durable checkpointing built in from day one: every super-step is
  persisted, runs are resumable, and human-in-the-loop interrupts suspend
  and resume a run.

## Non-goals (v1)

- No CLI. This is a library. An app is a separate concern that imports it.
- No task registry / plugin discovery. Users construct graphs in code.
- No distributed execution across machines. Concurrency is asyncio within
  one process.
## Package layout

The library is a single import root, `agentflow`. Old code (`src/`, `tasks/`,
`main.py`, `best/`, `agent_workspace/`) is removed.

```text
agentflow/
  __init__.py            public API re-exports
  state.py               State schema, channels, reducers, update merge
  graph.py               Graph builder: add_node, add_edge, conditional edges
  compiled.py            CompiledGraph: the async runnable + execution loop
  runtime.py             super-step scheduler, fan-out, interrupts
  events.py              typed streaming events (engine + backend)
  errors.py              exception hierarchy
  backends/
    __init__.py
    base.py              Backend / AgentBackend / LLMBackend protocols
    kiro.py              Kiro ACP agent adapter (async JSON-RPC over subprocess)
    ollama.py            Ollama LLM adapter (async HTTP, /api/chat stream)
    # codex.py, claude.py, openai_compat.py later, same protocols
  checkpoint/
    __init__.py
    base.py              Checkpointer protocol + Checkpoint dataclass
    memory.py            in-memory checkpointer
    file.py              JSONL/dir-based durable checkpointer
  prebuilt/
    __init__.py
    loop.py              iterate-until-converged as a reusable subgraph
  telemetry.py           async-friendly structured event sink (kept)
examples/
  hello_graph.py         backend-agnostic smoke test
  optimize_loop.py       loop prebuilt over a backend
tests/
  ...
```

## Layer overview

Four layers, each depending only on the ones above it.

1. State + reducers (`state.py`) — pure data. No async, no I/O.
2. Graph engine (`graph.py`, `compiled.py`, `runtime.py`) — depends on state
   and events. Backend-agnostic. Knows nothing about Kiro.
3. Backends (`backends/`) — implement one of two protocols (`AgentBackend`
   for session agents, `LLMBackend` for stateless chat models), both over a
   common `Backend` base. The engine calls them through a node; backends never
   import the engine.
4. Checkpointing + telemetry (`checkpoint/`, `telemetry.py`) — orthogonal
   services the runtime calls at super-step boundaries.

Nodes are the only place the three lower layers meet: a node receives state,
may call a backend, and returns a state update. The engine merges updates via
reducers and checkpoints the result.
## 1. State model (channels + reducers)

State is a set of named channels. Each channel has a value type and a
reducer: a pure function `(current, update) -> new` that folds a node's
partial update into the current value. This is how concurrent node outputs
merge without last-writer-wins races.

A schema is declared as a typed mapping. The recommended form is a
`TypedDict` annotated with reducers, mirroring LangGraph's ergonomics but
explicit:

```python
from agentflow import State, add, last, append

class ChatState(State):
    messages: Annotated[list[Message], append]   # concatenate
    tokens: Annotated[int, add]                   # sum
    best_score: Annotated[float, last]            # overwrite (default)
```

Reducer contract:

- A reducer is `Callable[[C, U], C]` — current channel value and an incoming
  update produce the new channel value. It must be pure and deterministic.
- `last(current, update) -> update` is the default when no reducer is
  annotated (last-value-wins).
- Built-ins: `last`, `append` (list concat), `add` (numeric/`+`), `merge`
  (dict update), `union` (set). Users supply their own for anything else.
- Order independence is the reducer's responsibility. For fan-out where two
  nodes write the same channel in one super-step, updates are folded in a
  deterministic node order; a reducer that must be commutative (e.g. `add`,
  `union`) is order-safe, `append` preserves node order.

State values are treated as immutable between super-steps. A node returns a
partial update `dict[channel, update]`; it never mutates the state it was
given. The runtime produces the next state by applying each channel's reducer.

Special channels:

- Every state carries engine-managed channels under reserved names
  (prefixed `__`): `__step` (super-step counter), `__next` (pending nodes).
  User channels may not start with `__`.
## 2. Backend taxonomy (structured, async)

There are two fundamentally different kinds of engine we drive, and forcing
one interface on both would lose capability. We model them as a common base
protocol with two specializations. The graph engine depends only on the base;
a node that needs agent-specific or LLM-specific features narrows to the
subtype it constructed.

```text
Backend            common: start / close / invoke -> AsyncIterator[BackendEvent]
├─ AgentBackend    long-lived session; the agent runs tools & asks permission
│    KiroBackend   port of acp.py  (Codex, Claude Code later)
└─ LLMBackend      stateless chat; messages in, token stream out; no built-in tools
     OllamaBackend local http (/api/chat, stream:true)  (OpenAI-compat later)
```

Why the split:

- An agent (Kiro/Codex/Claude) owns a session, calls tools itself, and asks
  the client for permission. Its input is a text prompt; its output is a rich
  event stream including `ToolCall`/`PermissionRequest`.
- An LLM (Ollama and any chat-completions endpoint) is stateless per call:
  input is a message list plus optional tool schemas, output is a token
  stream. It never executes tools itself — if it emits a `ToolCall`, the
  graph decides whether/how to run it and feeds the result back on the next
  call. Sessions and permission prompts do not exist here.

### Common base

```python
class Backend(Protocol):
    async def start(self) -> None: ...
    async def close(self) -> None: ...
    def invoke(self, request: BackendRequest, *, session: str | None = None
               ) -> AsyncIterator[BackendEvent]: ...
```

`invoke` is an async generator: it yields structured events as they arrive
and completes when the turn ends. The terminal event of a successful turn is
`TurnEnd`, carrying the aggregated text and a `stop_reason`. A caller wanting
only the final result drains the generator and reads `TurnEnd`; a caller that
streams consumes events as they arrive. This replaces today's
`prompt() -> dict` + `print`.

`BackendRequest` is a small union so the same `invoke` serves both kinds:

- `TextRequest(text)` — what agents take. `AgentBackend` accepts this.
- `ChatRequest(messages, tools=None, options=None)` — what LLMs take.
  `LLMBackend` accepts this.

The two subtypes add convenience methods and narrow the accepted request:

```python
class AgentBackend(Backend, Protocol):
    permission: PermissionPolicy
    def prompt(self, text: str, *, session=None) -> AsyncIterator[BackendEvent]: ...

class LLMBackend(Backend, Protocol):
    def chat(self, messages: list[Message], *, tools=None, options=None
             ) -> AsyncIterator[BackendEvent]: ...
```

Both `prompt` and `chat` are thin wrappers over `invoke` with the matching
request type, so graph code can stay generic (call `invoke`) while user code
gets an ergonomic, type-narrowed entry point.

### Backend event types (`events.py`)

Typed, frozen dataclasses. One closed union `BackendEvent`:

- `TextChunk(text)` — a streamed delta of the agent's message.
- `ToolCall(id, name, title, args)` — the agent invoked a tool.
- `ToolResult(id, status, content)` — a tool call resolved.
- `PermissionRequest(id, tool, options)` — backend asks the client to allow
  a tool. Resolved via the permission policy (below), not by yielding.
- `TurnEnd(text, stop_reason)` — terminal event of a turn.
- `BackendError(message, detail)` — recoverable protocol/agent error surfaced
  to the caller; fatal transport failures raise instead.

### Permission policy (agent backends only)

Permission handling is a strategy on `AgentBackend`, not hardcoded
auto-approve. `LLMBackend` has no permission concept.

```python
class PermissionPolicy(Protocol):
    async def decide(self, req: PermissionRequest) -> PermissionDecision: ...
```

Built-ins: `AllowAll` (today's behavior, the default), `DenyAll`, and
`Interactive` (raises an engine interrupt so a human resolves it — this is
the HITL path, see checkpointing). A backend answers the underlying protocol
using the policy's decision; the engine never sees the raw RPC.

### Message and tool types

`Message(role, content, tool_calls=None, tool_call_id=None)` is the neutral
chat unit used by `LLMBackend` and by agent event history. A `ToolSpec(name,
description, schema)` describes a tool offered to an LLM. These live in
`events.py` so both backend kinds and the graph share one vocabulary — a node
can take an agent's `ToolCall` event and turn it into a `Message` for an LLM,
or vice versa, without adapters.

### Backend construction

Backends are constructed by the user and injected — never built inside the
engine. Backend-specific config lives in each backend's `__init__`, not in
any engine-level config object:

- `KiroBackend(agent, model=None, engine="v3", permission=AllowAll(), cwd=...)`
  — engine v2/v3, auth method, agent mode all internal.
- `OllamaBackend(model, host="http://localhost:11434", options=None)` — talks
  to Ollama's HTTP API; no subprocess, no session.

This is the fix for today's `AgentLoopRunner.__init__` hard-coupling. Adding a
provider (Codex, Claude Code, an OpenAI-compatible endpoint) is a new class
implementing `AgentBackend` or `LLMBackend` — zero engine changes.
## 3. Graph engine

A workflow is a directed graph of nodes over a shared state schema. This is
the LangGraph-equivalent core, async and backend-agnostic.

### Node

A node is an async callable that reads state and returns a partial update:

```python
Node = Callable[[State, Context], Awaitable[dict[str, Any]]]
```

- Input: the current immutable `State` and a `Context` (run id, config,
  handle to emit engine events, and the checkpointer's `interrupt(...)`).
- Output: a partial update `dict[channel, update]`, merged by reducers. A
  node returning `{}` or `None` makes no change.
- A node never mutates its input state and never writes another node's
  channel directly — it only proposes updates.
- Nodes may call a backend (`await backend.invoke(...)`), spawn concurrent
  work, or be pure functions. Calling a backend is not special to the engine.

`Context` carries an event sink so a node can surface backend events to the
run's stream (`ctx.emit(event)`), and the `interrupt` primitive for HITL.

### Graph builder (`graph.py`)

```python
g = Graph(ChatState)
g.add_node("plan", plan_node)
g.add_node("act", act_node)
g.add_edge(START, "plan")
g.add_edge("plan", "act")
g.add_conditional_edges("act", route, {"loop": "plan", "done": END})
app = g.compile(checkpointer=FileCheckpointer(".runs"))
```

- `add_node(name, fn)` — register a node.
- `add_edge(src, dst)` — unconditional edge; `START`/`END` are sentinels.
- `add_conditional_edges(src, router, mapping)` — `router(state) -> key` (sync
  or async) selects the next node(s) via `mapping`. A router may return a list
  for fan-out.
- `compile(...)` — validate (reachability, unknown targets, dangling nodes)
  and return a `CompiledGraph`. Compilation fails fast on a malformed graph.

### CompiledGraph (`compiled.py`)

The compiled graph is the async runnable:

```python
async def invoke(self, input, *, thread: str, config=None) -> State: ...
def stream(self, input, *, thread: str, config=None
           ) -> AsyncIterator[StreamEvent]: ...
async def resume(self, thread: str, value=None) -> State: ...
```

- `invoke` runs to `END` and returns the final state.
- `stream` yields `StreamEvent`s as the run progresses: node start/end, state
  deltas, and forwarded backend events. This is how a UI or log tails a run.
- `resume` continues a run that was suspended by an interrupt (HITL), optionally
  injecting the human's `value`.
- `thread` identifies a durable run for checkpointing; two calls with the same
  thread continue the same run.
## 4. Execution semantics (super-steps)

Execution is a sequence of super-steps (bulk-synchronous parallel), the same
model LangGraph uses. This makes fan-out, reducers, and checkpointing
well-defined.

One super-step:

1. The runtime has a frontier: the set of nodes scheduled to run now (after
   `START`, the frontier is the targets of `START`'s edges).
2. All frontier nodes run concurrently (`asyncio.gather`) against the same
   immutable input state. They cannot observe each other's updates this step.
3. Their partial updates are collected and folded into the state channel by
   channel via reducers, in a deterministic node order (registration order).
   Commutative reducers (`add`, `union`) are order-independent; `append`
   preserves that order; `last` takes the last writer in that order.
4. The runtime resolves outgoing edges of the nodes that ran (static targets
   plus conditional-router results) to compute the next frontier.
5. A checkpoint is written (see below). `__step` increments.

The run ends when the frontier is empty or reaches `END`. Guardrails: a
configurable `step_limit` aborts runaway graphs (replaces the old
`--iterations` safety valve, now a generic property of any graph).

Determinism: given the same inputs, checkpoints, and pure reducers, a run is
reproducible up to backend nondeterminism (the LLM/agent itself). The engine
adds no hidden ordering.

### Errors

- A node raising propagates as a `NodeError` that aborts the super-step; the
  last good checkpoint is intact, so the run is resumable after a fix.
- A backend `BackendError` event is data a node can branch on; a fatal
  transport failure raises and is treated like a node error.
- Policy for ret/retries is a node concern (a `retry` wrapper is a prebuilt),
  not engine magic.
## 5. Checkpointing and human-in-the-loop

Durable checkpointing is built in, not bolted on. A checkpoint is the full
state plus the pending frontier at a super-step boundary.

```python
@dataclass(frozen=True)
class Checkpoint:
    thread: str
    step: int
    state: Mapping[str, Any]
    next: tuple[str, ...]          # frontier to run on resume
    parent: str | None             # previous checkpoint id (time-travel)
    ts: str

class Checkpointer(Protocol):
    async def put(self, cp: Checkpoint) -> str: ...          # returns id
    async def get(self, thread: str, step: int | None = None) -> Checkpoint | None: ...
    async def history(self, thread: str) -> AsyncIterator[Checkpoint]: ...
```

Implementations:

- `MemoryCheckpointer` — dict-backed, for tests and ephemeral runs.
- `FileCheckpointer` — one directory per thread, one JSON file per step,
  written atomically (reusing the atomic-write helper from the old
  `workspace.py`). Durable and inspectable; survives process restart.

Capabilities this unlocks:

- Resume: `app.resume(thread)` loads the latest checkpoint and continues from
  its frontier. A crashed or killed run picks up where it left off.
- Time-travel: `app.get_state(thread, step=k)` and resume from an earlier
  checkpoint to explore an alternate branch.
- Human-in-the-loop: a node (or the `Interactive` permission policy) calls
  `ctx.interrupt(payload)`. The runtime writes a checkpoint marked
  interrupted and suspends the run, surfacing `payload` to the caller. The
  human inspects it and calls `app.resume(thread, value=answer)`; the node
  resumes as if `interrupt` returned `answer`. This is the same mechanism for
  approvals, edits, and mid-run questions.

Checkpoint frequency is every super-step by default; a compile-time option
allows before/after specific nodes only (interrupt-before/after), matching
LangGraph's `interrupt_before` / `interrupt_after`.
## 6. Backend adapter mappings

### KiroBackend (port of `acp.py`)

Same protocol as today, made async and event-structured:

- `start()` spawns `kiro-cli acp` via `asyncio.create_subprocess_exec`, does
  `initialize` + `session/new`, and (v3) `session/set_mode`; (v2) optional
  `session/set_model`. Engine/auth/model branching stays internal.
- Transport becomes an async read loop over the subprocess stdout; the
  `_request` correlation-by-id logic is preserved but non-blocking.
- `session/prompt` becomes `invoke(TextRequest(text))` yielding events:
  `session/update` chunks → `TextChunk`; `tool_call` → `ToolCall`;
  `tool_call_update` → `ToolResult`; the RPC result → `TurnEnd(text,
  stop_reason)`.
- `session/request_permission` → resolved by the injected `PermissionPolicy`
  (default `AllowAll`, reproducing today's auto-approve) and answered on the
  wire. With `Interactive`, it raises an engine interrupt instead.
- `AcpError` maps to the new `errors.py` hierarchy (`BackendTransportError`).

### OllamaBackend (example LLM backend)

- No subprocess and no session. `start()`/`close()` manage an
  `aiohttp`/`httpx` async client (stdlib-only is a goal; if we forbid deps,
  fall back to `asyncio` + `urllib` streaming, decided in scaffolding).
- `invoke(ChatRequest(messages, tools, options))` POSTs to `/api/chat` with
  `stream: true`, reads the newline-delimited JSON stream, and yields a
  `TextChunk` per token delta, a `ToolCall` if the model emits one
  (Ollama tool-calling), and a final `TurnEnd` with the assembled message and
  `stop_reason` from `done_reason`.
- Serves as the template for any OpenAI-compatible endpoint: swap the URL,
  request shape, and stream parser; the event mapping is identical.

## 7. Prebuilt patterns (`prebuilt/`)

The old fixed loop is not the engine — it is one graph built from primitives,
shipped as a convenience:

- `iterate_until_converged(work_node, score_fn, *, perfect, patience,
  step_limit)` returns a compiled subgraph reproducing the baseline→iterate→
  stop-on-converged/optimal/done behavior, with the verify/promote/restore
  logic expressed as ordinary nodes over channels (`best_score`, `streak`,
  `candidate`). This proves the model subsumes the current `AgentLoopRunner`.
- `tool_loop(llm, tools)` — the classic LLM agent loop: call model, if it
  emits `ToolCall`, run the tool, append `ToolResult` as a `Message`, repeat
  until no tool call. Demonstrates `LLMBackend` + graph-driven tool use.

## 8. Public API (`agentflow/__init__.py`)

Re-exports the stable surface: `State`, reducers (`last`, `append`, `add`,
`merge`, `union`), `Graph`, `START`, `END`, `CompiledGraph`, event types,
`Backend`/`AgentBackend`/`LLMBackend`, `PermissionPolicy` + built-ins,
`Checkpointer` + `MemoryCheckpointer`/`FileCheckpointer`, and the prebuilts.
Backends live under `agentflow.backends.*` so importing the core never pulls
in a subprocess or HTTP dependency.

## 9. Open questions to confirm before coding

1. External deps: is the library allowed non-stdlib deps? Async HTTP for
   Ollama is far cleaner with `httpx`. Options: (a) allow `httpx` (and make
   backends optional extras, `pip install agentflow[ollama]`), or (b) keep a
   strict stdlib-only core and hand-roll streaming. Recommendation: (a) with
   optional extras, core stays dependency-free.
2. State schema mechanism: `TypedDict` + `Annotated[..., reducer]` (static,
   LangGraph-like) vs a class with descriptor fields. Recommendation:
   `TypedDict` + `Annotated`, resolved at `compile()`.
3. Package/distribution name: `agentflow` as the import root — good? And do we
   set up `pyproject.toml` for an installable package now?
4. Python version floor: 3.11 (current) or 3.12? Affects typing ergonomics.

If you're good with the recommendations in section 9, I'll scaffold the
package, delete the old code, and implement bottom-up: state → events →
backends base → graph/runtime → checkpointer → Kiro + Ollama → prebuilts →
example + tests.
