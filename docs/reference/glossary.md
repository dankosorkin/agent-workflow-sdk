# Glossary

The vocabulary of AgentFlow, in one place. Each term links to the chapter where it's explained in full.

### Agent backend
A [backend](#backend) that drives a stateful session and runs its own tools, asking permission before it acts. `KiroBackend`, `CodexBackend`, `ClaudeCodeBackend`. See [agent backends](../backends/agent-backends.md).

### Backend
How a node talks to a model or an agent. Two families — [agent backends](#agent-backend) and [LLM backends](#llm-backend) — share one event vocabulary. See [backends overview](../backends/overview.md).

### Backend event
A unit in the stream a backend produces: `TextChunk`, `ToolCall`, `ToolResult`, `PermissionRequest`, `TurnEnd`, `ErrorEvent`. Distinct from a [stream event](#stream-event). See [backends overview](../backends/overview.md).

### Candidate
The output of the worker in [`iterate_until_converged`](../patterns/prebuilt.md): a score, a payload, and a `done` flag. Distinct from a [gate result](#gate-result) — `Candidate.done` means "the loop may stop", not "this passed a bar".

### Channel
One named field of the [state](#state), with a [reducer](#reducer) that decides how updates to it fold in. See [state and reducers](../concepts/state.md).

### Checkpoint
One persisted point in a run's history, keyed by `(thread, step)`. Holds the full state, the [frontier](#frontier) to resume from, and interrupt details when suspended. See [checkpointing](../durability/checkpointing.md).

### Checkpointer
The store that persists [checkpoints](#checkpoint) so a run can resume, be inspected, or pause for a human. See [checkpointing](../durability/checkpointing.md).

### Compiled graph
The runnable result of `Graph.compile()`. Validated, immutable, driven with `invoke`, `stream`, or `resume`. See [running a graph](../concepts/running.md).

### Context
The per-node handle passed as `ctx`. Carries the node's identity and offers `ctx.emit` and `ctx.interrupt`. See [the Context object](../concepts/context.md).

### Control plane
The durable [run queue](#run-queue) plus [worker](#worker) loop for executing runs out of process. See [the control plane](../durability/control-plane.md).

### Edge
A connection between nodes that drives control flow. A static edge always fires; a conditional edge picks its target by a router's key. See [nodes and edges](../concepts/nodes-and-edges.md).

### Evaluator
A [quality gate](#quality-gate)'s decision function, `async (state, ctx) -> GateResult`. An ordinary node body, not a backend. See [quality gates](../durability/quality-gates.md).

### Frontier
The set of node names a checkpoint will run next. An empty frontier that isn't interrupted means the run finished. See [checkpointing](../durability/checkpointing.md).

### Gate event
A `GateEvent` a [quality gate](#quality-gate) emits via `ctx.emit` on each decision (`started`, `passed`, `failed`, `escalated`), so telemetry shows where a process looped on quality. See [quality gates](../durability/quality-gates.md).

### Gate result
A `GateResult` — an [evaluator](#evaluator)'s verdict: `passed`, plus optional `score`, `reasons`, and `feedback`. See [quality gates](../durability/quality-gates.md).

### Graph
The builder you assemble from nodes and edges over a [state](#state) schema, then `compile()`. See [nodes and edges](../concepts/nodes-and-edges.md).

### Hooks
The observability interface: async lifecycle callbacks the runtime fires as a run unfolds. See [observability](../durability/observability.md).

### Human-in-the-loop (HITL)
Pausing a run for a person: a node awaits `ctx.interrupt`, the state is checkpointed, and `app.resume(thread, value)` continues it. See [human-in-the-loop](../durability/human-in-the-loop.md).

### Idempotency
Running a step once per distinct input. [`skip_if_done`](../patterns/long-running-loops.md) caches a node's update in a [Store](#store), keyed by an [`artifact_key`](#artifact_key) content hash. See [long-running loops](../patterns/long-running-loops.md).

### artifact_key
A stable SHA-256 hash of a JSON-serializable value, used to key an idempotency cache by content. See [long-running loops](../patterns/long-running-loops.md).

### Interrupt
The suspension a node triggers with `await ctx.interrupt(payload)`. On resume it returns the supplied value. See [human-in-the-loop](../durability/human-in-the-loop.md).

### Interrupt payload
The value passed to `ctx.interrupt`. A [structured convention](../durability/quality-gates.md) (`QualityGateReview`, `ClarificationRequest`, and the permission shape) gives it a `type` discriminator so a UI can render each pause. See [human-in-the-loop](../durability/human-in-the-loop.md).

### LLM backend
A stateless [backend](#backend): messages in, token stream out. It never runs tools — a `ToolCall` it emits is a request the graph fulfils. See [LLM backends](../backends/llm-backends.md).

### Node
A unit of work: an async `(state, ctx) -> update` callable that reads state and returns a partial update. Never mutates state. See [nodes and edges](../concepts/nodes-and-edges.md).

### Permission policy
How an [agent backend](#agent-backend) answers a tool-authorization request. `AllowAll`, `DenyAll`, `ToolAllowlist`, `Interactive`, `Callback`, or your own. See [permissions](../backends/permissions.md).

### Quality gate
A checked "good enough?" decision added with [`add_quality_gate`](../durability/quality-gates.md): an [evaluator](#evaluator) plus three-way routing (accept / revise / escalate) and an attempt budget. See [quality gates](../durability/quality-gates.md).

### Reducer
A pure `(current, update) -> new` function that folds a node's update into a [channel](#channel). `last`, `append`, `add`, `merge`, `union`, or custom. See [state and reducers](../concepts/state.md).

### Resume
Continuing a suspended or crashed run from its last [checkpoint](#checkpoint), via `app.resume(thread, value)`. Requires a [checkpointer](#checkpointer). See [human-in-the-loop](../durability/human-in-the-loop.md).

### Retry policy
Configuration for retrying the connect/initial-response phase of a backend HTTP call. Distinct from [`with_retry`](#resilience-wrapper), which retries a whole node. See [retries and rate limits](../backends/retries.md).

### Resilience wrapper
`with_retry` and `with_timeout` — functions that wrap a [node](#node) and return a node. Never catch an [interrupt](#interrupt). See [prebuilt patterns](../patterns/prebuilt.md).

### Run queue
The durable queue of the [control plane](#control-plane). `MemoryRunQueue`, `PostgresRunQueue`. See [the control plane](../durability/control-plane.md).

### State
The typed, shared data a graph operates on — a set of [channels](#channel), each with a [reducer](#reducer). See [state and reducers](../concepts/state.md).

### Store
Durable key-value memory shared *across* runs, keyed by `(namespace, key)` — the counterpart to a [checkpointer](#checkpointer). See [cross-thread memory](../durability/store.md).

### Stream event
A `StreamEvent` from `app.stream`: one observable moment in a run's lifecycle (`node_start`, `node_end`, `backend`, `step`, `interrupt`, `done`). See [running a graph](../concepts/running.md).

### Subgraph
A [compiled graph](#compiled-graph) embedded as a node in another graph, running on an isolated checkpoint sub-thread. See [subgraphs](../concepts/subgraphs.md).

### Super-step
One tick of the [execution model](../concepts/execution.md): every node in the current [frontier](#frontier) runs concurrently, their updates fold into state, and the next frontier is computed. Each super-step is checkpointed. See [the execution model](../concepts/execution.md).

### Thread
The identity of a single run. Its [checkpoints](#checkpoint) and resume point hang off it. See [checkpointing](../durability/checkpointing.md).

### Tool
A callable a model may invoke, described by a `ToolSpec` (name, description, JSON-Schema). See [LLM backends](../backends/llm-backends.md).

### TurnEnd
The single terminal [backend event](#backend-event) of a turn, carrying the assembled text, the stop reason, and a ready-to-append assistant message. See [backends overview](../backends/overview.md).

### Worker
The [control plane](#control-plane) loop that claims runs from a [run queue](#run-queue), resolves the graph, executes it, and reports the outcome. See [the control plane](../durability/control-plane.md).
