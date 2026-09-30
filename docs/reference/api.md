# API cheatsheet

A one-page map of the public surface. Everything here is importable from the top-level `agentflow` package unless noted; backends live under `agentflow.backends.*` and prebuilt patterns under `agentflow.prebuilt`. For the reasoning behind each piece, follow the links into the guide. For full generated signatures, see the [API Reference](api/state.md).

## State — [guide](../concepts/state.md)

```python
class MyState(State):
    field: Annotated[T, reducer]     # a channel + how updates fold in
```

| Reducer | Folds `(current, update)` into |
| --- | --- |
| `last` | the update (default) |
| `append` | list concatenation |
| `add` | numeric sum |
| `merge` | dict merge (update wins) |
| `union` | set union |

## Graph — [guide](../concepts/nodes-and-edges.md)

```python
g = Graph(MyState)
g.add_node(name, fn)                              # fn: async (state, ctx) -> dict | None
g.add_edge(src, dst)                              # START, END, or node name
g.add_conditional_edges(src, router, mapping)     # router(state) -> key (or list of keys)
g.add_subgraph(name, compiled, input_map=..., output_map=...)
app = g.compile(checkpointer=None, step_limit=100, hooks=None,
                max_node_concurrency=None, isolate_state="fanout")
```

## Running — [guide](../concepts/running.md)

```python
await app.invoke(input, thread="default", timeout=None) -> dict
async for ev in app.stream(input, thread="default"): ...      # StreamEvent
await app.resume(thread, value=None, timeout=None) -> dict
async for ev in app.stream_resume(thread, value=None): ...
await app.get_state(thread, step=None) -> Checkpoint | None
async for cp in app.history(thread): ...
app.invoke_sync(...) / app.resume_sync(...) / app.stream_sync(...)
```

`StreamEvent.kind` ∈ `node_start | node_end | backend | step | interrupt | done`.

## Context — [guide](../concepts/context.md)

```python
async def node(state, ctx):
    ctx.emit(event)                        # backend event or GateEvent -> stream + telemetry
    answer = await ctx.interrupt(payload)  # suspend for a human; returns the resume value
    ctx.node, ctx.thread, ctx.step
```

## Backends — [guide](../backends/overview.md)

```python
await backend.start(); async for ev in backend.invoke(request): ...; await backend.close()
# or: async with backend: ...

# LLM backends — chat(messages, tools=None, options=None)
from agentflow.backends.ollama import OllamaBackend
OllamaBackend(model, host="http://localhost:11434", options=None, timeout=120.0,
              retry=None, max_concurrency=None)
from agentflow.backends.openai import OpenAIBackend
OpenAIBackend(model, base_url="https://api.openai.com/v1", api_key=None, headers=None,
              options=None, timeout=120.0, retry=None, max_concurrency=None)
from agentflow.backends.anthropic import AnthropicBackend
AnthropicBackend(model, api_key=None, base_url="https://api.anthropic.com", max_tokens=1024,
                 anthropic_version="2023-06-01", workspace_id=None, headers=None,
                 options=None, timeout=120.0, retry=None, max_concurrency=None)

# Agent backends — prompt(text); permission is required
from agentflow.backends.kiro import KiroBackend
KiroBackend(agent, model=None, engine="v3", cwd=".", permission=<required>, close_grace=0.5)
```

Generation settings (`temperature`, `top_p`, …) go in the `options` dict — no typed params.

## Events — [guide](../backends/overview.md)

```python
TextRequest(text)
ChatRequest(messages, tools=(), options={})
Message(role, content="", tool_calls=(), tool_call_id=None, name=None)  # role: system|user|assistant|tool
ToolSpec(name, description="", schema={})
ToolCallSpec(id, name, args={})

# BackendEvent
TextChunk(text)
ToolCall(id, name, args={}, title=None)
ToolResult(id, status="ok", content=None)
PermissionRequest(id, tool, options=(), detail={})
PermissionOption(option_id, name, kind)
TurnEnd(text="", stop_reason=None, message=None)
ErrorEvent(message, detail={})
GateEvent(gate, phase, attempt=0, score=None, reasons=())   # emitted by quality gates
```

## Permissions — [guide](../backends/permissions.md)

```python
from agentflow.backends.base import AllowAll, DenyAll, ToolAllowlist, Interactive, Callback
AllowAll(); DenyAll(); ToolAllowlist(allowed, fallback=DenyAll())
Interactive(); Callback(resolver)   # resolver: async (PermissionRequest) -> decision
Allow(option_id=None, remember=False); Deny(reason=None)
```

## Retries — [guide](../backends/retries.md)

```python
from agentflow.backends._http import RetryPolicy
RetryPolicy(max_retries=2, backoff=0.5, factor=2.0, max_backoff=30.0,
            jitter=0.25, respect_retry_after=True)
```

## Checkpointing — [guide](../durability/checkpointing.md)

```python
MemoryCheckpointer()
FileCheckpointer(root=".runs", redact=None, secure_permissions=True)
SqliteCheckpointer(path=".runs/checkpoints.db", redact=None)
PostgresCheckpointer(dsn=..., pool=None, table="checkpoints", redact=None)   # [postgres]
RedisCheckpointer(url=..., client=None, prefix="agentflow", redact=None)     # [redis]

Checkpoint(thread, step, state, next=(), parent=None, ts="",
           interrupted=False, interrupt_node=None, interrupt_payload=None, revision=0)
Checkpoint.done   # not next and not interrupted
```

## Store — [guide](../durability/store.md)

```python
await store.put(namespace, key, value, ttl=None) -> Item
await store.get(namespace, key) -> Item | None
await store.search(prefix, limit=..., offset=...) -> list[Item]
await store.delete(namespace, key) -> bool
MemoryStore(); PostgresStore(dsn)   # [postgres]
```

## Control plane — [guide](../durability/control-plane.md)

```python
registry = GraphRegistry(); registry.register(name, factory)   # factory: () -> CompiledGraph
queue = MemoryRunQueue()                                        # or PostgresRunQueue(dsn=...) [postgres]
await queue.enqueue(graph_name, input=None, thread=None) -> RunRecord
await queue.enqueue_resume(run_id, value=None)
await queue.request_cancel(run_id) -> bool
worker = Worker(queue, registry, lease_seconds=60.0, heartbeat_interval=15.0, poll_interval=1.0)
await worker.run_once() -> bool; await worker.run_forever(); worker.stop()
pool = WorkerPool(queue, registry, concurrency=4)
await pool.start(); await pool.stop(); pool.health() -> PoolHealth
```

`RunStatus`: `QUEUED RUNNING SUCCEEDED INTERRUPTED FAILED CANCELLED`.

## Observability — [guide](../durability/observability.md)

```python
class MyHooks(Hooks):
    async def on_run_start / on_node_start / on_node_end(seconds) / on_node_error /
             on_step_end(ran) / on_run_end(completed) / on_event(event): ...
RunMetrics()                       # .summary()
JsonlTelemetry(path, per_thread=None, redact=None, secure_permissions=True)
MultiHooks(*hooks)                 # .add(hook)
from agentflow.otel import OtelHooks            # [otel]
from agentflow.prometheus import PrometheusHooks  # [prometheus] — .exposition()
```

## Prebuilt patterns — [guide](../patterns/prebuilt.md)

```python
from agentflow.prebuilt import (
    tool_loop, Tool, ToolLoopState,
    iterate_until_converged, Candidate,
    with_retry, with_timeout,
    GateResult, GateState, Evaluator, add_quality_gate,
    artifact_key, skip_if_done,
)

tool_loop(llm, tools, max_turns=10, stream_text=False, checkpointer=None)
iterate_until_converged(work, perfect_score=100.0, patience=4, max_iterations=20, checkpointer=None)
with_retry(node, retries=2, backoff=0.1, backoff_factor=2.0, max_backoff=30.0, on=(Exception,), jitter=0.0)
with_timeout(node, seconds)
add_quality_gate(g, name, evaluator, *, on_pass, on_fail, on_escalate, max_attempts=3)
skip_if_done(node, store, *, namespace, key_from, ttl=None); artifact_key(value) -> str
```

## Interrupt payloads — [guide](../durability/quality-gates.md)

```python
from agentflow.interrupts import QualityGateReview, ClarificationRequest, interrupt_type
QualityGateReview(artifact, criteria=[], auto_score=None, reasons=[], gate=None).to_payload()
ClarificationRequest(question, options=[], context={}).to_payload()
interrupt_type(payload) -> "quality_gate" | "permission" | "clarification" | None
```

## Optional extras

Install with `pip install "agent-workflow-sdk[<extra>]"`.

| Extra | Enables |
| --- | --- |
| `ollama` | the HTTP LLM backends via `httpx` |
| `postgres` | `PostgresCheckpointer`, `PostgresStore`, `PostgresRunQueue` |
| `redis` | `RedisCheckpointer` |
| `otel` | `OtelHooks` |
| `prometheus` | `PrometheusHooks` |

Unsure what a term means? The [glossary](glossary.md) defines every concept.
