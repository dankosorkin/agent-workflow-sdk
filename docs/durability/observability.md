# Observability

You cannot operate what you cannot see. AgentFlow exposes a run's lifecycle through hooks — callbacks fired at run, step, node, and event boundaries. From those hooks come metrics, durable telemetry, OpenTelemetry spans, and Prometheus counters. This chapter covers all of them, plus redaction.

## Hooks

`Hooks` is a no-op base class of async lifecycle callbacks. Subclass it and override the ones you care about, then attach it at `compile(hooks=...)`.

```python
class Hooks:
    async def on_run_start(self, thread, step): ...
    async def on_node_start(self, thread, step, node): ...
    async def on_node_end(self, thread, step, node, seconds): ...
    async def on_node_error(self, thread, step, node, exc): ...
    async def on_step_end(self, thread, step, ran): ...
    async def on_run_end(self, thread, step, completed): ...
    async def on_event(self, thread, step, node, event): ...   # a ctx.emit'd backend event
```

The one iron rule: a hook must never break a run. Every hook is awaited through a guard that swallows any exception it raises. Instrumentation failing is never allowed to fail the workflow.

## RunMetrics — per-node and per-run counters

`RunMetrics` is a ready-made `Hooks` that accumulates call counts, errors, and durations per node.

```python
from agentflow import RunMetrics

m = RunMetrics()
await g.compile(hooks=m).invoke(inp)
print(m.summary())
# {"steps": 3, "completed": True,
#  "nodes": {"tick": {"calls": 5, "errors": 0, "avg_seconds": ..., "max_seconds": ...}}}
```

A quick way to see where time goes and whether a run completed, with zero setup.

## JsonlTelemetry — durable event log

`JsonlTelemetry` writes one JSON line per event to a file, giving you a durable, greppable trace of every run.

```python
from agentflow import JsonlTelemetry

app = g.compile(hooks=JsonlTelemetry(".runs"))   # directory -> one file per thread
```

Pass a directory to get `<dir>/<thread>.jsonl` per run, or a `.jsonl` file path to write everything to one file. Each line records the event type (`run_start`, `node_start`, `node_end`, `node_error`, `event`, `step`, `run_end`), the thread, step, and a timestamp. Backend events you surfaced with `ctx.emit` are logged too, with long text truncated so the log stays readable. Writes are serialized under a lock so concurrent nodes never interleave partial lines. A runnable example lives in `examples/telemetry_demo.py`.

## Composing listeners with MultiHooks

You usually want several listeners at once — metrics and a log, say. `MultiHooks` fans every callback out to each child, under the same swallow-exceptions guarantee, so a failing listener never breaks the others or the run.

```python
from agentflow import MultiHooks, RunMetrics, JsonlTelemetry

metrics = RunMetrics()
telemetry = JsonlTelemetry(".runs")
app = g.compile(hooks=MultiHooks(metrics, telemetry))
```

## OpenTelemetry spans

For distributed tracing, `OtelHooks` emits a span per run and per node against the global tracer. Install the `otel` extra.

```python
from agentflow.otel import OtelHooks
app = g.compile(hooks=OtelHooks())
```

## Prometheus metrics

For a `/metrics` endpoint, `PrometheusHooks` records run/node/step counters, a node-duration histogram, and backend-event counts. Install the `prometheus` extra. It uses a private registry by default; call `exposition()` to render the text format — you own the HTTP layer.

```python
from agentflow.prometheus import PrometheusHooks

metrics = PrometheusHooks()
app = g.compile(hooks=metrics)

# inside your /metrics handler:
body, content_type = metrics.exposition()
```

Compose these too: `MultiHooks(RunMetrics(), OtelHooks(), PrometheusHooks())`.

## Redaction

Telemetry and checkpoints contain prompts, model output, and tool arguments — potentially secrets. A `Redactor` masks sensitive values before they are written. `RedactKeys` recursively masks dict values whose key matches a sensitive fragment (case-insensitive substring), covering common names like `api_key`, `authorization`, `token`, `password`, and more out of the box.

```python
from agentflow import RedactKeys, JsonlTelemetry

telemetry = JsonlTelemetry(".runs", redact=RedactKeys())
# a key like "OPENAI_API_KEY" is written as "***REDACTED***"
```

The default redactor, `redact_none`, is a no-op that persists data as-is. Note that redacting a checkpoint makes it non-resumable to the exact original state, since masked values are lost — use redaction for telemetry you keep, not for state you plan to resume from.

## Choosing what to attach

- Development: `RunMetrics` for a quick summary, `JsonlTelemetry` when you want to inspect a run after the fact.
- Production with tracing: `OtelHooks` into your existing collector.
- Production with Prometheus scraping: `PrometheusHooks` behind your `/metrics`.
- Any of the above together: wrap them in `MultiHooks`.
- Anywhere secrets might appear in state: add a `RedactKeys` redactor.

That completes durability and operations. Next: [prebuilt patterns](../patterns/prebuilt.md).
