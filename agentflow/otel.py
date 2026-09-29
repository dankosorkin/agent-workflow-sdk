"""OpenTelemetry exporter as a :class:`~agentflow.observability.Hooks`.

Turns a run into OTel spans: one span per run (per thread) and a child span
per node execution, with the node's duration, errors recorded as span events,
and backend events (from ``ctx.emit``) added as span events. This is purely a
listener over the existing hooks — the engine is unaware of it.

Requires the ``otel`` extra:
``pip install 'agentic-workflow-sdk[otel]'``.

    from agentflow.otel import OtelHooks
    app = g.compile(hooks=OtelHooks())              # uses the global tracer
    # or compose with others:
    from agentflow import MultiHooks, RunMetrics
    app = g.compile(hooks=MultiHooks(RunMetrics(), OtelHooks()))
"""

from __future__ import annotations

from typing import Any

from agentflow.observability import Hooks

try:
    from opentelemetry import trace
    from opentelemetry.trace import Span, Status, StatusCode, Tracer
except ModuleNotFoundError as exc:  # pragma: no cover - import-time guard
    raise ModuleNotFoundError(
        "OtelHooks requires OpenTelemetry. Install the extra: "
        "pip install 'agentic-workflow-sdk[otel]'"
    ) from exc

__all__ = ["OtelHooks", "EVENT_SCHEMA_VERSION"]

#: Version of the span attribute / event schema emitted here. Bump on a
#: breaking change to attribute names so consumers can gate on it.
EVENT_SCHEMA_VERSION = "1"

_PREFIX = "agentflow"


class OtelHooks(Hooks):
    """Emit OpenTelemetry spans for runs and nodes.

    ``tracer`` defaults to the global tracer for this library; pass your own to
    route spans into a specific provider. Spans are keyed by ``(thread, step,
    node)`` so concurrent nodes in one super-step each get their own span.
    """

    def __init__(self, tracer: Tracer | None = None) -> None:
        self._tracer: Tracer = tracer or trace.get_tracer(_PREFIX)
        self._run_spans: dict[str, Span] = {}
        self._node_spans: dict[tuple[str, int, str], Span] = {}

    # ------------------------------------------------------------------

    async def on_run_start(self, thread: str, step: int) -> None:
        span = self._tracer.start_span(f"{_PREFIX}.run")
        span.set_attribute(f"{_PREFIX}.schema_version", EVENT_SCHEMA_VERSION)
        span.set_attribute(f"{_PREFIX}.thread", thread)
        span.set_attribute(f"{_PREFIX}.start_step", step)
        self._run_spans[thread] = span

    async def on_node_start(self, thread: str, step: int, node: str) -> None:
        span = self._tracer.start_span(f"{_PREFIX}.node.{node}")
        span.set_attribute(f"{_PREFIX}.thread", thread)
        span.set_attribute(f"{_PREFIX}.step", step)
        span.set_attribute(f"{_PREFIX}.node", node)
        self._node_spans[(thread, step, node)] = span

    async def on_node_end(self, thread: str, step: int, node: str, seconds: float) -> None:
        span = self._node_spans.pop((thread, step, node), None)
        if span is not None:
            span.set_attribute(f"{_PREFIX}.duration_s", round(seconds, 6))
            span.set_status(Status(StatusCode.OK))
            span.end()

    async def on_node_error(self, thread: str, step: int, node: str, exc: BaseException) -> None:
        span = self._node_spans.pop((thread, step, node), None)
        if span is not None:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            span.end()

    async def on_step_end(self, thread: str, step: int, ran: tuple[str, ...]) -> None:
        run = self._run_spans.get(thread)
        if run is not None:
            run.add_event(
                f"{_PREFIX}.step",
                attributes={f"{_PREFIX}.step": step, f"{_PREFIX}.ran": list(ran)},
            )

    async def on_run_end(self, thread: str, step: int, completed: bool) -> None:
        span = self._run_spans.pop(thread, None)
        if span is not None:
            span.set_attribute(f"{_PREFIX}.completed", completed)
            span.set_attribute(f"{_PREFIX}.end_step", step)
            span.set_status(Status(StatusCode.OK if completed else StatusCode.ERROR))
            span.end()

    async def on_event(self, thread: str, step: int, node: str, event: Any) -> None:
        span = self._node_spans.get((thread, step, node)) or self._run_spans.get(thread)
        if span is not None:
            span.add_event(
                f"{_PREFIX}.backend_event",
                attributes={
                    f"{_PREFIX}.node": node,
                    f"{_PREFIX}.event_type": type(event).__name__,
                },
            )
