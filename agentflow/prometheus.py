# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of AgentFlow.
#
# AgentFlow is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""Prometheus exporter as a :class:`~agentflow.observability.Hooks`.

Records run/node/step counters, node duration histograms, and backend-event
counts as Prometheus metrics. This is purely a listener over the existing
hooks — the engine is unaware of it — for operators who scrape Prometheus but
don't run an OpenTelemetry collector.

By default it uses a private :class:`CollectorRegistry` (not the process-global
one), so multiple exporters and tests don't clash; pass ``registry=`` to route
metrics into a shared registry (e.g. the default one a WSGI ``/metrics`` app
scrapes). Call :meth:`exposition` to render the text format for an endpoint.

Requires the ``prometheus`` extra:
``pip install 'agentic-workflow-sdk[prometheus]'``.

    from agentflow.prometheus import PrometheusHooks
    metrics = PrometheusHooks()
    app = g.compile(hooks=metrics)
    # ... in an HTTP handler:
    body, content_type = metrics.exposition()
"""

from __future__ import annotations

from typing import Any

from agentflow.observability import Hooks

try:
    from prometheus_client import (
        CollectorRegistry,
        Counter,
        Gauge,
        Histogram,
        generate_latest,
    )
    from prometheus_client.exposition import CONTENT_TYPE_LATEST
except ModuleNotFoundError as exc:  # pragma: no cover - import-time guard
    raise ModuleNotFoundError(
        "PrometheusHooks requires prometheus-client. Install the extra: "
        "pip install 'agentic-workflow-sdk[prometheus]'"
    ) from exc

__all__ = ["PrometheusHooks"]

_PREFIX = "agentflow"

#: Default histogram buckets (seconds) tuned for node execution latency —
#: sub-millisecond pure-Python nodes up to multi-second LLM/tool calls.
_DEFAULT_BUCKETS = (
    0.001,
    0.005,
    0.01,
    0.05,
    0.1,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
)


class PrometheusHooks(Hooks):
    """Emit Prometheus metrics for runs and nodes.

    Metrics (all prefixed ``agentflow_``):

    - ``runs_started_total`` / ``runs_completed_total`` (labels: ``outcome`` =
      completed|incomplete) — run lifecycle counters.
    - ``runs_in_progress`` — gauge of currently executing runs.
    - ``steps_total`` — super-steps executed.
    - ``node_runs_total`` (labels: ``node``, ``outcome`` = ok|error).
    - ``node_duration_seconds`` (label: ``node``) — histogram.
    - ``backend_events_total`` (label: ``event_type``).
    """

    def __init__(
        self,
        registry: CollectorRegistry | None = None,
        *,
        buckets: tuple[float, ...] = _DEFAULT_BUCKETS,
    ) -> None:
        # A private registry by default keeps instances isolated (tests,
        # multiple graphs) and avoids duplicate-timeseries errors on the global.
        self.registry = registry if registry is not None else CollectorRegistry()

        self._runs_started = Counter(
            f"{_PREFIX}_runs_started_total",
            "Runs started.",
            registry=self.registry,
        )
        self._runs_completed = Counter(
            f"{_PREFIX}_runs_completed_total",
            "Runs finished, by outcome.",
            ["outcome"],
            registry=self.registry,
        )
        self._runs_in_progress = Gauge(
            f"{_PREFIX}_runs_in_progress",
            "Runs currently executing.",
            registry=self.registry,
        )
        self._steps = Counter(
            f"{_PREFIX}_steps_total",
            "Super-steps executed.",
            registry=self.registry,
        )
        self._node_runs = Counter(
            f"{_PREFIX}_node_runs_total",
            "Node executions, by node and outcome.",
            ["node", "outcome"],
            registry=self.registry,
        )
        self._node_duration = Histogram(
            f"{_PREFIX}_node_duration_seconds",
            "Node execution duration in seconds.",
            ["node"],
            buckets=buckets,
            registry=self.registry,
        )
        self._backend_events = Counter(
            f"{_PREFIX}_backend_events_total",
            "Backend events surfaced by nodes, by type.",
            ["event_type"],
            registry=self.registry,
        )

    # ------------------------------------------------------------------

    async def on_run_start(self, thread: str, step: int) -> None:
        self._runs_started.inc()
        self._runs_in_progress.inc()

    async def on_node_end(self, thread: str, step: int, node: str, seconds: float) -> None:
        self._node_runs.labels(node=node, outcome="ok").inc()
        self._node_duration.labels(node=node).observe(seconds)

    async def on_node_error(self, thread: str, step: int, node: str, exc: BaseException) -> None:
        self._node_runs.labels(node=node, outcome="error").inc()

    async def on_step_end(self, thread: str, step: int, ran: tuple[str, ...]) -> None:
        self._steps.inc()

    async def on_run_end(self, thread: str, step: int, completed: bool) -> None:
        self._runs_in_progress.dec()
        self._runs_completed.labels(outcome="completed" if completed else "incomplete").inc()

    async def on_event(self, thread: str, step: int, node: str, event: Any) -> None:
        self._backend_events.labels(event_type=type(event).__name__).inc()

    # ------------------------------------------------------------------

    def exposition(self) -> tuple[bytes, str]:
        """Render the current metrics as the Prometheus text exposition format.

        Returns ``(body, content_type)`` ready to serve from a ``/metrics``
        endpoint (any framework — you own the HTTP layer)."""
        return generate_latest(self.registry), CONTENT_TYPE_LATEST
