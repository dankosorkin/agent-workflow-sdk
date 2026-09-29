"""Observability: lifecycle hooks and basic run metrics.

Attach a :class:`Hooks` implementation at ``compile(hooks=...)`` to observe a
run without changing its behavior. All callbacks are optional; subclass
:class:`Hooks` and override only what you need. Hooks are awaited, so they can
do async I/O (emit to a log sink, push a metric), but they must not raise —
an exception in a hook is swallowed so instrumentation can never break a run.

:class:`RunMetrics` is a ready-made :class:`Hooks` that records per-node
durations, counts, and errors, plus step and run totals.
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Mapping

__all__ = ["Hooks", "RunMetrics", "NodeStat"]


class Hooks:
    """No-op base for run lifecycle callbacks. Override selectively.

    Every method is async and must not raise. Timing is the runtime's concern;
    hooks receive already-measured values where relevant.
    """

    async def on_run_start(self, thread: str, step: int) -> None: ...
    async def on_node_start(self, thread: str, step: int, node: str) -> None: ...
    async def on_node_end(self, thread: str, step: int, node: str, seconds: float) -> None: ...
    async def on_node_error(self, thread: str, step: int, node: str, exc: BaseException) -> None: ...
    async def on_step_end(self, thread: str, step: int, ran: tuple[str, ...]) -> None: ...
    async def on_run_end(self, thread: str, step: int, completed: bool) -> None: ...


@dataclass
class NodeStat:
    """Aggregated stats for one node across a run."""
    calls: int = 0
    errors: int = 0
    total_seconds: float = 0.0
    max_seconds: float = 0.0

    @property
    def avg_seconds(self) -> float:
        return self.total_seconds / self.calls if self.calls else 0.0


@dataclass
class RunMetrics(Hooks):
    """A Hooks implementation that accumulates per-node and per-run metrics."""

    nodes: dict[str, NodeStat] = field(default_factory=lambda: defaultdict(NodeStat))
    steps: int = 0
    started: bool = False
    completed: bool = False

    async def on_run_start(self, thread: str, step: int) -> None:
        self.started = True

    async def on_node_end(self, thread: str, step: int, node: str, seconds: float) -> None:
        stat = self.nodes[node]
        stat.calls += 1
        stat.total_seconds += seconds
        stat.max_seconds = max(stat.max_seconds, seconds)

    async def on_node_error(self, thread: str, step: int, node: str, exc: BaseException) -> None:
        self.nodes[node].errors += 1

    async def on_step_end(self, thread: str, step: int, ran: tuple[str, ...]) -> None:
        self.steps += 1

    async def on_run_end(self, thread: str, step: int, completed: bool) -> None:
        self.completed = completed

    def summary(self) -> dict[str, Any]:
        return {
            "steps": self.steps,
            "completed": self.completed,
            "nodes": {
                name: {
                    "calls": s.calls,
                    "errors": s.errors,
                    "avg_seconds": round(s.avg_seconds, 6),
                    "max_seconds": round(s.max_seconds, 6),
                }
                for name, s in self.nodes.items()
            },
        }


_NOOP = Hooks()


async def _safe(coro) -> None:
    """Await a hook coroutine, swallowing any exception it raises."""
    try:
        await coro
    except Exception:  # noqa: BLE001 - instrumentation must never break a run
        pass
