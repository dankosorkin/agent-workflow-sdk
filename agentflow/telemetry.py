"""Persistent telemetry as a :class:`~agentflow.observability.Hooks`.

Attach at ``compile(hooks=...)`` to durably record a run. Two pieces:

- :class:`MultiHooks` composes several ``Hooks`` so a run can, say, collect
  in-memory metrics AND write a JSONL log AND push a custom sink at once.
- :class:`JsonlTelemetry` writes one JSON-per-line event stream to a file:
  ``run_start``, ``node_start``, ``node_end``, ``node_error``, ``event``
  (backend events surfaced via ``ctx.emit``), ``step``, ``run_end``. Each line
  is a flat object with a UTC timestamp, thread, and step.

Nothing here is required by the engine; it is one more optional listener.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from agentflow.observability import Hooks, _safe

__all__ = ["MultiHooks", "JsonlTelemetry"]


class MultiHooks(Hooks):
    """Fan every lifecycle callback out to several :class:`Hooks`.

    Each child is invoked through the same swallow-exceptions guarantee as a
    single hook, so one misbehaving listener cannot break the others or the run.
    """

    def __init__(self, *hooks: Hooks) -> None:
        self._hooks: tuple[Hooks, ...] = tuple(hooks)

    def add(self, hook: Hooks) -> "MultiHooks":
        self._hooks += (hook,)
        return self

    async def on_run_start(self, thread, step):
        for h in self._hooks:
            await _safe(h.on_run_start(thread, step))

    async def on_node_start(self, thread, step, node):
        for h in self._hooks:
            await _safe(h.on_node_start(thread, step, node))

    async def on_node_end(self, thread, step, node, seconds):
        for h in self._hooks:
            await _safe(h.on_node_end(thread, step, node, seconds))

    async def on_node_error(self, thread, step, node, exc):
        for h in self._hooks:
            await _safe(h.on_node_error(thread, step, node, exc))

    async def on_step_end(self, thread, step, ran):
        for h in self._hooks:
            await _safe(h.on_step_end(thread, step, ran))

    async def on_run_end(self, thread, step, completed):
        for h in self._hooks:
            await _safe(h.on_run_end(thread, step, completed))

    async def on_event(self, thread, step, node, event):
        for h in self._hooks:
            await _safe(h.on_event(thread, step, node, event))


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class JsonlTelemetry(Hooks):
    """Append run events to a JSON Lines file.

    Pass a directory to get ``<dir>/<thread>.jsonl`` per run thread, or a file
    path to write everything to one file. Writes are appended under an async
    lock so concurrent nodes in a super-step don't interleave partial lines.
    """

    def __init__(self, path: Path | str, *, per_thread: bool | None = None) -> None:
        p = Path(path)
        # Heuristic: a path without a .jsonl suffix is treated as a directory.
        self._is_dir = per_thread if per_thread is not None else (p.suffix == "")
        self.path = p
        self._lock = asyncio.Lock()
        if self._is_dir:
            self.path.mkdir(parents=True, exist_ok=True)
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)

    def _file_for(self, thread: str) -> Path:
        if self._is_dir:
            safe = thread.replace("/", "_").replace("\\", "_")
            return self.path / f"{safe}.jsonl"
        return self.path

    async def _write(self, thread: str, event: str, **data: Any) -> None:
        entry = {"ts": _now(), "thread": thread, "event": event, **data}
        line = json.dumps(entry, ensure_ascii=False, default=_json_default) + "\n"
        async with self._lock:
            # Local file append is fast; running it inline keeps ordering
            # deterministic within the lock.
            with self._file_for(thread).open("a", encoding="utf-8") as fh:
                fh.write(line)

    async def on_run_start(self, thread, step):
        await self._write(thread, "run_start", step=step)

    async def on_node_start(self, thread, step, node):
        await self._write(thread, "node_start", step=step, node=node)

    async def on_node_end(self, thread, step, node, seconds):
        await self._write(thread, "node_end", step=step, node=node,
                          seconds=round(seconds, 6))

    async def on_node_error(self, thread, step, node, exc):
        await self._write(thread, "node_error", step=step, node=node,
                          error=f"{type(exc).__name__}: {exc}")

    async def on_step_end(self, thread, step, ran):
        await self._write(thread, "step", step=step, ran=list(ran))

    async def on_run_end(self, thread, step, completed):
        await self._write(thread, "run_end", step=step, completed=completed)

    async def on_event(self, thread, step, node, event):
        await self._write(thread, "event", step=step, node=node,
                          eventType=type(event).__name__, payload=_summarize(event))


def _json_default(obj: Any) -> Any:
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return dataclasses.asdict(obj)
    if isinstance(obj, (set, frozenset, tuple)):
        return list(obj)
    return str(obj)


def _summarize(event: Any) -> Any:
    """Compact a backend event for the log; dataclasses -> dict, else str."""
    if dataclasses.is_dataclass(event) and not isinstance(event, type):
        d = dataclasses.asdict(event)
        # Truncate long text chunks so the log stays readable.
        text = d.get("text")
        if isinstance(text, str) and len(text) > 500:
            d["text"] = text[:500] + f"...(+{len(text) - 500} chars)"
        return d
    return str(event)
