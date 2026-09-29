"""Execution runtime: the super-step scheduler.

Runs a compiled graph as a sequence of super-steps (bulk-synchronous
parallel). Each super-step runs the current frontier of nodes concurrently
against the same immutable state, folds their partial updates through the
channel reducers in a deterministic order, resolves outgoing edges to the
next frontier, and checkpoints.

Human-in-the-loop interrupts and resume are implemented here:
``Context.interrupt`` raises on a fresh run (the runtime suspends and
checkpoints); on resume the runtime seeds the node's answer so the same call
returns a value instead of raising.
"""

from __future__ import annotations

import asyncio
import contextvars
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Mapping

from agentflow.checkpoint.base import Checkpoint
from agentflow.errors import InterruptError, NodeError
from agentflow.events import BackendEvent
from agentflow.graph import END, START, _ConditionalEdge
from agentflow.observability import Hooks, _NOOP, _safe
from agentflow.state import Channel, apply_updates

__all__ = ["Context", "StreamEvent", "run", "current_context"]


#: The Context of the node currently executing in this task. Set by the
#: runtime around each node call so helpers that run inside the node's frame
#: (e.g. an Interactive permission policy driven by a backend the node is
#: consuming) can reach ``ctx.interrupt`` without it being threaded through
#: every call. This works because a node and any backend generator it iterates
#: run in the same task, and contextvars are per-task.
current_context: contextvars.ContextVar["Context | None"] = contextvars.ContextVar(
    "agentflow_current_context", default=None
)


# ---------------------------------------------------------------------------
# Streaming events emitted by a run (distinct from backend events)
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class StreamEvent:
    """One observable moment in a run, yielded by ``CompiledGraph.stream``."""
    kind: str  # "node_start" | "node_end" | "backend" | "step" | "interrupt" | "done"
    step: int
    node: str | None = None
    data: Any = None


# ---------------------------------------------------------------------------
# Context handed to every node
# ---------------------------------------------------------------------------

class _Resume:
    """Carries a human's answer for a node awaiting an interrupt on resume."""

    __slots__ = ("value", "used")

    def __init__(self, value: Any) -> None:
        self.value = value
        self.used = False


@dataclass
class Context:
    """Per-node handle: identity, event emission, and interrupts."""

    node: str
    thread: str
    step: int
    _emit_queue: "asyncio.Queue[StreamEvent] | None" = field(default=None, repr=False)
    _resume: _Resume | None = field(default=None, repr=False)

    def emit(self, event: BackendEvent) -> None:
        """Surface a backend event to the run's stream."""
        if self._emit_queue is not None:
            self._emit_queue.put_nowait(
                StreamEvent(kind="backend", step=self.step, node=self.node, data=event)
            )

    async def interrupt(self, payload: Any = None) -> Any:
        """Suspend the run for a human, or return the supplied answer on resume.

        On a fresh run this raises :class:`InterruptError`; the runtime catches
        it, writes an interrupted checkpoint, and stops. When the run is later
        resumed with a value, that value is seeded here and returned directly.
        """
        if self._resume is not None and not self._resume.used:
            self._resume.used = True
            return self._resume.value
        raise InterruptError(self.node, payload)


# ---------------------------------------------------------------------------
# The scheduler
# ---------------------------------------------------------------------------

@dataclass
class _Graph:
    """The compiled structure the runtime executes."""
    channels: Mapping[str, Channel]
    nodes: Mapping[str, Any]
    edges: Mapping[Any, list[Any]]
    branches: Mapping[str, _ConditionalEdge]
    step_limit: int


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _resolve_targets(graph: _Graph, node: str, state: Mapping[str, Any]) -> list[str]:
    """Compute the next nodes to run after ``node`` completes."""
    targets: list[str] = []

    for dst in graph.edges.get(node, ()):
        if dst is not END:
            targets.append(dst)

    branch = graph.branches.get(node)
    if branch is not None:
        key = branch.router(state)
        if asyncio.iscoroutine(key):
            key = await key
        keys = key if isinstance(key, (list, tuple)) else [key]
        for k in keys:
            if k not in branch.mapping:
                raise NodeError(node, f"router returned unmapped key {k!r}")
            dst = branch.mapping[k]
            if dst is not END:
                targets.append(dst)

    return targets


async def _run_node(
    graph: _Graph,
    node: str,
    state: Mapping[str, Any],
    ctx: Context,
    hooks: Hooks = _NOOP,
) -> Mapping[str, Any] | None:
    fn = graph.nodes[node]
    token = current_context.set(ctx)
    await _safe(hooks.on_node_start(ctx.thread, ctx.step, node))
    t0 = time.monotonic()
    try:
        result = fn(state, ctx)
        if asyncio.iscoroutine(result):
            result = await result
        await _safe(hooks.on_node_end(ctx.thread, ctx.step, node, time.monotonic() - t0))
        return result
    except InterruptError:
        # A suspend is not an error; still record the elapsed time.
        await _safe(hooks.on_node_end(ctx.thread, ctx.step, node, time.monotonic() - t0))
        raise
    except Exception as exc:  # noqa: BLE001 - wrap any node failure
        await _safe(hooks.on_node_error(ctx.thread, ctx.step, node, exc))
        raise NodeError(node, str(exc)) from exc
    finally:
        current_context.reset(token)


async def run(
    graph: _Graph,
    initial_state: Mapping[str, Any],
    *,
    thread: str,
    start_frontier: list[str] | None,
    start_step: int,
    checkpointer: Any,
    resume_values: Mapping[str, Any] | None = None,
    emit_queue: "asyncio.Queue[StreamEvent] | None" = None,
    hooks: Hooks = _NOOP,
) -> Checkpoint:
    """Drive super-steps until the graph ends, interrupts, or hits the limit.

    Returns the terminal :class:`Checkpoint` (done or interrupted). ``resume_values``
    maps a node name to the human answer that should satisfy its pending
    ``interrupt`` on the first super-step of a resume.
    """
    # Determine the opening frontier.
    if start_frontier is None:
        frontier = [dst for dst in graph.edges.get(START, ()) if dst is not END]
    else:
        frontier = list(start_frontier)

    state: dict[str, Any] = dict(initial_state)
    step = start_step
    parent_id: str | None = None
    resume_values = dict(resume_values or {})

    async def _emit(ev: StreamEvent) -> None:
        if emit_queue is not None:
            emit_queue.put_nowait(ev)

    await _safe(hooks.on_run_start(thread, step))

    while frontier:
        if step - start_step >= graph.step_limit:
            raise NodeError(
                frontier[0],
                f"step limit {graph.step_limit} exceeded (possible infinite loop)",
            )
        step += 1

        # Build a context per node; seed a resume value if this node is
        # resuming from an interrupt.
        contexts = {
            name: Context(
                node=name,
                thread=thread,
                step=step,
                _emit_queue=emit_queue,
                _resume=_Resume(resume_values[name]) if name in resume_values else None,
            )
            for name in frontier
        }
        resume_values.clear()  # only applies to the first resumed super-step

        for name in frontier:
            await _emit(StreamEvent("node_start", step, node=name))

        # Run the whole frontier concurrently against the same input state.
        coros = [_run_node(graph, name, state, contexts[name], hooks) for name in frontier]
        results = await asyncio.gather(*coros, return_exceptions=True)

        # Handle interrupts: if any node interrupted, suspend the run now. The
        # frontier is preserved so resume re-runs the same super-step.
        for name, res in zip(frontier, results):
            if isinstance(res, InterruptError):
                await _emit(StreamEvent("interrupt", step, node=name, data=res.payload))
                await _safe(hooks.on_run_end(thread, step, completed=False))
                cp = Checkpoint(
                    thread=thread, step=step, state=state, next=tuple(frontier),
                    parent=parent_id, ts=_now(),
                    interrupted=True, interrupt_node=name, interrupt_payload=res.payload,
                )
                if checkpointer is not None:
                    parent_id = await checkpointer.put(cp)
                return cp
            if isinstance(res, BaseException):
                await _safe(hooks.on_run_end(thread, step, completed=False))
                raise res  # NodeError or a real crash

        # No interrupts: fold updates in deterministic frontier order.
        updates = [r for r in results]  # each is a Mapping or None
        state = apply_updates(state, updates, graph.channels)

        for name in frontier:
            await _emit(StreamEvent("node_end", step, node=name))

        # Compute the next frontier from the nodes that just ran.
        next_frontier: list[str] = []
        for name in frontier:
            for dst in await _resolve_targets(graph, name, state):
                if dst not in next_frontier:
                    next_frontier.append(dst)

        cp = Checkpoint(
            thread=thread, step=step, state=state,
            next=tuple(next_frontier), parent=parent_id, ts=_now(),
        )
        if checkpointer is not None:
            parent_id = await checkpointer.put(cp)
        await _emit(StreamEvent("step", step, data=tuple(next_frontier)))
        await _safe(hooks.on_step_end(thread, step, tuple(frontier)))

        frontier = next_frontier

    await _emit(StreamEvent("done", step, data=state))
    await _safe(hooks.on_run_end(thread, step, completed=True))
    return Checkpoint(thread=thread, step=step, state=state, next=(), parent=parent_id, ts=_now())
