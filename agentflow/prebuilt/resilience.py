"""Resilience wrappers for nodes: retry and timeout.

Both take a node and return a node, so they compose:

    node = with_retry(with_timeout(flaky, seconds=10), retries=3)

Design rules:

- An :class:`~agentflow.errors.InterruptError` is never caught. A node that
  suspends for a human (HITL) must propagate cleanly; retrying or timing out an
  interrupt would be wrong.
- On give-up, the original exception is re-raised. The runtime turns any node
  exception into a :class:`~agentflow.errors.NodeError`, so callers still see a
  uniform failure with the node name.
- Wrappers are transparent to the engine: the result is an ordinary async
  ``(state, ctx) -> update`` callable.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from agentflow.errors import InterruptError

__all__ = ["with_retry", "with_timeout"]

Node = Callable[[Mapping[str, Any], Any], Awaitable[Mapping[str, Any] | None]]


async def _maybe_await(value: Any) -> Any:
    if asyncio.iscoroutine(value):
        return await value
    return value


def with_timeout(node: Node, seconds: float) -> Node:
    """Wrap ``node`` so it fails if it runs longer than ``seconds``.

    Raises :class:`asyncio.TimeoutError` on expiry (the runtime wraps it as a
    ``NodeError``). An :class:`InterruptError` still propagates: a suspended
    node is not a timeout.
    """

    async def wrapped(state: Mapping[str, Any], ctx: Any) -> Mapping[str, Any] | None:
        # asyncio.wait_for cancels the inner task on timeout; if that inner
        # task was raising InterruptError at the moment of cancellation the
        # InterruptError still surfaces because wait_for re-raises the task's
        # exception if it finished first.
        return await asyncio.wait_for(_maybe_await(node(state, ctx)), timeout=seconds)

    wrapped.__name__ = f"with_timeout({getattr(node, '__name__', 'node')})"
    return wrapped


def with_retry(
    node: Node,
    *,
    retries: int = 2,
    backoff: float = 0.1,
    backoff_factor: float = 2.0,
    max_backoff: float = 30.0,
    on: tuple[type[BaseException], ...] = (Exception,),
    jitter: float = 0.0,
) -> Node:
    """Wrap ``node`` to retry on failure with exponential backoff.

    - ``retries`` is the number of *additional* attempts after the first, so
      the node runs up to ``retries + 1`` times.
    - ``on`` selects which exception types trigger a retry; anything else
      propagates immediately.
    - :class:`InterruptError` is always excluded, even if ``on`` would match
      it, so HITL suspends are never retried.
    - After the last attempt the final exception is re-raised unchanged.

    ``backoff`` is the first delay; each retry multiplies it by
    ``backoff_factor`` up to ``max_backoff``. ``jitter`` adds up to that many
    seconds of uniform random delay (0 disables it, keeping tests deterministic).
    """
    if retries < 0:
        raise ValueError("retries must be >= 0")

    async def wrapped(state: Mapping[str, Any], ctx: Any) -> Mapping[str, Any] | None:
        import random

        attempt = 0
        delay = backoff
        while True:
            try:
                return await _maybe_await(node(state, ctx))
            except InterruptError:
                raise  # never retry a human-in-the-loop suspend
            except on as exc:  # noqa: B902 - configurable exception set
                attempt += 1
                if attempt > retries:
                    raise
                wait = delay + (random.uniform(0, jitter) if jitter else 0.0)
                if wait > 0:
                    await asyncio.sleep(wait)
                delay = min(delay * backoff_factor, max_backoff)
                # loop to retry; keep last exc bound only for clarity
                del exc

    wrapped.__name__ = f"with_retry({getattr(node, '__name__', 'node')})"
    return wrapped
