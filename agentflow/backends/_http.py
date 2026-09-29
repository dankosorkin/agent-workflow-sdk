"""Shared HTTP retry/backoff for the httpx-based LLM backends.

Retries are applied only to the *connection and initial response* phase — up
to and including the status check — never mid-stream. Once the server has
started streaming tokens we cannot safely replay the request, so a failure
after the first chunk propagates. This keeps retries correct for a streaming,
non-idempotent-by-default API.

What is retried:
- transport errors (connection refused/reset, DNS, read timeout on connect)
- retryable status codes: 408, 409, 425, 429, 500, 502, 503, 504

What is NOT retried:
- 400/401/403/404 and other non-retryable 4xx — surfaced immediately
- any failure after streaming has begun

``Retry-After`` (seconds or HTTP-date) is honored when present on a retryable
response; otherwise exponential backoff with jitter is used.
"""

from __future__ import annotations

import asyncio
import email.utils
import random
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator

import httpx

from agentflow.errors import BackendTransportError

__all__ = ["RetryPolicy", "open_stream"]

_RETRYABLE_STATUS = frozenset({408, 409, 425, 429, 500, 502, 503, 504})


@dataclass(frozen=True)
class RetryPolicy:
    """Retry configuration for the connect/initial-response phase.

    ``max_retries`` is the number of *additional* attempts after the first, so
    a request runs up to ``max_retries + 1`` times. ``backoff`` is the first
    delay; each retry multiplies by ``factor`` up to ``max_backoff``.
    ``jitter`` adds up to that many seconds of uniform random delay (keep 0 for
    deterministic tests). ``respect_retry_after`` uses the server's header when
    present.
    """

    max_retries: int = 2
    backoff: float = 0.5
    factor: float = 2.0
    max_backoff: float = 30.0
    jitter: float = 0.25
    respect_retry_after: bool = True
    retryable_status: frozenset[int] = _RETRYABLE_STATUS

    def delay_for(self, attempt: int) -> float:
        """Backoff delay before retry ``attempt`` (1-based)."""
        raw = self.backoff * (self.factor ** (attempt - 1))
        capped = min(raw, self.max_backoff)
        return capped + (random.uniform(0, self.jitter) if self.jitter else 0.0)


def _parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    # HTTP-date form
    try:
        dt = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.0, (dt - datetime.now(timezone.utc)).total_seconds())


@asynccontextmanager
async def open_stream(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    json: Any,
    policy: RetryPolicy | None,
    label: str,
    sleep=None,
) -> AsyncIterator[httpx.Response]:
    """Open a streaming request, retrying the connect/status phase per policy.

    Yields a live :class:`httpx.Response` positioned before the body, with a
    2xx status already confirmed. Raises :class:`BackendTransportError` on a
    non-retryable status, on exhausted retries, or on a transport error that
    outlives the policy.
    """
    pol = policy or RetryPolicy(max_retries=0)
    # Resolve the sleeper at call time so tests can monkeypatch asyncio.sleep.
    if sleep is None:
        sleep = asyncio.sleep
    attempt = 0
    last_detail = ""

    while True:
        attempt += 1
        stream_cm = client.stream(method, url, json=json)
        try:
            resp = await stream_cm.__aenter__()
        except httpx.HTTPError as exc:
            # Connection-level failure: retry if attempts remain.
            if attempt <= pol.max_retries:
                await sleep(pol.delay_for(attempt))
                continue
            raise BackendTransportError(f"{label} request failed: {exc}") from exc

        status = resp.status_code
        if status == 200:
            try:
                yield resp
            finally:
                await stream_cm.__aexit__(None, None, None)
            return

        # Non-200: read the (small) error body, then decide.
        detail = (await resp.aread()).decode("utf-8", "replace")
        retry_after = resp.headers.get("retry-after")
        await stream_cm.__aexit__(None, None, None)
        last_detail = detail

        retryable = status in pol.retryable_status
        if retryable and attempt <= pol.max_retries:
            wait = None
            if pol.respect_retry_after:
                wait = _parse_retry_after(retry_after)
            await sleep(wait if wait is not None else pol.delay_for(attempt))
            continue

        raise BackendTransportError(f"{label} returned {status}: {detail}")
