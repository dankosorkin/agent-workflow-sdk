# Retries and rate limits

Networks fail and providers rate-limit. The HTTP LLM backends handle transient failures with a configurable retry policy, and surface a persistent rate limit as a typed error you can back off on. This chapter covers what is retried, what is not, and how to tune it.

## What gets retried

The HTTP LLM backends retry the connect / initial-response phase only — establishing the connection and receiving the response status. Retryable conditions are connection errors, timeouts, and HTTP 429/5xx responses.

Crucially, retries never cover a partially received stream. Once tokens start arriving, a mid-stream failure is not retried, because replaying a half-consumed stream would duplicate output. A partial stream is never replayed.

## Tuning with RetryPolicy

Pass a `RetryPolicy` when constructing an HTTP backend.

```python
from agentflow.backends import RetryPolicy
from agentflow.backends.openai import OpenAIBackend

llm = OpenAIBackend(
    "gpt-4o-mini",
    api_key="...",
    retry=RetryPolicy(max_retries=4, backoff=0.5),
)
```

The policy's fields:

```python
RetryPolicy(
    max_retries=2,            # additional attempts after the first (so up to max_retries + 1 total)
    backoff=0.5,              # first delay in seconds
    factor=2.0,               # multiply the delay by this each retry
    max_backoff=30.0,         # cap on the delay
    jitter=0.25,              # up to this many seconds of random extra delay
    respect_retry_after=True, # honor the server's Retry-After header
)
```

The delay before retry N (1-based) is `backoff * factor**(N-1)`, capped at `max_backoff`, plus up to `jitter` seconds of random noise. Set `jitter=0` for deterministic timing in tests. When the server sends a `Retry-After` header and `respect_retry_after` is true, that value is used instead.

To disable retries entirely, use `RetryPolicy(max_retries=0)`.

## When retries run out

If the connect phase keeps failing past the policy, the backend raises a typed error rather than returning a partial result:

- `BackendTransportError` — a fatal transport failure. It carries `status` and `headers` when the failure came from an HTTP response, so you can branch on them.
- `BackendRateLimitError` — a 429 that outlived the retry policy. A subclass of `BackendTransportError`, it carries `retry_after` (seconds) when the provider supplied it, so you can back off at the application level.

```python
from agentflow.errors import BackendRateLimitError, BackendTransportError

try:
    async for ev in llm.chat(messages):
        ...
except BackendRateLimitError as e:
    # provider is rate-limiting us; back off e.retry_after seconds
    ...
except BackendTransportError as e:
    # e.status, e.headers — decide whether to fail the node or route around it
    ...
```

## Retries at the node level

The backend's retry policy covers transport. For higher-level resilience — retrying a whole node that failed for any reason, or timing a node out — AgentFlow provides node wrappers `with_retry` and `with_timeout`. They compose with backend retries: the backend handles a flaky connection, the node wrapper handles a node that failed as a unit. Both are covered in the [prebuilt patterns](../patterns/prebuilt.md) chapter, and both correctly let a human-in-the-loop interrupt pass through untouched.

## Rules of thumb

- Leave the default policy for interactive use; it recovers from brief blips without a noticeable stall.
- Raise `max_retries` and `max_backoff` for batch jobs that can afford to wait out a rate limit.
- Catch `BackendRateLimitError` at the application level and honor `retry_after` when you are driving many runs against one provider.
- Do not expect a mid-stream failure to be recovered — design nodes so a failed turn can be retried as a whole if that matters.

That completes the backends section. Next: [checkpointing](../durability/checkpointing.md).
