"""HTTP retry/backoff for the httpx LLM backends (via the shared helper)."""

from __future__ import annotations

import json

import httpx
import pytest
from agentflow.backends._http import RetryPolicy, open_stream
from agentflow.backends.ollama import OllamaBackend
from agentflow.errors import BackendTransportError
from agentflow.events import Message, TurnEnd


def _ndjson_ok() -> bytes:
    return (
        json.dumps(
            {"message": {"role": "assistant", "content": "hi"}, "done": True, "done_reason": "stop"}
        )
        + "\n"
    ).encode()


class _Sequence:
    """A MockTransport handler returning scripted responses in order."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        r = self._responses[min(self.calls - 1, len(self._responses) - 1)]
        return r() if callable(r) else r


async def _no_sleep(_seconds):  # deterministic: never actually wait
    return None


def _backend(handler, policy):
    b = OllamaBackend("m", retry=policy)
    b._client = httpx.AsyncClient(base_url=b.host, transport=httpx.MockTransport(handler))
    return b


async def _drain(backend):
    return [e async for e in backend.chat([Message(role="user", content="hi")])]


# --- via a real backend (Ollama) ---


async def test_retries_then_succeeds(monkeypatch):
    import agentflow.backends._http as h

    monkeypatch.setattr(h.asyncio, "sleep", _no_sleep)

    seq = _Sequence(
        [
            httpx.Response(503, content=b"busy"),
            httpx.Response(503, content=b"busy"),
            httpx.Response(200, content=_ndjson_ok()),
        ]
    )
    b = _backend(seq, RetryPolicy(max_retries=3, backoff=0.01, jitter=0))
    events = await _drain(b)
    await b.close()
    assert seq.calls == 3
    assert isinstance(events[-1], TurnEnd)


async def test_gives_up_after_max_retries(monkeypatch):
    import agentflow.backends._http as h

    monkeypatch.setattr(h.asyncio, "sleep", _no_sleep)

    seq = _Sequence([httpx.Response(503, content=b"busy")])
    b = _backend(seq, RetryPolicy(max_retries=2, backoff=0.01, jitter=0))
    with pytest.raises(BackendTransportError) as ei:
        await _drain(b)
    await b.close()
    assert "503" in str(ei.value)
    assert seq.calls == 3  # 1 initial + 2 retries


async def test_does_not_retry_client_error(monkeypatch):
    import agentflow.backends._http as h

    monkeypatch.setattr(h.asyncio, "sleep", _no_sleep)

    seq = _Sequence([httpx.Response(401, content=b"bad key")])
    b = _backend(seq, RetryPolicy(max_retries=5, backoff=0.01, jitter=0))
    with pytest.raises(BackendTransportError):
        await _drain(b)
    await b.close()
    assert seq.calls == 1  # 401 is non-retryable → no retries


async def test_retries_connection_error(monkeypatch):
    import agentflow.backends._http as h

    monkeypatch.setattr(h.asyncio, "sleep", _no_sleep)

    state = {"calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        state["calls"] += 1
        if state["calls"] < 2:
            raise httpx.ConnectError("refused")
        return httpx.Response(200, content=_ndjson_ok())

    b = _backend(handler, RetryPolicy(max_retries=3, backoff=0.01, jitter=0))
    events = await _drain(b)
    await b.close()
    assert state["calls"] == 2
    assert isinstance(events[-1], TurnEnd)


# --- the shared helper directly, incl. Retry-After ---


async def test_open_stream_respects_retry_after(monkeypatch):
    import agentflow.backends._http as h

    slept: list[float] = []

    async def record_sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr(h.asyncio, "sleep", record_sleep)

    seq = _Sequence(
        [
            httpx.Response(429, headers={"retry-after": "7"}, content=b"slow down"),
            httpx.Response(200, content=b"ok"),
        ]
    )
    client = httpx.AsyncClient(base_url="http://x", transport=httpx.MockTransport(seq))
    policy = RetryPolicy(max_retries=2, backoff=99, jitter=0)  # backoff would be huge
    async with open_stream(client, "POST", "/y", json={}, policy=policy, label="t") as resp:
        assert resp.status_code == 200
    await client.aclose()
    # It slept for the Retry-After value (7s), not the policy backoff (99s).
    assert slept == [7.0]


async def test_open_stream_no_policy_no_retry():
    seq = _Sequence([httpx.Response(503, content=b"busy")])
    client = httpx.AsyncClient(base_url="http://x", transport=httpx.MockTransport(seq))
    with pytest.raises(BackendTransportError):
        async with open_stream(client, "POST", "/y", json={}, policy=None, label="t"):
            pass
    await client.aclose()
    assert seq.calls == 1  # policy=None → max_retries=0
