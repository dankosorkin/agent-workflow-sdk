"""Ollama backend tests against a mocked httpx transport (no server).

Uses httpx.MockTransport to serve a streamed NDJSON /api/chat response, so the
stream parser, text aggregation, tool-call extraction, and TurnEnd mapping are
all exercised without a running Ollama.
"""

from __future__ import annotations

import json

import httpx
import pytest

from agentflow.backends.ollama import OllamaBackend
from agentflow.events import ChatRequest, Message, TextChunk, ToolCall, TurnEnd


def _ndjson(*objs) -> bytes:
    return b"".join((json.dumps(o) + "\n").encode("utf-8") for o in objs)


def _make_backend(handler) -> OllamaBackend:
    from agentflow.backends._http import RetryPolicy
    backend = OllamaBackend("llama3.2", retry=RetryPolicy(max_retries=0))
    backend._client = httpx.AsyncClient(
        base_url=backend.host, transport=httpx.MockTransport(handler)
    )
    return backend


async def test_streams_text_and_turn_end():
    def handler(request: httpx.Request) -> httpx.Response:
        body = _ndjson(
            {"message": {"role": "assistant", "content": "Hel"}, "done": False},
            {"message": {"role": "assistant", "content": "lo"}, "done": False},
            {"message": {"role": "assistant", "content": ""}, "done": True,
             "done_reason": "stop"},
        )
        return httpx.Response(200, content=body)

    backend = _make_backend(handler)
    events = [e async for e in backend.chat([Message(role="user", content="hi")])]
    await backend.close()

    kinds = [type(e).__name__ for e in events]
    assert kinds == ["TextChunk", "TextChunk", "TurnEnd"]
    assert events[0] == TextChunk("Hel")
    end = events[-1]
    assert isinstance(end, TurnEnd)
    assert end.text == "Hello"
    assert end.stop_reason == "stop"
    assert end.message.content == "Hello"


async def test_extracts_tool_call():
    def handler(request: httpx.Request) -> httpx.Response:
        body = _ndjson(
            {"message": {"role": "assistant", "content": "",
                         "tool_calls": [{"function": {"name": "get_weather",
                                                       "arguments": {"city": "Paris"}}}]},
             "done": True, "done_reason": "stop"},
        )
        return httpx.Response(200, content=body)

    backend = _make_backend(handler)
    events = [e async for e in backend.chat(
        [Message(role="user", content="weather?")],
        tools=[],
    )]
    await backend.close()

    tool_calls = [e for e in events if isinstance(e, ToolCall)]
    assert len(tool_calls) == 1
    assert tool_calls[0].name == "get_weather"
    assert tool_calls[0].args == {"city": "Paris"}
    end = events[-1]
    assert isinstance(end, TurnEnd)
    assert len(end.message.tool_calls) == 1


async def test_http_error_raises_transport_error():
    from agentflow.errors import BackendTransportError

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, content=b"boom")

    backend = _make_backend(handler)
    with pytest.raises(BackendTransportError):
        _ = [e async for e in backend.chat([Message(role="user", content="hi")])]
    await backend.close()


async def test_rejects_wrong_request_type():
    from agentflow.errors import BackendError
    from agentflow.events import TextRequest

    backend = _make_backend(lambda r: httpx.Response(200, content=b""))
    with pytest.raises(BackendError):
        _ = [e async for e in backend.invoke(TextRequest("nope"))]
    await backend.close()
