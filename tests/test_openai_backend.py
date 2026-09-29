# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""OpenAIBackend tests against a mocked httpx SSE transport (no network).

Covers text streaming, TurnEnd/finish_reason, tool-call assembly from streamed
indexed fragments, error mapping, and request-type validation.
"""

from __future__ import annotations

import httpx
import pytest
from agentflow.backends.openai import OpenAIBackend
from agentflow.errors import BackendError, BackendTransportError
from agentflow.events import Message, TurnEnd


def _sse(*chunks) -> bytes:
    import json

    lines = []
    for c in chunks:
        lines.append(f"data: {json.dumps(c)}")
        lines.append("")  # blank line between SSE events
    lines.append("data: [DONE]")
    lines.append("")
    return ("\n".join(lines)).encode("utf-8")


def _make(handler) -> OpenAIBackend:
    from agentflow.backends._http import RetryPolicy

    b = OpenAIBackend("gpt-4o-mini", api_key="test", retry=RetryPolicy(max_retries=0))
    b._client = httpx.AsyncClient(
        base_url=b.base_url,
        transport=httpx.MockTransport(handler),
        headers={"Authorization": "Bearer test"},
    )
    return b


async def test_streams_text_and_finish():
    def handler(request: httpx.Request) -> httpx.Response:
        body = _sse(
            {"choices": [{"delta": {"content": "Hel"}, "finish_reason": None}]},
            {"choices": [{"delta": {"content": "lo"}, "finish_reason": None}]},
            {"choices": [{"delta": {}, "finish_reason": "stop"}]},
        )
        return httpx.Response(200, content=body, headers={"content-type": "text/event-stream"})

    b = _make(handler)
    events = [e async for e in b.chat([Message(role="user", content="hi")])]
    await b.close()

    kinds = [type(e).__name__ for e in events]
    assert kinds == ["TextChunk", "TextChunk", "TurnEnd"]
    end = events[-1]
    assert isinstance(end, TurnEnd)
    assert end.text == "Hello"
    assert end.stop_reason == "stop"


async def test_assembles_streamed_tool_call():
    """A tool call streams as id/name in one chunk, then argument fragments."""

    def handler(request: httpx.Request) -> httpx.Response:
        body = _sse(
            {
                "choices": [
                    {
                        "delta": {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "call_1",
                                    "function": {"name": "get_weather", "arguments": ""},
                                }
                            ]
                        },
                        "finish_reason": None,
                    }
                ]
            },
            {
                "choices": [
                    {
                        "delta": {"tool_calls": [{"index": 0, "function": {"arguments": '{"ci'}}]},
                        "finish_reason": None,
                    }
                ]
            },
            {
                "choices": [
                    {
                        "delta": {
                            "tool_calls": [{"index": 0, "function": {"arguments": 'ty": "Paris"}'}}]
                        },
                        "finish_reason": None,
                    }
                ]
            },
            {"choices": [{"delta": {}, "finish_reason": "tool_calls"}]},
        )
        return httpx.Response(200, content=body, headers={"content-type": "text/event-stream"})

    b = _make(handler)
    events = [e async for e in b.chat([Message(role="user", content="weather?")])]
    await b.close()

    end = events[-1]
    assert isinstance(end, TurnEnd)
    assert end.stop_reason == "tool_calls"
    assert len(end.message.tool_calls) == 1
    tc = end.message.tool_calls[0]
    assert tc.id == "call_1"
    assert tc.name == "get_weather"
    assert tc.args == {"city": "Paris"}  # assembled + parsed from fragments


async def test_two_parallel_tool_calls():
    def handler(request: httpx.Request) -> httpx.Response:
        body = _sse(
            {
                "choices": [
                    {
                        "delta": {
                            "tool_calls": [
                                {
                                    "index": 0,
                                    "id": "a",
                                    "function": {"name": "f", "arguments": "{}"},
                                },
                                {
                                    "index": 1,
                                    "id": "b",
                                    "function": {"name": "g", "arguments": "{}"},
                                },
                            ]
                        },
                        "finish_reason": None,
                    }
                ]
            },
            {"choices": [{"delta": {}, "finish_reason": "tool_calls"}]},
        )
        return httpx.Response(200, content=body, headers={"content-type": "text/event-stream"})

    b = _make(handler)
    events = [e async for e in b.chat([Message(role="user", content="go")])]
    await b.close()
    calls = events[-1].message.tool_calls
    assert [c.id for c in calls] == ["a", "b"]
    assert [c.name for c in calls] == ["f", "g"]


async def test_http_error_maps_to_transport_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, content=b'{"error":"bad key"}')

    b = _make(handler)
    with pytest.raises(BackendTransportError):
        _ = [e async for e in b.chat([Message(role="user", content="hi")])]
    await b.close()


async def test_rejects_wrong_request_type():
    from agentflow.events import TextRequest

    b = _make(lambda r: httpx.Response(200, content=b""))
    with pytest.raises(BackendError):
        _ = [e async for e in b.invoke(TextRequest("nope"))]
    await b.close()


async def test_start_builds_auth_header():
    b = OpenAIBackend("m", api_key="secret", base_url="https://x/v1")
    await b.start()
    assert b._client.headers.get("Authorization") == "Bearer secret"
    await b.close()
