# Copyright 2026 Daniel Sorkin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""AnthropicBackend tests against a mocked Messages-API SSE transport."""

from __future__ import annotations

import json

import httpx
import pytest
from agentflow.backends.anthropic import AnthropicBackend
from agentflow.errors import BackendError, BackendTransportError
from agentflow.events import Message, ToolCall, ToolSpec, TurnEnd


def _sse(*events) -> bytes:
    lines = []
    for e in events:
        lines.append(f"event: {e['type']}")
        lines.append(f"data: {json.dumps(e)}")
        lines.append("")
    return ("\n".join(lines) + "\n").encode("utf-8")


def _make(handler) -> AnthropicBackend:
    from agentflow.backends._http import RetryPolicy

    b = AnthropicBackend(
        "claude-sonnet-4", api_key="test", max_tokens=256, retry=RetryPolicy(max_retries=0)
    )
    b._client = httpx.AsyncClient(
        base_url=b.base_url,
        transport=httpx.MockTransport(handler),
        headers={"x-api-key": "test", "anthropic-version": "2023-06-01"},
    )
    return b


async def test_streams_text_and_turn_end():
    def handler(request: httpx.Request) -> httpx.Response:
        body = _sse(
            {"type": "message_start", "message": {"id": "m1"}},
            {
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""},
            },
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": "Hel"},
            },
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": "lo"},
            },
            {"type": "content_block_stop", "index": 0},
            {"type": "message_delta", "delta": {"stop_reason": "end_turn"}},
            {"type": "message_stop"},
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
    assert end.stop_reason == "end_turn"


async def test_assembles_streamed_tool_use():
    def handler(request: httpx.Request) -> httpx.Response:
        body = _sse(
            {"type": "message_start", "message": {"id": "m1"}},
            {
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "tool_use", "id": "tu_1", "name": "get_weather"},
            },
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "input_json_delta", "partial_json": '{"ci'},
            },
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "input_json_delta", "partial_json": 'ty": "Paris"}'},
            },
            {"type": "content_block_stop", "index": 0},
            {"type": "message_delta", "delta": {"stop_reason": "tool_use"}},
            {"type": "message_stop"},
        )
        return httpx.Response(200, content=body, headers={"content-type": "text/event-stream"})

    b = _make(handler)
    events = [e async for e in b.chat([Message(role="user", content="weather?")])]
    await b.close()

    tool_calls = [e for e in events if isinstance(e, ToolCall)]
    assert len(tool_calls) == 1
    assert tool_calls[0].id == "tu_1"
    assert tool_calls[0].name == "get_weather"
    assert tool_calls[0].args == {"city": "Paris"}
    end = events[-1]
    assert isinstance(end, TurnEnd) and end.stop_reason == "tool_use"
    assert len(end.message.tool_calls) == 1


async def test_system_prompt_extracted_to_top_level():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        body = _sse(
            {"type": "content_block_start", "index": 0, "content_block": {"type": "text"}},
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": "ok"},
            },
            {"type": "content_block_stop", "index": 0},
            {"type": "message_delta", "delta": {"stop_reason": "end_turn"}},
        )
        return httpx.Response(200, content=body, headers={"content-type": "text/event-stream"})

    b = _make(handler)
    _ = [
        e
        async for e in b.chat(
            [
                Message(role="system", content="You are terse."),
                Message(role="user", content="hi"),
            ]
        )
    ]
    await b.close()

    body = captured["body"]
    assert body["system"] == "You are terse."
    # system message removed from the messages array
    assert all(m["role"] != "system" for m in body["messages"])
    assert body["max_tokens"] == 256


async def test_tools_serialized_with_input_schema():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        body = _sse({"type": "message_delta", "delta": {"stop_reason": "end_turn"}})
        return httpx.Response(200, content=body, headers={"content-type": "text/event-stream"})

    b = _make(handler)
    schema = {"type": "object", "properties": {"x": {"type": "number"}}}
    _ = [
        e
        async for e in b.chat(
            [Message(role="user", content="hi")],
            tools=[ToolSpec(name="calc", description="d", schema=schema)],
        )
    ]
    await b.close()
    tool = captured["body"]["tools"][0]
    assert tool["name"] == "calc"
    assert tool["input_schema"] == schema


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


async def test_start_sets_auth_headers():
    b = AnthropicBackend("m", api_key="secret")
    await b.start()
    assert b._client.headers.get("x-api-key") == "secret"
    assert b._client.headers.get("anthropic-version") == "2023-06-01"
    await b.close()
