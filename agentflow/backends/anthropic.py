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

"""Anthropic LLM backend — streaming Messages API (``/v1/messages``).

An :class:`~agentflow.backends.base.LLMBackend` speaking Anthropic's Messages
wire format over Server-Sent Events. Distinct from the OpenAI shape:

- Auth header is ``x-api-key`` plus ``anthropic-version``.
- ``max_tokens`` is required in the request body.
- The system prompt is a top-level ``system`` field, not a message role.
- Events are typed: ``message_start``, ``content_block_start`` (text or
  tool_use), ``content_block_delta`` (``text_delta`` or ``input_json_delta``
  streaming a tool's JSON args), ``content_block_stop``, ``message_delta``
  (carries the final ``stop_reason``), ``message_stop``.

Requires the ``ollama`` extra (which provides ``httpx`` — the dependency is
shared): ``pip install 'agent-workflow-sdk[ollama]'``.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

from agentflow.backends._http import RetryPolicy, open_stream
from agentflow.backends.base import BaseLLMBackend
from agentflow.errors import BackendError, BackendTransportError
from agentflow.events import (
    BackendEvent,
    BackendRequest,
    ChatRequest,
    ErrorEvent,
    Message,
    TextChunk,
    ToolCall,
    ToolCallSpec,
    TurnEnd,
)

try:
    import httpx
except ModuleNotFoundError as exc:  # pragma: no cover - import-time guard
    raise ModuleNotFoundError(
        "AnthropicBackend requires httpx. Install the extra: "
        "pip install 'agent-workflow-sdk[ollama]'"
    ) from exc

__all__ = ["AnthropicBackend"]

_DEFAULT_VERSION = "2023-06-01"


class AnthropicBackend(BaseLLMBackend):
    """Talk to the Anthropic Messages API."""

    def __init__(
        self,
        model: str,
        *,
        api_key: str | None = None,
        base_url: str = "https://api.anthropic.com",
        max_tokens: int = 1024,
        anthropic_version: str = _DEFAULT_VERSION,
        workspace_id: str | None = None,
        options: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 120.0,
        retry: RetryPolicy | None = None,
        max_concurrency: int | None = None,
    ) -> None:
        self.model = model
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.max_tokens = max_tokens
        self.anthropic_version = anthropic_version
        # An org-level (unscoped) key needs the workspace id header; a
        # workspace-scoped key does not.
        self.workspace_id = workspace_id
        self.options = dict(options or {})
        self.extra_headers = dict(headers or {})
        self.timeout = timeout
        self.retry = retry if retry is not None else RetryPolicy()
        self.max_concurrency = max_concurrency
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        if self._client is None:
            headers = {
                "anthropic-version": self.anthropic_version,
                "content-type": "application/json",
                **self.extra_headers,
            }
            if self.api_key:
                headers.setdefault("x-api-key", self.api_key)
            if self.workspace_id:
                headers.setdefault("anthropic-workspace-id", self.workspace_id)
            self._client = httpx.AsyncClient(
                base_url=self.base_url, timeout=self.timeout, headers=headers
            )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def invoke(
        self, request: BackendRequest, *, session: str | None = None
    ) -> AsyncIterator[BackendEvent]:
        if not isinstance(request, ChatRequest):
            raise BackendError(
                f"AnthropicBackend accepts ChatRequest, got {type(request).__name__}"
            )
        if self._client is None:
            raise BackendError("AnthropicBackend.start() was not called")

        body = self._build_body(request)
        text_parts: list[str] = []
        # Content blocks assembled by index. Text blocks accumulate text;
        # tool_use blocks accumulate a partial_json string to parse at stop.
        blocks: dict[int, dict[str, Any]] = {}
        tool_calls: list[ToolCallSpec] = []
        stop_reason: str | None = None

        async with (
            self.concurrency_guard(),
            open_stream(
                self._client,
                "POST",
                "/v1/messages",
                json=body,
                policy=self.retry,
                label="/v1/messages",
            ) as resp,
        ):
            try:
                async for line in resp.aiter_lines():
                    line = line.strip()
                    if not line or not line.startswith("data:"):
                        continue
                    data = line[len("data:") :].strip()
                    try:
                        event = json.loads(data)
                    except json.JSONDecodeError:
                        yield ErrorEvent(message=f"malformed SSE data: {data!r}")
                        continue
                    for out in self._parse_event(event, text_parts, blocks, tool_calls):
                        yield out
                    sr = _stop_reason(event)
                    if sr:
                        stop_reason = sr
            except httpx.HTTPError as exc:
                raise BackendTransportError(f"/v1/messages stream failed: {exc}") from exc

        final_text = "".join(text_parts)
        message = Message(role="assistant", content=final_text, tool_calls=tuple(tool_calls))
        yield TurnEnd(text=final_text, stop_reason=stop_reason or "end_turn", message=message)

    # ------------------------------------------------------------------

    def _build_body(self, request: ChatRequest) -> dict[str, Any]:
        system, messages = _split_system(request.messages)
        body: dict[str, Any] = {
            "model": self.model,
            "max_tokens": self.max_tokens,
            "messages": messages,
            "stream": True,
        }
        if system:
            body["system"] = system
        body.update(self.options)
        body.update(request.options)
        if request.tools:
            body["tools"] = [_tool_to_anthropic(t) for t in request.tools]
        return body

    def _parse_event(
        self,
        event: dict[str, Any],
        text_parts: list[str],
        blocks: dict[int, dict[str, Any]],
        tool_calls: list[ToolCallSpec],
    ) -> list[BackendEvent]:
        etype = event.get("type")

        if etype == "content_block_start":
            index = event.get("index", 0)
            block = event.get("content_block") or {}
            blocks[index] = {
                "type": block.get("type"),
                "id": block.get("id"),
                "name": block.get("name"),
                "json": "",
            }
            return []

        if etype == "content_block_delta":
            delta = event.get("delta") or {}
            dtype = delta.get("type")
            if dtype == "text_delta":
                text = delta.get("text") or ""
                if text:
                    text_parts.append(text)
                    return [TextChunk(text=text)]
            elif dtype == "input_json_delta":
                index = event.get("index", 0)
                blk = blocks.get(index)
                if blk is not None:
                    blk["json"] += delta.get("partial_json") or ""
            return []

        if etype == "content_block_stop":
            index = event.get("index", 0)
            blk = blocks.get(index)
            if blk and blk.get("type") == "tool_use":
                raw = blk.get("json") or ""
                try:
                    args = json.loads(raw) if raw else {}
                except json.JSONDecodeError:
                    args = {"_raw": raw}
                call_id = blk.get("id") or f"call_{index}"
                name = blk.get("name") or ""
                spec_args = args if isinstance(args, dict) else {"_value": args}
                tool_calls.append(ToolCallSpec(id=call_id, name=name, args=spec_args))
                event_args = args if isinstance(args, dict) else {}
                return [ToolCall(id=call_id, name=name, args=event_args)]
            return []

        return []


def _stop_reason(event: dict[str, Any]) -> str | None:
    if event.get("type") == "message_delta":
        return (event.get("delta") or {}).get("stop_reason")
    return None


def _split_system(messages) -> tuple[str, list[dict[str, Any]]]:
    """Anthropic takes the system prompt as a top-level field, not a role."""
    system_parts: list[str] = []
    out: list[dict[str, Any]] = []
    for m in messages:
        if m.role == "system":
            if m.content:
                system_parts.append(m.content)
        else:
            out.append(_message_to_anthropic(m))
    return "\n".join(system_parts), out


def _message_to_anthropic(m: Message) -> dict[str, Any]:
    # Tool results are sent as a user message with a tool_result content block.
    if m.role == "tool":
        return {
            "role": "user",
            "content": [
                {
                    "type": "tool_result",
                    "tool_use_id": m.tool_call_id or "",
                    "content": m.content,
                }
            ],
        }
    if m.tool_calls:
        content: list[dict[str, Any]] = []
        if m.content:
            content.append({"type": "text", "text": m.content})
        for tc in m.tool_calls:
            content.append({"type": "tool_use", "id": tc.id, "name": tc.name, "input": tc.args})
        return {"role": m.role, "content": content}
    return {"role": m.role, "content": m.content}


def _tool_to_anthropic(t) -> dict[str, Any]:
    return {
        "name": t.name,
        "description": t.description,
        "input_schema": t.schema or {"type": "object", "properties": {}},
    }
