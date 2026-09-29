"""Ollama LLM backend — async HTTP over the local ``/api/chat`` endpoint.

An :class:`~agentflow.backends.base.LLMBackend`: stateless per call, messages
in, token stream out. It never runs tools itself — a tool call the model
emits is a request the graph fulfils and feeds back on the next call.

Requires the ``ollama`` extra (``pip install agentic-workflow-sdk[ollama]``),
which pulls in ``httpx``. Importing this module without httpx raises a clear
error rather than failing obscurely.

This is also the template for any OpenAI-compatible endpoint: swap the URL,
the request body, and the stream parser; the event mapping is identical.
"""

from __future__ import annotations

import json
from typing import Any, AsyncIterator

from agentflow.backends.base import BaseLLMBackend
from agentflow.backends._http import RetryPolicy, open_stream
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
        "OllamaBackend requires httpx. Install the extra: "
        "pip install 'agentic-workflow-sdk[ollama]'"
    ) from exc

__all__ = ["OllamaBackend"]


class OllamaBackend(BaseLLMBackend):
    """Talk to a local (or remote) Ollama server."""

    def __init__(
        self,
        model: str,
        *,
        host: str = "http://localhost:11434",
        options: dict[str, Any] | None = None,
        timeout: float = 120.0,
        retry: RetryPolicy | None = None,
        max_concurrency: int | None = None,
    ) -> None:
        self.model = model
        self.host = host.rstrip("/")
        self.options = dict(options or {})
        self.timeout = timeout
        self.retry = retry if retry is not None else RetryPolicy()
        self.max_concurrency = max_concurrency
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(base_url=self.host, timeout=self.timeout)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def invoke(
        self, request: BackendRequest, *, session: str | None = None
    ) -> AsyncIterator[BackendEvent]:
        if not isinstance(request, ChatRequest):
            raise BackendError(
                f"OllamaBackend accepts ChatRequest, got {type(request).__name__}"
            )
        if self._client is None:
            raise BackendError("OllamaBackend.start() was not called")

        body = self._build_body(request)
        text_parts: list[str] = []
        tool_calls: list[ToolCallSpec] = []
        stop_reason: str | None = None

        async with self.concurrency_guard(), open_stream(
            self._client, "POST", "/api/chat",
            json=body, policy=self.retry, label="ollama /api/chat",
        ) as resp:
            try:
                async for line in resp.aiter_lines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        chunk = json.loads(line)
                    except json.JSONDecodeError:
                        yield ErrorEvent(message=f"malformed stream line: {line!r}")
                        continue

                    for event in self._parse_chunk(chunk, text_parts, tool_calls):
                        yield event

                    if chunk.get("done"):
                        stop_reason = chunk.get("done_reason") or "stop"
            except httpx.HTTPError as exc:
                raise BackendTransportError(f"ollama stream failed: {exc}") from exc

        final_text = "".join(text_parts)
        message = Message(
            role="assistant",
            content=final_text,
            tool_calls=tuple(tool_calls),
        )
        yield TurnEnd(text=final_text, stop_reason=stop_reason, message=message)

    # ------------------------------------------------------------------

    def _build_body(self, request: ChatRequest) -> dict[str, Any]:
        body: dict[str, Any] = {
            "model": self.model,
            "messages": [_message_to_ollama(m) for m in request.messages],
            "stream": True,
        }
        merged = {**self.options, **request.options}
        if merged:
            body["options"] = merged
        if request.tools:
            body["tools"] = [_tool_to_ollama(t) for t in request.tools]
        return body

    def _parse_chunk(
        self,
        chunk: dict[str, Any],
        text_parts: list[str],
        tool_calls: list[ToolCallSpec],
    ) -> list[Any]:
        events: list[Any] = []
        message = chunk.get("message") or {}

        content = message.get("content")
        if content:
            text_parts.append(content)
            events.append(TextChunk(text=content))

        for i, call in enumerate(message.get("tool_calls") or []):
            fn = call.get("function") or {}
            args = fn.get("arguments")
            if isinstance(args, str):
                try:
                    args = json.loads(args)
                except json.JSONDecodeError:
                    args = {"_raw": args}
            call_id = call.get("id") or f"call_{len(tool_calls) + i}"
            name = fn.get("name", "")
            spec = ToolCallSpec(id=call_id, name=name, args=args or {})
            tool_calls.append(spec)
            events.append(ToolCall(id=call_id, name=name, args=args or {}))

        return events


def _message_to_ollama(m: Message) -> dict[str, Any]:
    out: dict[str, Any] = {"role": m.role, "content": m.content}
    if m.tool_calls:
        out["tool_calls"] = [
            {"function": {"name": tc.name, "arguments": tc.args}} for tc in m.tool_calls
        ]
    if m.tool_call_id:
        out["tool_call_id"] = m.tool_call_id
    if m.name:
        out["name"] = m.name
    return out


def _tool_to_ollama(t) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": t.name,
            "description": t.description,
            "parameters": t.schema or {"type": "object", "properties": {}},
        },
    }
