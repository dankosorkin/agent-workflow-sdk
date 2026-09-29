"""OpenAI-compatible LLM backend — streaming ``/v1/chat/completions``.

An :class:`~agentflow.backends.base.LLMBackend` speaking the OpenAI Chat
Completions wire format over Server-Sent Events. Works against OpenAI itself
and the many compatible endpoints: Together, Groq, Fireworks, vLLM, LM Studio,
and Ollama's own ``/v1`` shim.

Two things differ from the Ollama backend:

- Framing is SSE: each event is a ``data: {json}`` line, and the stream ends
  with ``data: [DONE]``.
- Tool calls stream as indexed fragments (``choices[].delta.tool_calls[]``
  with an ``index``, and ``function.arguments`` arriving as concatenated
  string pieces), so they must be assembled across chunks.

Requires the ``ollama`` extra (which provides ``httpx``); the dependency is
shared. Install: ``pip install 'agentic-workflow-sdk[ollama]'``.
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
        "OpenAIBackend requires httpx. Install the extra: "
        "pip install 'agentic-workflow-sdk[ollama]'"
    ) from exc

__all__ = ["OpenAIBackend"]


class OpenAIBackend(BaseLLMBackend):
    """Talk to any OpenAI-compatible chat-completions endpoint."""

    def __init__(
        self,
        model: str,
        *,
        base_url: str = "https://api.openai.com/v1",
        api_key: str | None = None,
        options: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 120.0,
        retry: RetryPolicy | None = None,
    ) -> None:
        self.model = model
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.options = dict(options or {})
        self.extra_headers = dict(headers or {})
        self.timeout = timeout
        self.retry = retry if retry is not None else RetryPolicy()
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        if self._client is None:
            headers = dict(self.extra_headers)
            if self.api_key:
                headers.setdefault("Authorization", f"Bearer {self.api_key}")
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
                f"OpenAIBackend accepts ChatRequest, got {type(request).__name__}"
            )
        if self._client is None:
            raise BackendError("OpenAIBackend.start() was not called")

        body = self._build_body(request)
        text_parts: list[str] = []
        # Assemble streamed tool-call fragments keyed by their choice index.
        tool_frags: dict[int, dict[str, Any]] = {}
        finish_reason: str | None = None

        async with open_stream(
            self._client, "POST", "/chat/completions",
            json=body, policy=self.retry, label="chat/completions",
        ) as resp:
            try:
                async for line in resp.aiter_lines():
                    line = line.strip()
                    if not line or not line.startswith("data:"):
                        continue
                    data = line[len("data:"):].strip()
                    if data == "[DONE]":
                        break
                    try:
                        chunk = json.loads(data)
                    except json.JSONDecodeError:
                        yield ErrorEvent(message=f"malformed SSE data: {data!r}")
                        continue
                    for event in self._parse_chunk(chunk, text_parts, tool_frags):
                        yield event
                    fr = _finish_reason(chunk)
                    if fr:
                        finish_reason = fr
            except httpx.HTTPError as exc:
                raise BackendTransportError(f"chat/completions stream failed: {exc}") from exc

        tool_calls = _assemble_tool_calls(tool_frags)
        final_text = "".join(text_parts)
        message = Message(role="assistant", content=final_text, tool_calls=tuple(tool_calls))
        yield TurnEnd(text=final_text, stop_reason=finish_reason or "stop", message=message)

    # ------------------------------------------------------------------

    def _build_body(self, request: ChatRequest) -> dict[str, Any]:
        body: dict[str, Any] = {
            "model": self.model,
            "messages": [_message_to_openai(m) for m in request.messages],
            "stream": True,
        }
        body.update(self.options)
        body.update(request.options)
        if request.tools:
            body["tools"] = [_tool_to_openai(t) for t in request.tools]
        return body

    def _parse_chunk(
        self,
        chunk: dict[str, Any],
        text_parts: list[str],
        tool_frags: dict[int, dict[str, Any]],
    ) -> list[BackendEvent]:
        events: list[BackendEvent] = []
        for choice in chunk.get("choices") or []:
            delta = choice.get("delta") or {}

            content = delta.get("content")
            if content:
                text_parts.append(content)
                events.append(TextChunk(text=content))

            for tc in delta.get("tool_calls") or []:
                index = tc.get("index", 0)
                frag = tool_frags.setdefault(index, {"id": None, "name": None, "args": ""})
                if tc.get("id"):
                    frag["id"] = tc["id"]
                fn = tc.get("function") or {}
                if fn.get("name"):
                    frag["name"] = fn["name"]
                if fn.get("arguments"):
                    frag["args"] += fn["arguments"]

        return events


def _finish_reason(chunk: dict[str, Any]) -> str | None:
    for choice in chunk.get("choices") or []:
        if choice.get("finish_reason"):
            return choice["finish_reason"]
    return None


def _assemble_tool_calls(tool_frags: dict[int, dict[str, Any]]) -> list[ToolCallSpec]:
    specs: list[ToolCallSpec] = []
    for index in sorted(tool_frags):
        frag = tool_frags[index]
        raw = frag.get("args") or ""
        try:
            args = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            args = {"_raw": raw}
        specs.append(
            ToolCallSpec(
                id=frag.get("id") or f"call_{index}",
                name=frag.get("name") or "",
                args=args if isinstance(args, dict) else {"_value": args},
            )
        )
    return specs


def _message_to_openai(m: Message) -> dict[str, Any]:
    out: dict[str, Any] = {"role": m.role, "content": m.content}
    if m.tool_calls:
        out["tool_calls"] = [
            {
                "id": tc.id,
                "type": "function",
                "function": {"name": tc.name, "arguments": json.dumps(tc.args)},
            }
            for tc in m.tool_calls
        ]
    if m.tool_call_id:
        out["tool_call_id"] = m.tool_call_id
    if m.name:
        out["name"] = m.name
    return out


def _tool_to_openai(t) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": t.name,
            "description": t.description,
            "parameters": t.schema or {"type": "object", "properties": {}},
        },
    }
