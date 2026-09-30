# Backends overview

A backend is how a node talks to a model or an agent. AgentFlow gives you two backend families under one interface, so a node consumes either the same way — a stream of events ending in a `TurnEnd`.

## Two families

- Agent backends drive a session that runs its own tools and asks permission. They hold a lifecycle: `start()`, `prompt(text)`, `close()`. Available: `KiroBackend`, `CodexBackend`, `ClaudeCodeBackend`.
- LLM backends are stateless: messages in, token stream out. They never run tools themselves — a tool call they emit is a request your graph fulfils and feeds back. Available: `OllamaBackend`, `OpenAIBackend`, `AnthropicBackend`.

The difference in one line: an agent decides and acts; an LLM only speaks, and your graph decides what to do about it.

## One event vocabulary

Whichever family, calling a backend yields an async stream of `BackendEvent`s:

| Event | Meaning |
| --- | --- |
| `TextChunk(text)` | A streamed delta of the assistant's message |
| `ToolCall(id, name, args, title)` | A tool was invoked (a request for an LLM; informational for an agent) |
| `ToolResult(id, status, content)` | A tool call resolved (agent backends that run their own tools) |
| `PermissionRequest(id, tool, options, detail)` | An agent asks to authorize a tool |
| `TurnEnd(text, stop_reason, message)` | Terminal event: the assembled text and stop reason |
| `ErrorEvent(message, detail)` | A recoverable error surfaced as data, not raised |

A turn always ends in exactly one `TurnEnd`. A node that only wants the final answer drains the stream and keeps the `TurnEnd`; a streaming node forwards `TextChunk`s with `ctx.emit` as they arrive.

## The common lifecycle

Every backend has the same shape:

```python
await backend.start()
async for event in backend.invoke(request):
    ...
await backend.close()
```

Both families are usable as async context managers, which guarantees `close()` even on error:

```python
async with backend:
    async for event in backend.prompt("..."):   # agent
        ...
```

There are convenience wrappers over `invoke`:

- Agent backends: `prompt(text)` wraps `invoke(TextRequest(text))`.
- LLM backends: `chat(messages, tools=..., options=...)` wraps `invoke(ChatRequest(...))`.

## You construct backends; the core never imports them

A backend is a plain object you build and pass into your nodes. The engine has no knowledge of any backend — that is the dependency rule that keeps `import agentflow` free of subprocesses and HTTP libraries. A common pattern is to construct the backend once, `start()` it, and close over it in your node functions.

```python
from agentflow.backends.ollama import OllamaBackend
from agentflow.events import TextChunk

llm = OllamaBackend("llama3.2")

async def chat_node(state, ctx):
    text = []
    async for ev in llm.chat(state["messages"]):
        if isinstance(ev, TextChunk):
            ctx.emit(ev)
            text.append(ev.text)
    return {"reply": "".join(text)}

# elsewhere: await llm.start() ... run the graph ... await llm.close()
```

## Errors

Transport failures raise typed exceptions, never silent partial results:

- `BackendTransportError` — the process died, the socket dropped, or the wire produced unparseable data. It carries `status` and `headers` when the failure came from HTTP.
- `BackendRateLimitError` — a 429 that outlived the retry policy; carries `retry_after` when the provider supplied it.

A recoverable, in-band problem arrives instead as an `ErrorEvent` in the stream — data your node can branch on rather than an exception.

The next chapters go deep on each family: [agent backends](agent-backends.md) and [LLM backends](llm-backends.md), then [permissions](permissions.md) and [retries](retries.md).
