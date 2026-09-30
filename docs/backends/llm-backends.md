# LLM backends

An LLM backend is a stateless chat model: you hand it messages, it streams back tokens and — if you offered tools — tool-call requests. It never runs a tool itself. A `ToolCall` it emits is a request your graph fulfils and feeds back on the next call. AgentFlow ships three: `OllamaBackend`, `OpenAIBackend`, and `AnthropicBackend`. All three need the `ollama` extra (which provides `httpx`).

## The shape

Call an LLM backend with a `ChatRequest` (via the `chat` convenience wrapper) and a list of `Message`s. It yields the shared event stream ending in a `TurnEnd`.

```python
from agentflow.backends.ollama import OllamaBackend
from agentflow.events import Message, TextChunk, TurnEnd

llm = OllamaBackend("llama3.2")
await llm.start()

messages = [Message("user", "Explain reducers in one sentence.")]
async for ev in llm.chat(messages):
    if isinstance(ev, TextChunk):
        print(ev.text, end="")
    elif isinstance(ev, TurnEnd):
        final = ev
await llm.close()
```

Because they are stateless, LLM backends carry no session and need no permission policy. You pass the full message list each call; the backend does not remember prior turns.

## The three backends

### OllamaBackend — local models

Talks to a local (or remote) Ollama server over its `/api/chat` endpoint.

```python
OllamaBackend(model, ...)   # e.g. OllamaBackend("llama3.2")
```

Good for offline development, private data, and zero per-token cost. Requires an Ollama server running.

### OpenAIBackend — any OpenAI-compatible endpoint

Speaks the OpenAI Chat Completions wire format over Server-Sent Events. It works against OpenAI itself and the many compatible endpoints: Groq, Together, Fireworks, vLLM, LM Studio, and Ollama's own `/v1` shim.

```python
OpenAIBackend(model, api_key="...", ...)   # point base_url at any compatible endpoint
```

This is the backend to reach for when you want a hosted model or a self-hosted inference server that speaks the OpenAI format.

### AnthropicBackend — the Messages API

Talks to the Anthropic Messages API.

```python
AnthropicBackend(model, api_key="...", ...)   # e.g. AnthropicBackend("claude-sonnet-4", api_key="...")
```

## Tool calling

An LLM backend does not execute tools. You advertise tool schemas in the request; if the model decides to call one, you get a `ToolCall` (or a `TurnEnd` whose `message.tool_calls` lists them). Your graph runs the tool and feeds the result back as a `role="tool"` message on the next call. That loop is common enough that AgentFlow ships it prebuilt — see `tool_loop` in the [prebuilt patterns](../patterns/prebuilt.md) chapter — but you can also drive it by hand:

```python
from agentflow.events import Message, ToolSpec, TurnEnd

tools = [ToolSpec(name="multiply", description="Multiply two numbers",
                  schema={"type": "object",
                          "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
                          "required": ["a", "b"]})]

messages = [Message("user", "What is 23 * 19?")]
async for ev in llm.chat(messages, tools=tools):
    if isinstance(ev, TurnEnd):
        assistant = ev.message      # may carry tool_calls
```

If `assistant.tool_calls` is non-empty, run each tool, append a `Message(role="tool", content=result, tool_call_id=call.id)`, and call `chat` again with the extended message list. Repeat until the model answers without a tool call.

## The Message type

`Message` is the neutral chat unit shared across all LLM backends:

```python
Message(
    role,               # "system" | "user" | "assistant" | "tool"
    content="",         # the text
    tool_calls=(),      # tuple[ToolCallSpec, ...] on an assistant message
    tool_call_id=None,  # links a role="tool" result to the call that produced it
    name=None,          # optional tool/function name
)
```

Building a conversation is just assembling a list of these and passing it each call.

## Bounding concurrency

If many graph nodes hit the same provider at once, set `max_concurrency` on the backend instance to cap in-flight requests. It composes with the graph's `max_node_concurrency`: node-level bounds the step, backend-level bounds the provider.

```python
llm = OpenAIBackend("gpt-4o-mini", api_key="...")
llm.max_concurrency = 4     # at most 4 concurrent requests from this instance
```

## Retries

The HTTP LLM backends retry transient failures on the connect/initial-response phase only — never mid-stream, so a partial token stream is never replayed. That behavior and how to tune it with a `RetryPolicy` is covered in the [retries chapter](retries.md).

Next: [permissions](permissions.md).
