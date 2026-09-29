# agentic-workflow-sdk

An async-first, LangGraph-style SDK for building agent workflows from
composable pieces. You describe a workflow as a graph of nodes over a typed,
reducer-based state, and run it against a pluggable backend — a coding agent
(Kiro, and later Codex or Claude Code) or a plain LLM (Ollama and any
OpenAI-compatible endpoint).

The import root is `agentflow`. The distribution name is
`agentic-workflow-sdk`.

## Why

- Build any workflow from primitives: nodes, edges, conditional routing, and
  a shared state. Not a fixed loop, not a linear chain.
- Swap the backend without touching workflow code. Agents and LLMs share one
  event vocabulary.
- Async everywhere: the engine, backends, checkpointing, and streaming.
- Durable by default: every super-step is checkpointed, runs resume after a
  crash, and human-in-the-loop interrupts suspend and resume a run.

The full design rationale is in `DESIGN.md`.

## Install

The core is dependency-free. Backends that need extra libraries are optional
extras.

```bash
pip install -e .            # core only
pip install -e '.[ollama]'  # + httpx, for the Ollama LLM backend
pip install -e '.[dev]'     # + pytest, pytest-asyncio
```

Python 3.11+ is required. The Kiro backend needs `kiro-cli` on PATH.

## Quickstart

A graph that loops until a counter reaches a target, then stops.

```python
import asyncio
from typing import Annotated
from agentflow import Graph, START, END, State, add, append

class CountState(State):
    n: Annotated[int, add]        # updates are summed
    log: Annotated[list, append]  # updates are concatenated

async def tick(state, ctx):
    return {"n": 1, "log": f"tick {state.get('n', 0) + 1}"}

def route(state):
    return "again" if state["n"] < 5 else "done"

g = Graph(CountState)
g.add_node("tick", tick)
g.add_edge(START, "tick")
g.add_conditional_edges("tick", route, {"again": "tick", "done": END})
app = g.compile()

print(asyncio.run(app.invoke({"n": 0, "log": []})))
```

See `examples/hello_graph.py` and `examples/optimize_loop.py` for runnable
versions.

## Core concepts

### State and reducers

State is a `TypedDict` subclass of `State`. Each field is a channel; annotate
it with a reducer that folds each node's update into the current value. A
field without a reducer uses `last` (last-value-wins). Built-in reducers:
`last`, `append`, `add`, `merge`, `union`. A node returns a partial update; it
never mutates the state it was given.

### Nodes and edges

A node is an async callable `(state, ctx) -> update`. Edges are either static
(`add_edge`) or conditional (`add_conditional_edges` with a `router(state) ->
key`). `START` and `END` are sentinels. A conditional router may return a list
of keys to fan out to several nodes at once.

### Execution

A run proceeds in super-steps. All nodes on the current frontier run
concurrently against the same immutable state, their updates are folded
through the reducers in a deterministic order, and the next frontier is
computed from the edges. A `step_limit` guards against runaway loops.

`CompiledGraph` gives you:

- `await app.invoke(input, thread=...)` — run to completion, return state.
- `app.stream(input, thread=...)` — async-iterate `StreamEvent`s as they occur.
- `await app.resume(thread, value=...)` — continue a suspended run.
- `await app.get_state(thread)` / `app.history(thread)` — inspect checkpoints.

## Backends

Two kinds of backend share one event stream, so a node calls either the same
way.

- Agent backends (`AgentBackend`) drive a session that runs its own tools and
  asks permission. Available: `KiroBackend` (persistent ACP session over
  `kiro-cli`), `CodexBackend` (`codex exec --json`), `ClaudeCodeBackend`
  (`claude -p --output-format stream-json`).
- LLM backends (`LLMBackend`) are stateless: messages in, token stream out.
  They never run tools themselves — a tool call is a request the graph
  fulfils. `OllamaBackend` is the example and the template for any
  OpenAI-compatible endpoint.

```python
from agentflow.backends.kiro import KiroBackend
from agentflow.backends.codex import CodexBackend
from agentflow.backends.claude_code import ClaudeCodeBackend
from agentflow.backends.ollama import OllamaBackend

agent = KiroBackend("vibe", engine="v3")          # persistent session
codex = CodexBackend(sandbox="read-only")          # one-shot per turn
claude = ClaudeCodeBackend(model="sonnet")         # one-shot per turn
llm = OllamaBackend("llama3.2")                     # local HTTP

await agent.start()
async for event in agent.prompt("summarize the repo"):
    ...  # TextChunk, ToolCall, ToolResult, ..., TurnEnd
await agent.close()
```

The three agent backends have different lifecycles under one interface. Kiro
holds a long-lived JSON-RPC session; Codex and Claude Code run a fresh
subprocess per turn and carry a resumable session id between turns. Either way
you call `start()`, `prompt(text)`, `close()` and consume the same event
stream. Backends are constructed by you and passed into your nodes. The core
never imports a backend, so importing `agentflow` pulls in no subprocess or
HTTP dependency.

Run `python examples/agents_demo.py` to smoke every backend installed on your
machine (set `KIRO_AGENT` to a valid agent id, e.g. `vibe`, to include Kiro).

### Permission policies

Agent backends answer permission prompts through a `PermissionPolicy`:
`AllowAll` (default, auto-approve), `DenyAll`, or `Interactive` (escalate to a
human via an engine interrupt).

## Checkpointing and human-in-the-loop

Pass a checkpointer to `compile` to make runs durable.

```python
from agentflow import FileCheckpointer
app = g.compile(checkpointer=FileCheckpointer(".runs"))
```

`MemoryCheckpointer` is for tests; `FileCheckpointer` writes one atomic JSON
file per super-step under `.runs/<thread>/`.

A node calls `await ctx.interrupt(payload)` to suspend the run for a human.
The runtime writes an interrupted checkpoint and stops. Later, `await
app.resume(thread, value=answer)` continues the run, and the same `interrupt`
call returns `answer`. This works across process restarts, since the frontier
is persisted.

## Prebuilt patterns

`agentflow.prebuilt.iterate_until_converged(work, ...)` compiles the classic
baseline-then-iterate optimization loop as a graph. You supply an async
`work(state) -> Candidate`; the loop owns the best-so-far, the
no-improvement streak, and the stop decision (`done`, `optimal`, `converged`,
`exhausted`).

```python
from agentflow.prebuilt import Candidate, iterate_until_converged

async def work(state):
    best = state.get("best_score", 0.0)
    return Candidate(score=min(best + 25.0, 100.0))

app = iterate_until_converged(work, perfect_score=100.0, patience=4)
result = await app.invoke({})
```

`agentflow.prebuilt.tool_loop(llm, tools)` compiles the function-calling agent
loop as a two-node graph: an `agent` node calls the `LLMBackend`, a `tools`
node runs any tool the model requested and appends the result to the
conversation, and a conditional edge repeats until the model answers without a
tool call. The LLM never executes a tool itself — the graph does.

```python
from agentflow.prebuilt import Tool, tool_loop
from agentflow.events import Message
from agentflow.backends.ollama import OllamaBackend

async def multiply(a: float, b: float) -> str:
    return str(a * b)

tools = [Tool("multiply", multiply, description="Multiply two numbers",
              schema={"type": "object",
                      "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
                      "required": ["a", "b"]})]

llm = OllamaBackend("gpt-oss")
await llm.start()
app = tool_loop(llm, tools, max_turns=6)
out = await app.invoke({"messages": [Message("user", "What is 23 * 19?")]})
print(out["messages"][-1].content)   # -> the model's final answer
await llm.close()
```

See `examples/tool_loop_ollama.py` for a runnable version.

## Project layout

```text
agentflow/
  state.py            channels, reducers, update merge
  graph.py            Graph builder + validation
  runtime.py          super-step scheduler, Context, interrupts
  compiled.py         CompiledGraph runnable
  events.py           messages, requests, streaming events
  errors.py           exception hierarchy
  backends/           base protocols + kiro + ollama adapters
  checkpoint/         Checkpointer protocol + memory + file
  prebuilt/           iterate_until_converged
examples/             runnable examples
tests/                pytest suite (async)
DESIGN.md             architecture and contracts
```

## Development

```bash
pip install -e '.[ollama,dev]'
pytest -q
```

Tests are async and run under `pytest-asyncio` in `auto` mode, so no
per-test decorator is needed.
