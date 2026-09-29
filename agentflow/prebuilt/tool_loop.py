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

"""Tool-calling agent loop over an LLMBackend, as a graph.

The classic function-calling loop: call the model with the conversation and
the available tool schemas; if it asks to call tools, run them, append their
results to the conversation, and call the model again; repeat until the model
answers without requesting a tool.

This is expressed as a real two-node graph — an ``agent`` node that calls the
LLM and a ``tools`` node that executes the requested tools — with a
conditional edge between them. The LLM never runs a tool itself; the graph
owns execution, which is the whole point of the ``LLMBackend`` split.

    async def get_weather(city: str) -> str: ...
    tools = [Tool("get_weather", get_weather,
                  description="Get weather for a city",
                  schema={"type": "object",
                          "properties": {"city": {"type": "string"}},
                          "required": ["city"]})]
    app = tool_loop(ollama, tools)
    out = await app.invoke({"messages": [Message("user", "weather in Paris?")]})
    print(out["messages"][-1].content)
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Annotated, Any

from agentflow.backends.base import LLMBackend
from agentflow.events import (
    ChatRequest,
    Message,
    TextChunk,
    ToolCall,
    ToolSpec,
    TurnEnd,
)
from agentflow.graph import END, START, Graph
from agentflow.state import State, add, append, last

__all__ = ["Tool", "ToolLoopState", "tool_loop"]


@dataclass(frozen=True)
class Tool:
    """A callable the model may invoke, plus the schema advertised to it.

    ``fn`` may be sync or async and is called with the model-supplied argument
    dict as keyword arguments. Its return value becomes the tool result
    content (stringified for the model).
    """

    name: str
    fn: Callable[..., Any]
    description: str = ""
    schema: dict[str, Any] = field(default_factory=dict)

    def spec(self) -> ToolSpec:
        return ToolSpec(name=self.name, description=self.description, schema=self.schema)


class ToolLoopState(State):
    """Conversation plus loop bookkeeping."""

    messages: Annotated[list, append]  # the running chat transcript
    turns: Annotated[int, add]  # model calls made
    _pending: Annotated[list, last]  # tool calls awaiting execution
    status: Annotated[str, last]  # terminal status (see tool_loop docs)


async def _run_tool(tool: Tool, args: dict[str, Any]) -> tuple[str, bool]:
    """Execute a tool. Returns (content, ok)."""
    try:
        result = tool.fn(**args)
        if inspect.isawaitable(result):
            result = await result
        return (result if isinstance(result, str) else repr(result)), True
    except Exception as exc:  # noqa: BLE001 - surface tool errors to the model
        return f"tool {tool.name!r} raised {type(exc).__name__}: {exc}", False


def tool_loop(
    llm: LLMBackend,
    tools: list[Tool],
    *,
    max_turns: int = 10,
    stream_text: bool = False,
    checkpointer: Any = None,
):
    """Compile a tool-calling loop around ``llm`` and ``tools``.

    The final state carries the full ``messages`` transcript and a terminal
    ``status`` the caller MUST check — the last message is only guaranteed to
    be a tool-free answer when ``status == "completed"``:

    - ``"completed"`` — the model answered without requesting a tool.
    - ``"tool_calls_unresolved"`` — ``max_turns`` was hit while the last
      assistant message still had pending tool calls. The plan is incomplete;
      do not treat the last message as a final answer.

    ``max_turns`` bounds model calls.

    When ``stream_text`` is true, the agent node forwards the model's
    ``TextChunk``s to the run stream via ``ctx.emit`` so a caller streaming the
    compiled graph sees tokens as they arrive.
    """
    by_name = {t.name: t for t in tools}
    specs = [t.spec() for t in tools]

    async def agent(state: dict, ctx) -> dict:
        messages: list[Message] = list(state.get("messages", []))
        final: TurnEnd | None = None
        async for event in llm.invoke(ChatRequest(messages=tuple(messages), tools=tuple(specs))):
            if stream_text and isinstance(event, TextChunk):
                ctx.emit(event)
            elif isinstance(event, TurnEnd):
                final = event

        if final is None:
            raise RuntimeError("LLM turn produced no TurnEnd")

        assistant = final.message or Message(role="assistant", content=final.text)
        pending = [{"id": tc.id, "name": tc.name, "args": tc.args} for tc in assistant.tool_calls]
        turns_after = state.get("turns", 0) + 1

        # Decide the terminal status if the loop is about to stop.
        if not pending:
            status = "completed"  # model answered, no tools
        elif turns_after >= max_turns:
            status = "tool_calls_unresolved"  # hit cap with tools pending
        else:
            status = "running"  # will loop into tools

        return {
            "messages": [assistant],
            "turns": 1,
            "_pending": pending,
            "status": status,
        }

    async def run_tools(state: dict, ctx) -> dict:
        pending = state.get("_pending") or []
        results: list[Message] = []
        for call in pending:
            tool = by_name.get(call["name"])
            if tool is None:
                content, ok = f"unknown tool {call['name']!r}", False
            else:
                content, ok = await _run_tool(tool, call.get("args") or {})
            ctx.emit(ToolCall(id=call["id"], name=call["name"], args=call.get("args") or {}))
            results.append(
                Message(role="tool", content=content, tool_call_id=call["id"], name=call["name"])
            )
        # Clear pending; append tool results to the transcript.
        return {"messages": results, "_pending": []}

    def route(state: Mapping[str, Any]) -> str:
        pending = state.get("_pending") or []
        if pending and state.get("turns", 0) < max_turns:
            return "tools"
        return "done"

    g = Graph(ToolLoopState)
    g.add_node("agent", agent)
    g.add_node("tools", run_tools)
    g.add_edge(START, "agent")
    g.add_conditional_edges("agent", route, {"tools": "tools", "done": END})
    g.add_edge("tools", "agent")
    return g.compile(checkpointer=checkpointer, step_limit=max_turns * 2 + 5)
