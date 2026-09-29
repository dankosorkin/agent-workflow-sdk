"""Tests for the tool_loop prebuilt against a scripted fake LLM backend."""

from __future__ import annotations

from agentflow.backends.base import BaseLLMBackend
from agentflow.events import ChatRequest, Message, TextChunk, ToolCallSpec, TurnEnd
from agentflow.prebuilt import Tool, tool_loop


class ScriptedLLM(BaseLLMBackend):
    """Yields a preset sequence of turns, one per invoke() call.

    Each turn is either a tool-calling assistant message or a plain answer.
    """

    def __init__(self, turns: list[Message]):
        self._turns = turns
        self._i = 0
        self.seen_requests: list[ChatRequest] = []

    async def start(self): ...
    async def close(self): ...

    async def invoke(self, request, *, session=None):
        self.seen_requests.append(request)
        msg = self._turns[self._i]
        self._i += 1
        if msg.content:
            yield TextChunk(text=msg.content)
        yield TurnEnd(text=msg.content, stop_reason="end_turn", message=msg)


async def test_single_tool_call_then_answer():
    calls = []

    async def get_weather(city: str) -> str:
        calls.append(city)
        return f"{city}: sunny, 22C"

    tools = [
        Tool(
            "get_weather",
            get_weather,
            description="weather",
            schema={"type": "object", "properties": {"city": {"type": "string"}}},
        )
    ]

    llm = ScriptedLLM(
        [
            Message(
                role="assistant",
                content="",
                tool_calls=(ToolCallSpec(id="c1", name="get_weather", args={"city": "Paris"}),),
            ),
            Message(role="assistant", content="It's sunny and 22C in Paris."),
        ]
    )
    app = tool_loop(llm, tools)
    out = await app.invoke({"messages": [Message(role="user", content="weather in Paris?")]})

    assert calls == ["Paris"]
    assert out["turns"] == 2  # tool-call turn + final answer turn
    last = out["messages"][-1]
    assert last.role == "assistant" and "Paris" in last.content
    # transcript: user, assistant(tool_call), tool(result), assistant(answer)
    roles = [m.role for m in out["messages"]]
    assert roles == ["user", "assistant", "tool", "assistant"]
    tool_msg = out["messages"][2]
    assert tool_msg.tool_call_id == "c1" and "sunny" in tool_msg.content


async def test_no_tool_call_ends_immediately():
    llm = ScriptedLLM([Message(role="assistant", content="Hello!")])
    app = tool_loop(llm, [])
    out = await app.invoke({"messages": [Message(role="user", content="hi")]})
    assert out["turns"] == 1
    assert out["messages"][-1].content == "Hello!"


async def test_multiple_tool_calls_in_one_turn():
    log = []

    async def add(a: int, b: int) -> str:
        log.append(("add", a, b))
        return str(a + b)

    tools = [Tool("add", add)]
    llm = ScriptedLLM(
        [
            Message(
                role="assistant",
                content="",
                tool_calls=(
                    ToolCallSpec(id="c1", name="add", args={"a": 1, "b": 2}),
                    ToolCallSpec(id="c2", name="add", args={"a": 3, "b": 4}),
                ),
            ),
            Message(role="assistant", content="3 and 7"),
        ]
    )
    app = tool_loop(llm, tools)
    out = await app.invoke({"messages": [Message(role="user", content="add them")]})

    assert len(log) == 2
    tool_msgs = [m for m in out["messages"] if m.role == "tool"]
    assert {m.tool_call_id for m in tool_msgs} == {"c1", "c2"}
    assert {m.content for m in tool_msgs} == {"3", "7"}


async def test_tool_error_is_reported_to_model():
    async def boom(**kwargs) -> str:
        raise ValueError("nope")

    tools = [Tool("boom", boom)]
    llm = ScriptedLLM(
        [
            Message(
                role="assistant",
                content="",
                tool_calls=(ToolCallSpec(id="c1", name="boom", args={}),),
            ),
            Message(role="assistant", content="handled it"),
        ]
    )
    app = tool_loop(llm, tools)
    out = await app.invoke({"messages": [Message(role="user", content="go")]})
    tool_msg = [m for m in out["messages"] if m.role == "tool"][0]
    assert "ValueError" in tool_msg.content and "nope" in tool_msg.content


async def test_unknown_tool_reported():
    llm = ScriptedLLM(
        [
            Message(
                role="assistant",
                content="",
                tool_calls=(ToolCallSpec(id="c1", name="ghost", args={}),),
            ),
            Message(role="assistant", content="ok"),
        ]
    )
    app = tool_loop(llm, [])
    out = await app.invoke({"messages": [Message(role="user", content="go")]})
    tool_msg = [m for m in out["messages"] if m.role == "tool"][0]
    assert "unknown tool" in tool_msg.content


async def test_max_turns_stops_loop():
    # A model that always asks for a tool would loop forever; max_turns caps it.
    def make_turn():
        return Message(
            role="assistant", content="", tool_calls=(ToolCallSpec(id="c", name="noop", args={}),)
        )

    async def noop(**kwargs) -> str:
        return "ok"

    llm = ScriptedLLM([make_turn() for _ in range(10)])
    app = tool_loop(llm, [Tool("noop", noop)], max_turns=3)
    out = await app.invoke({"messages": [Message(role="user", content="go")]})
    assert out["turns"] == 3  # stopped at the cap
    # Terminal status must flag the incomplete plan, not look like a clean answer.
    assert out["status"] == "tool_calls_unresolved"


async def test_status_completed_on_clean_answer():
    llm = ScriptedLLM([Message(role="assistant", content="all done")])
    app = tool_loop(llm, [])
    out = await app.invoke({"messages": [Message(role="user", content="go")]})
    assert out["status"] == "completed"
    assert out["messages"][-1].content == "all done"


async def test_streams_text_when_enabled():
    llm = ScriptedLLM([Message(role="assistant", content="streamed answer")])
    app = tool_loop(llm, [], stream_text=True)
    events = [e async for e in app.stream({"messages": [Message(role="user", content="hi")]})]
    backend_events = [e for e in events if e.kind == "backend"]
    text = "".join(e.data.text for e in backend_events if isinstance(e.data, TextChunk))
    assert text == "streamed answer"
