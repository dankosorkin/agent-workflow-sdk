"""Offline parser tests for Codex and Claude Code backends.

Feeds the real JSONL event shapes captured from `codex exec --json` and
`claude -p --output-format stream-json` through each backend's parse_line,
and drives a full turn against a fake subprocess for the CLIExecBackend base.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from agentflow.backends.base import AllowAll
from agentflow.backends.cli_exec import CLIExecBackend, TurnAccumulator
from agentflow.backends.claude_code import ClaudeCodeBackend
from agentflow.backends.codex import CodexBackend
from agentflow.events import TextChunk, ToolCall, ToolResult, TurnEnd


# --- captured Codex event stream (trimmed to the essentials) ---
CODEX_LINES = [
    {"type": "thread.started", "thread_id": "01a0ebda-5b74-7c51"},
    {"type": "turn.started"},
    {"type": "item.completed", "item": {"id": "item_0", "type": "agent_message", "text": "pong"}},
    {"type": "turn.completed", "usage": {"output_tokens": 5}},
]

# --- captured Claude Code stream (trimmed) ---
CLAUDE_LINES = [
    {"type": "system", "subtype": "init", "session_id": "e8d6202d", "model": "claude-sonnet-5"},
    {"type": "stream_event", "event": {"type": "content_block_delta",
     "delta": {"type": "text_delta", "text": "po"}}},
    {"type": "stream_event", "event": {"type": "content_block_delta",
     "delta": {"type": "text_delta", "text": "ng"}}},
    {"type": "result", "subtype": "success", "result": "pong",
     "stop_reason": "end_turn", "session_id": "e8d6202d", "is_error": False},
]


def _feed(backend, lines):
    acc = TurnAccumulator()
    events = []
    for obj in lines:
        events.extend(backend.parse_line(obj, acc))
    return events, acc


def test_codex_parses_agent_message():
    backend = CodexBackend(permission=AllowAll())
    events, acc = _feed(backend, CODEX_LINES)
    assert acc.session_id == "01a0ebda-5b74-7c51"
    assert acc.stop_reason == "end_turn"
    text_chunks = [e for e in events if isinstance(e, TextChunk)]
    assert "".join(c.text for c in text_chunks) == "pong"


def test_codex_parses_command_execution():
    backend = CodexBackend(permission=AllowAll())
    acc = TurnAccumulator()
    events = backend.parse_line(
        {"type": "item.completed",
         "item": {"id": "c1", "type": "command_execution",
                  "command": "ls", "exit_code": 0, "aggregated_output": "file.txt"}},
        acc,
    )
    calls = [e for e in events if isinstance(e, ToolCall)]
    results = [e for e in events if isinstance(e, ToolResult)]
    assert calls and calls[0].name == "shell" and calls[0].args["command"] == "ls"
    assert results and results[0].status == "ok"


def test_claude_parses_streamed_text():
    backend = ClaudeCodeBackend(permission=AllowAll())
    events, acc = _feed(backend, CLAUDE_LINES)
    assert acc.session_id == "e8d6202d"
    assert acc.stop_reason == "end_turn"
    assert acc.text == "pong"
    # result must NOT re-emit text because deltas already streamed
    text_chunks = [e for e in events if isinstance(e, TextChunk)]
    assert "".join(c.text for c in text_chunks) == "pong"


def test_claude_result_fallback_when_no_stream():
    """If partial messages didn't stream, the result text is the fallback."""
    backend = ClaudeCodeBackend(permission=AllowAll())
    acc = TurnAccumulator()
    events = []
    events += backend.parse_line({"type": "system", "subtype": "init", "session_id": "s"}, acc)
    events += backend.parse_line(
        {"type": "result", "result": "hello", "stop_reason": "end_turn", "is_error": False}, acc)
    text = [e for e in events if isinstance(e, TextChunk)]
    assert acc.text == "hello"
    assert text and text[0].text == "hello"


def test_claude_parses_tool_use():
    backend = ClaudeCodeBackend(permission=AllowAll())
    acc = TurnAccumulator()
    # stream some text first so tool_use isn't treated as text fallback
    backend.parse_line({"type": "stream_event", "event": {"type": "content_block_delta",
                        "delta": {"type": "text_delta", "text": "ok"}}}, acc)
    events = backend.parse_line(
        {"type": "assistant", "message": {"content": [
            {"type": "text", "text": "ok"},
            {"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "ls"}}]}},
        acc,
    )
    calls = [e for e in events if isinstance(e, ToolCall)]
    assert len(calls) == 1
    assert calls[0].name == "Bash" and calls[0].args == {"command": "ls"}


# --- full-turn drive of the CLIExecBackend base against a fake subprocess ---

class _FakeStdout:
    def __init__(self, lines: list[dict]):
        self._data = [(json.dumps(o) + "\n").encode() for o in lines] + [b""]
        self._i = 0

    async def readline(self) -> bytes:
        item = self._data[self._i]
        self._i += 1
        return item


class _FakeStderr:
    async def read(self) -> bytes:
        return b""


class _FakeProc:
    def __init__(self, lines):
        self.stdout = _FakeStdout(lines)
        self.stderr = _FakeStderr()
        self.stdin = None
        self.returncode = 0

    def kill(self):
        self.returncode = -9

    async def wait(self):
        return self.returncode


async def test_cli_exec_full_turn(monkeypatch):
    backend = CodexBackend(permission=AllowAll())

    async def fake_exec(*args, **kwargs):
        return _FakeProc(CODEX_LINES)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)

    events = [e async for e in backend.prompt("say pong")]
    kinds = [type(e).__name__ for e in events]
    assert kinds[-1] == "TurnEnd"
    end = events[-1]
    assert isinstance(end, TurnEnd) and end.text == "pong" and end.stop_reason == "end_turn"
    # session id captured for resume
    assert backend._session_id == "01a0ebda-5b74-7c51"
