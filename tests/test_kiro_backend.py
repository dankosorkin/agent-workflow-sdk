"""Kiro backend transport tests against a scripted fake kiro-cli process.

No real ``kiro-cli`` is spawned. A fake process reads the client's JSON-RPC
requests off its stdin pipe and emits scripted responses, notifications, and
an agent->client permission request on its stdout pipe. This exercises the
async read loop, id correlation, event mapping, and the permission policy.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from agentflow.backends.base import AllowAll, DenyAll
from agentflow.backends.kiro import KiroBackend
from agentflow.events import TextChunk, ToolCall, ToolResult, TurnEnd


class _FakeStdin:
    """Collects the client's outgoing JSON-RPC lines and notifies a waiter."""

    def __init__(self) -> None:
        self.lines: list[dict] = []
        self._event = asyncio.Event()
        self.closed = False

    def write(self, data: bytes) -> None:
        for raw in data.decode("utf-8").splitlines():
            if raw.strip():
                self.lines.append(json.loads(raw))
        self._event.set()

    def is_closing(self) -> bool:
        return self.closed

    def close(self) -> None:
        self.closed = True

    async def wait_for(self, predicate):
        while True:
            for msg in self.lines:
                if predicate(msg):
                    return msg
            self._event.clear()
            await self._event.wait()


class _FakeStdout:
    """Serves scripted lines to the client's read loop."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()

    def feed(self, message: dict) -> None:
        self._queue.put_nowait((json.dumps(message) + "\n").encode("utf-8"))

    def feed_eof(self) -> None:
        self._queue.put_nowait(b"")

    async def readline(self) -> bytes:
        return await self._queue.get()


class _FakeProc:
    def __init__(self) -> None:
        self.stdin = _FakeStdin()
        self.stdout = _FakeStdout()
        self.returncode = None

    def terminate(self) -> None:
        self.returncode = -15

    def kill(self) -> None:
        self.returncode = -9

    async def wait(self) -> int:
        return self.returncode or 0


async def _drive_setup(proc: _FakeProc) -> None:
    """Answer initialize + session/new so start() completes (engine v2)."""
    init = await proc.stdin.wait_for(lambda m: m.get("method") == "initialize")
    proc.stdout.feed({"jsonrpc": "2.0", "id": init["id"], "result": {"agentInfo": {"name": "fake"}}})
    new = await proc.stdin.wait_for(lambda m: m.get("method") == "session/new")
    proc.stdout.feed({"jsonrpc": "2.0", "id": new["id"], "result": {"sessionId": "sess-1"}})


@pytest.fixture
def fake_proc(monkeypatch):
    proc = _FakeProc()

    async def _fake_exec(*args, **kwargs):
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_exec)
    return proc


async def test_prompt_streams_events_and_ends(fake_proc):
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())  # v2 avoids set_mode path
    setup = asyncio.create_task(_drive_setup(fake_proc))
    await backend.start()
    await setup

    async def _drive_turn() -> None:
        prompt = await fake_proc.stdin.wait_for(lambda m: m.get("method") == "session/prompt")
        pid = prompt["id"]
        # stream: two text chunks, a tool call, a tool result
        fake_proc.stdout.feed({"jsonrpc": "2.0", "method": "session/update",
                               "params": {"update": {"sessionUpdate": "agent_message_chunk",
                                                       "content": {"text": "Hel"}}}})
        fake_proc.stdout.feed({"jsonrpc": "2.0", "method": "session/update",
                               "params": {"update": {"sessionUpdate": "agent_message_chunk",
                                                       "content": {"text": "lo"}}}})
        fake_proc.stdout.feed({"jsonrpc": "2.0", "method": "session/update",
                               "params": {"update": {"sessionUpdate": "tool_call",
                                                       "toolCallId": "t1", "name": "search",
                                                       "args": {"q": "x"}}}})
        fake_proc.stdout.feed({"jsonrpc": "2.0", "method": "session/update",
                               "params": {"update": {"sessionUpdate": "tool_call_update",
                                                       "toolCallId": "t1", "status": "completed"}}})
        # terminal response
        fake_proc.stdout.feed({"jsonrpc": "2.0", "id": pid, "result": {"stopReason": "end_turn"}})

    driver = asyncio.create_task(_drive_turn())
    events = [e async for e in backend.prompt("hi")]
    await driver

    kinds = [type(e).__name__ for e in events]
    assert kinds == ["TextChunk", "TextChunk", "ToolCall", "ToolResult", "TurnEnd"]
    assert events[0] == TextChunk("Hel")
    assert isinstance(events[2], ToolCall) and events[2].name == "search"
    assert isinstance(events[3], ToolResult) and events[3].status == "ok"
    end = events[-1]
    assert isinstance(end, TurnEnd) and end.text == "Hello" and end.stop_reason == "end_turn"

    await backend.close()


async def test_permission_allow(fake_proc):
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())
    setup = asyncio.create_task(_drive_setup(fake_proc))
    await backend.start()
    await setup

    async def _drive_turn() -> None:
        prompt = await fake_proc.stdin.wait_for(lambda m: m.get("method") == "session/prompt")
        pid = prompt["id"]
        # agent asks for permission (agent->client request with an id)
        fake_proc.stdout.feed({"jsonrpc": "2.0", "id": 9001, "method": "session/request_permission",
                               "params": {"toolName": "fs_write",
                                          "options": [{"optionId": "a", "name": "Allow always", "kind": "allow_always"},
                                                      {"optionId": "o", "name": "Once", "kind": "allow_once"}]}})
        # client must answer 9001 before we finish; wait for it
        ans = await fake_proc.stdin.wait_for(lambda m: m.get("id") == 9001 and "result" in m)
        assert ans["result"]["outcome"]["outcome"] == "selected"
        assert ans["result"]["outcome"]["optionId"] == "a"  # allow_always preferred
        fake_proc.stdout.feed({"jsonrpc": "2.0", "id": pid, "result": {"stopReason": "end_turn"}})

    driver = asyncio.create_task(_drive_turn())
    events = [e async for e in backend.prompt("do it")]
    await driver
    assert isinstance(events[-1], TurnEnd)
    await backend.close()


async def test_permission_deny(fake_proc):
    backend = KiroBackend("fake-agent", engine="v2", permission=DenyAll())
    setup = asyncio.create_task(_drive_setup(fake_proc))
    await backend.start()
    await setup

    async def _drive_turn() -> None:
        prompt = await fake_proc.stdin.wait_for(lambda m: m.get("method") == "session/prompt")
        pid = prompt["id"]
        fake_proc.stdout.feed({"jsonrpc": "2.0", "id": 9002, "method": "session/request_permission",
                               "params": {"toolName": "fs_write",
                                          "options": [{"optionId": "a", "name": "Allow", "kind": "allow_once"}]}})
        ans = await fake_proc.stdin.wait_for(lambda m: m.get("id") == 9002 and "result" in m)
        assert ans["result"]["outcome"]["outcome"] == "cancelled"
        fake_proc.stdout.feed({"jsonrpc": "2.0", "id": pid, "result": {"stopReason": "end_turn"}})

    driver = asyncio.create_task(_drive_turn())
    events = [e async for e in backend.prompt("do it")]
    await driver
    assert isinstance(events[-1], TurnEnd)
    await backend.close()
