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
from agentflow.errors import BackendError, BackendTransportError
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

    async def drain(self) -> None:
        return None

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
    proc.stdout.feed(
        {"jsonrpc": "2.0", "id": init["id"], "result": {"agentInfo": {"name": "fake"}}}
    )
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
    backend = KiroBackend(
        "fake-agent", engine="v2", permission=AllowAll()
    )  # v2 avoids set_mode path
    setup = asyncio.create_task(_drive_setup(fake_proc))
    await backend.start()
    await setup

    async def _drive_turn() -> None:
        prompt = await fake_proc.stdin.wait_for(lambda m: m.get("method") == "session/prompt")
        pid = prompt["id"]
        # stream: two text chunks, a tool call, a tool result
        fake_proc.stdout.feed(
            {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "update": {"sessionUpdate": "agent_message_chunk", "content": {"text": "Hel"}}
                },
            }
        )
        fake_proc.stdout.feed(
            {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "update": {"sessionUpdate": "agent_message_chunk", "content": {"text": "lo"}}
                },
            }
        )
        fake_proc.stdout.feed(
            {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "tool_call",
                        "toolCallId": "t1",
                        "name": "search",
                        "args": {"q": "x"},
                    }
                },
            }
        )
        fake_proc.stdout.feed(
            {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": "t1",
                        "status": "completed",
                    }
                },
            }
        )
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
        fake_proc.stdout.feed(
            {
                "jsonrpc": "2.0",
                "id": 9001,
                "method": "session/request_permission",
                "params": {
                    "toolName": "fs_write",
                    "options": [
                        {"optionId": "a", "name": "Allow always", "kind": "allow_always"},
                        {"optionId": "o", "name": "Once", "kind": "allow_once"},
                    ],
                },
            }
        )
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
        fake_proc.stdout.feed(
            {
                "jsonrpc": "2.0",
                "id": 9002,
                "method": "session/request_permission",
                "params": {
                    "toolName": "fs_write",
                    "options": [{"optionId": "a", "name": "Allow", "kind": "allow_once"}],
                },
            }
        )
        ans = await fake_proc.stdin.wait_for(lambda m: m.get("id") == 9002 and "result" in m)
        assert ans["result"]["outcome"]["outcome"] == "cancelled"
        fake_proc.stdout.feed({"jsonrpc": "2.0", "id": pid, "result": {"stopReason": "end_turn"}})

    driver = asyncio.create_task(_drive_turn())
    events = [e async for e in backend.prompt("do it")]
    await driver
    assert isinstance(events[-1], TurnEnd)
    await backend.close()


# ---------------------------------------------------------------------------
# Transport / lifecycle error branches
# ---------------------------------------------------------------------------


async def test_start_oserror_wrapped(monkeypatch):
    async def _boom(*args, **kwargs):
        raise OSError("no such binary")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _boom)
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())
    with pytest.raises(BackendTransportError, match="could not start kiro-cli"):
        await backend.start()


async def test_close_without_start_is_noop():
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())
    await backend.close()  # no process — must not raise


async def test_close_terminates_then_kills_on_timeout(fake_proc, monkeypatch):
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())
    setup = asyncio.create_task(_drive_setup(fake_proc))
    await backend.start()
    await setup

    # proc.wait() never returns -> close() must terminate, then kill.
    async def _never() -> int:
        await asyncio.sleep(3600)
        return 0

    monkeypatch.setattr(fake_proc, "wait", _never)

    async def _fast_wait_for(coro, timeout):
        # Force both wait_for calls in close() to time out immediately.
        coro.close()
        raise TimeoutError

    monkeypatch.setattr(asyncio, "wait_for", _fast_wait_for)
    await backend.close()
    assert fake_proc.returncode == -9  # kill() was reached


async def test_turn_rpc_failure_yields_error_event(fake_proc):
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())
    setup = asyncio.create_task(_drive_setup(fake_proc))
    await backend.start()
    await setup

    async def _drive_turn() -> None:
        prompt = await fake_proc.stdin.wait_for(lambda m: m.get("method") == "session/prompt")
        # Respond with an RPC error to the prompt -> finisher emits ErrorEvent.
        fake_proc.stdout.feed(
            {"jsonrpc": "2.0", "id": prompt["id"], "error": {"code": -1, "message": "boom"}}
        )

    driver = asyncio.create_task(_drive_turn())
    events = [e async for e in backend.prompt("hi")]
    await driver
    kinds = [type(e).__name__ for e in events]
    assert kinds[-2:] == ["ErrorEvent", "TurnEnd"]
    assert events[-1].stop_reason == "error"
    await backend.close()


async def test_write_without_process_raises():
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())
    # No process started.
    with pytest.raises(BackendTransportError, match="not running"):
        backend._write({"jsonrpc": "2.0", "id": 1, "method": "x", "params": {}})


# ---------------------------------------------------------------------------
# invoke() guards + session setup branches
# ---------------------------------------------------------------------------


async def test_invoke_rejects_non_text_request(fake_proc):
    from agentflow.events import ChatRequest

    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())
    setup = asyncio.create_task(_drive_setup(fake_proc))
    await backend.start()
    await setup
    with pytest.raises(BackendError, match="accepts TextRequest"):
        async for _ in backend.invoke(ChatRequest(messages=[])):
            pass
    await backend.close()


async def test_invoke_before_start_raises():
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())
    with pytest.raises(BackendError, match="start.. was not called"):
        async for _ in backend.prompt("hi"):
            pass


async def test_double_turn_rejected(fake_proc):
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())
    setup = asyncio.create_task(_drive_setup(fake_proc))
    await backend.start()
    await setup

    gen = backend.prompt("first")
    # Drive the generator to the point where the prompt request is sent and the
    # turn queue is registered (first __anext__ blocks awaiting an event).
    first_step = asyncio.ensure_future(gen.__anext__())
    await fake_proc.stdin.wait_for(lambda m: m.get("method") == "session/prompt")

    # A turn is now in flight; starting another must raise.
    with pytest.raises(BackendError, match="already in progress"):
        async for _ in backend.prompt("second"):
            pass

    # Finish the first turn cleanly and drain it.
    prompt = next(m for m in fake_proc.stdin.lines if m.get("method") == "session/prompt")
    fake_proc.stdout.feed({"jsonrpc": "2.0", "id": prompt["id"], "result": {"stopReason": "end"}})
    first_event = await first_step
    assert isinstance(first_event, TurnEnd)
    async for _ in gen:
        pass
    await backend.close()


async def test_session_new_without_session_id(fake_proc):
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())

    async def _bad_setup() -> None:
        init = await fake_proc.stdin.wait_for(lambda m: m.get("method") == "initialize")
        fake_proc.stdout.feed({"jsonrpc": "2.0", "id": init["id"], "result": {}})
        new = await fake_proc.stdin.wait_for(lambda m: m.get("method") == "session/new")
        fake_proc.stdout.feed({"jsonrpc": "2.0", "id": new["id"], "result": {}})  # no sessionId

    setup = asyncio.create_task(_bad_setup())
    with pytest.raises(BackendTransportError, match="no sessionId"):
        await backend.start()
    await setup


async def _drive_v3_setup(proc, *, current="fake-agent", available=("fake-agent",)) -> None:
    init = await proc.stdin.wait_for(lambda m: m.get("method") == "initialize")
    proc.stdout.feed({"jsonrpc": "2.0", "id": init["id"], "result": {}})
    new = await proc.stdin.wait_for(lambda m: m.get("method") == "session/new")
    proc.stdout.feed(
        {
            "jsonrpc": "2.0",
            "id": new["id"],
            "result": {
                "sessionId": "sess-v3",
                "modes": {
                    "currentModeId": current,
                    "availableModes": [{"id": m} for m in available],
                },
            },
        }
    )


async def test_v3_mode_already_current_skips_set_mode(fake_proc):
    backend = KiroBackend("fake-agent", engine="v3", permission=AllowAll())
    setup = asyncio.create_task(_drive_v3_setup(fake_proc, current="fake-agent"))
    await backend.start()
    await setup
    # No session/set_mode should have been sent.
    assert not any(m.get("method") == "session/set_mode" for m in fake_proc.stdin.lines)
    await backend.close()


async def test_v3_mode_not_available_raises(fake_proc):
    backend = KiroBackend("fake-agent", engine="v3", permission=AllowAll())
    setup = asyncio.create_task(_drive_v3_setup(fake_proc, current="other", available=("other",)))
    with pytest.raises(BackendError, match="not in available modes"):
        await backend.start()
    await setup


async def test_v3_mode_switch_sends_set_mode(fake_proc):
    backend = KiroBackend("fake-agent", engine="v3", permission=AllowAll())

    async def _setup_and_confirm() -> None:
        await _drive_v3_setup(fake_proc, current="other", available=("other", "fake-agent"))
        sm = await fake_proc.stdin.wait_for(lambda m: m.get("method") == "session/set_mode")
        fake_proc.stdout.feed({"jsonrpc": "2.0", "id": sm["id"], "result": {}})

    setup = asyncio.create_task(_setup_and_confirm())
    await backend.start()
    await setup
    sent = [m for m in fake_proc.stdin.lines if m.get("method") == "session/set_mode"]
    assert sent and sent[0]["params"]["modeId"] == "fake-agent"
    await backend.close()


async def test_v2_set_model_sent(fake_proc):
    backend = KiroBackend("fake-agent", engine="v2", model="claude-x", permission=AllowAll())

    async def _setup_and_model() -> None:
        await _drive_setup(fake_proc)
        sm = await fake_proc.stdin.wait_for(lambda m: m.get("method") == "session/set_model")
        fake_proc.stdout.feed({"jsonrpc": "2.0", "id": sm["id"], "result": {}})

    setup = asyncio.create_task(_setup_and_model())
    await backend.start()
    await setup
    sent = [m for m in fake_proc.stdin.lines if m.get("method") == "session/set_model"]
    assert sent and sent[0]["params"]["modelId"] == "claude-x"
    await backend.close()


# ---------------------------------------------------------------------------
# Reader loop / dispatch branches
# ---------------------------------------------------------------------------


async def test_reader_eof_fails_pending(fake_proc):
    """An unexpected process exit fails any in-flight setup RPC."""
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())

    async def _eof_during_init() -> None:
        await fake_proc.stdin.wait_for(lambda m: m.get("method") == "initialize")
        fake_proc.returncode = 1
        fake_proc.stdout.feed_eof()

    setup = asyncio.create_task(_eof_during_init())
    with pytest.raises(BackendTransportError, match="terminated unexpectedly"):
        await backend.start()
    await setup


async def test_reader_invalid_json_fails_pending(fake_proc):
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())

    async def _bad_json_during_init() -> None:
        await fake_proc.stdin.wait_for(lambda m: m.get("method") == "initialize")
        fake_proc.stdout._queue.put_nowait(b"{not json\n")

    setup = asyncio.create_task(_bad_json_during_init())
    with pytest.raises(BackendTransportError, match="invalid JSON"):
        await backend.start()
    await setup


async def test_dispatch_response_with_non_int_id_ignored(fake_proc):
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())
    setup = asyncio.create_task(_drive_setup(fake_proc))
    await backend.start()
    await setup
    # A stray response with a string id must be ignored (no crash).
    fake_proc.stdout.feed({"jsonrpc": "2.0", "id": "not-int", "result": {}})
    await asyncio.sleep(0.02)
    await backend.close()


async def test_dispatch_scalar_and_null_results(fake_proc):
    """result=None -> {} ; scalar result -> {'value': ...}."""
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())
    setup = asyncio.create_task(_drive_setup(fake_proc))
    await backend.start()
    await setup

    null_fut = backend._send_request("x/null", {})
    scalar_fut = backend._send_request("x/scalar", {})
    null_id = next(m["id"] for m in fake_proc.stdin.lines if m.get("method") == "x/null")
    scalar_id = next(m["id"] for m in fake_proc.stdin.lines if m.get("method") == "x/scalar")
    fake_proc.stdout.feed({"jsonrpc": "2.0", "id": null_id, "result": None})
    fake_proc.stdout.feed({"jsonrpc": "2.0", "id": scalar_id, "result": 42})
    assert await null_fut == {}
    assert await scalar_fut == {"value": 42}
    await backend.close()


async def test_unsupported_agent_method_answered_with_error(fake_proc):
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())
    setup = asyncio.create_task(_drive_setup(fake_proc))
    await backend.start()
    await setup
    # Agent -> client request with an unknown method must get a -32601 error.
    fake_proc.stdout.feed({"jsonrpc": "2.0", "id": 7777, "method": "session/mystery", "params": {}})
    err = await fake_proc.stdin.wait_for(lambda m: m.get("id") == 7777 and "error" in m)
    assert err["error"]["code"] == -32601
    await backend.close()


# ---------------------------------------------------------------------------
# Permission edge cases + notification mapping
# ---------------------------------------------------------------------------


async def test_permission_allow_falls_back_to_allow_once(fake_proc):
    """Allow with no explicit option -> prefers allow_always, else allow_once."""
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())
    setup = asyncio.create_task(_drive_setup(fake_proc))
    await backend.start()
    await setup

    async def _drive_turn() -> None:
        prompt = await fake_proc.stdin.wait_for(lambda m: m.get("method") == "session/prompt")
        fake_proc.stdout.feed(
            {
                "jsonrpc": "2.0",
                "id": 8001,
                "method": "session/request_permission",
                "params": {
                    "toolName": "fs_write",
                    "options": [{"optionId": "once", "name": "Once", "kind": "allow_once"}],
                },
            }
        )
        ans = await fake_proc.stdin.wait_for(lambda m: m.get("id") == 8001 and "result" in m)
        assert ans["result"]["outcome"]["optionId"] == "once"
        fake_proc.stdout.feed({"jsonrpc": "2.0", "id": prompt["id"], "result": {"stopReason": "e"}})

    driver = asyncio.create_task(_drive_turn())
    events = [e async for e in backend.prompt("go")]
    await driver
    assert isinstance(events[-1], TurnEnd)
    await backend.close()


async def test_permission_allow_no_allow_option_cancels(fake_proc):
    """Allow but the agent offered no allow_* option -> cancelled."""
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())
    setup = asyncio.create_task(_drive_setup(fake_proc))
    await backend.start()
    await setup

    async def _drive_turn() -> None:
        prompt = await fake_proc.stdin.wait_for(lambda m: m.get("method") == "session/prompt")
        fake_proc.stdout.feed(
            {
                "jsonrpc": "2.0",
                "id": 8002,
                "method": "session/request_permission",
                "params": {
                    "tool": "fs_write",  # alt key: "tool" not "toolName"
                    "options": [{"optionId": "r", "name": "Reject", "kind": "reject_once"}],
                },
            }
        )
        ans = await fake_proc.stdin.wait_for(lambda m: m.get("id") == 8002 and "result" in m)
        assert ans["result"]["outcome"]["outcome"] == "cancelled"
        fake_proc.stdout.feed({"jsonrpc": "2.0", "id": prompt["id"], "result": {"stopReason": "e"}})

    driver = asyncio.create_task(_drive_turn())
    events = [e async for e in backend.prompt("go")]
    await driver
    assert isinstance(events[-1], TurnEnd)
    await backend.close()


async def test_notification_mapping_edge_cases(fake_proc):
    """Empty chunk dropped; tool_call alt keys; tool_call_update error status."""
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())
    setup = asyncio.create_task(_drive_setup(fake_proc))
    await backend.start()
    await setup

    async def _drive_turn() -> None:
        prompt = await fake_proc.stdin.wait_for(lambda m: m.get("method") == "session/prompt")
        # empty text chunk -> no event
        fake_proc.stdout.feed(
            {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {"update": {"sessionUpdate": "agent_message_chunk", "content": {}}},
            }
        )
        # tool_call via alt keys (id/toolName/input)
        fake_proc.stdout.feed(
            {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "tool_call",
                        "id": "tc9",
                        "toolName": "grep",
                        "input": {"q": "z"},
                    }
                },
            }
        )
        # tool_call_update with a failing status -> ToolResult status "error"
        fake_proc.stdout.feed(
            {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "tool_call_update",
                        "id": "tc9",
                        "status": "failed",
                    }
                },
            }
        )
        # an unknown update type -> ignored
        fake_proc.stdout.feed(
            {
                "jsonrpc": "2.0",
                "method": "session/update",
                "params": {"update": {"sessionUpdate": "mystery"}},
            }
        )
        fake_proc.stdout.feed({"jsonrpc": "2.0", "id": prompt["id"], "result": {"stopReason": "e"}})

    driver = asyncio.create_task(_drive_turn())
    events = [e async for e in backend.prompt("go")]
    await driver
    tool_calls = [e for e in events if isinstance(e, ToolCall)]
    tool_results = [e for e in events if isinstance(e, ToolResult)]
    assert tool_calls and tool_calls[0].name == "grep" and tool_calls[0].args == {"q": "z"}
    assert tool_results and tool_results[0].status == "error"
    # the empty-content chunk produced no TextChunk event
    assert not [e for e in events if isinstance(e, TextChunk)]
    await backend.close()


def test_map_status_variants():
    from agentflow.backends.kiro import _map_status

    assert _map_status("completed") == "ok"
    assert _map_status("success") == "ok"
    assert _map_status("failed") == "error"
    assert _map_status("error") == "error"
    assert _map_status("running") == "pending"
    assert _map_status(None) == "pending"


def test_emit_without_active_queue_is_noop():
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())
    # No turn in flight -> _turn_queue is None; _emit must silently drop.
    backend._emit(TextChunk("orphan"))  # must not raise


async def test_permission_future_cancelled_on_early_teardown(fake_proc):
    """If the turn generator is torn down while a permission is pending, the
    pending answer future is cancelled and the RPC is left unanswered (the
    interrupt/abandon path) without killing the reader."""
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())
    setup = asyncio.create_task(_drive_setup(fake_proc))
    await backend.start()
    await setup

    async def _drive_turn() -> None:
        await fake_proc.stdin.wait_for(lambda m: m.get("method") == "session/prompt")
        # Emit a permission request but never send the prompt's terminal result.
        fake_proc.stdout.feed(
            {
                "jsonrpc": "2.0",
                "id": 8500,
                "method": "session/request_permission",
                "params": {
                    "toolName": "fs_write",
                    "options": [{"optionId": "a", "name": "Allow", "kind": "allow_once"}],
                },
            }
        )

    driver = asyncio.create_task(_drive_turn())
    gen = backend.prompt("go")
    # Pull one event (the PermissionRequest is resolved inside the generator;
    # AllowAll answers it) — then abandon the generator mid-turn.
    first = await gen.__anext__()
    await gen.aclose()  # tear down before the turn completed
    await driver

    # The backend recovered: a fresh turn can start.
    assert backend._turn_queue is None
    assert first is not None
    await backend.close()


def test_extract_tool_name_v3_and_fallbacks():
    from agentflow.backends.kiro import _extract_tool_name

    # v3 shape: name lives under _meta.kiro.toolId
    v3 = {
        "toolCall": {"toolCallId": "t1", "title": "Write File", "status": "pending"},
        "_meta": {"kiro": {"toolId": "fs_write", "consent": {"capability": "fs_write"}}},
    }
    assert _extract_tool_name(v3) == "fs_write"

    # capability fallback when toolId missing
    cap = {"_meta": {"kiro": {"consent": {"capability": "fs_read"}}}}
    assert _extract_tool_name(cap) == "fs_read"

    # toolCall.title fallback when no _meta
    title_only = {"toolCall": {"title": "Execute Bash"}}
    assert _extract_tool_name(title_only) == "Execute Bash"

    # legacy flat keys still work
    assert _extract_tool_name({"toolName": "grep"}) == "grep"
    assert _extract_tool_name({"tool": "ls"}) == "ls"

    # nothing recognizable -> empty string (no crash)
    assert _extract_tool_name({}) == ""
