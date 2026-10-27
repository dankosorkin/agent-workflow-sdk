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
import os
import signal

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


async def _started(fake_proc) -> KiroBackend:
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())
    setup = asyncio.create_task(_drive_setup(fake_proc))
    await backend.start()
    await setup
    return backend


def _force_timeouts(fake_proc, monkeypatch) -> None:
    """proc.wait() never returns and every wait_for in close() times out at once."""

    async def _never() -> int:
        await asyncio.sleep(3600)
        return 0

    async def _fast_wait_for(coro, timeout):
        coro.close()
        raise TimeoutError

    monkeypatch.setattr(fake_proc, "wait", _never)
    monkeypatch.setattr(asyncio, "wait_for", _fast_wait_for)


def _record_process_calls(fake_proc, monkeypatch) -> list[str]:
    calls: list[str] = []
    real_terminate, real_kill = fake_proc.terminate, fake_proc.kill

    def terminate() -> None:
        calls.append("terminate")
        real_terminate()

    def kill() -> None:
        calls.append("kill")
        real_kill()

    monkeypatch.setattr(fake_proc, "terminate", terminate)
    monkeypatch.setattr(fake_proc, "kill", kill)
    return calls


async def test_close_without_posix_signals_escalates_terminate_then_kill(fake_proc, monkeypatch):
    """Windows shape: no os.killpg / os.getpgid / signal.SIGKILL. close() must not
    read a missing attribute, and must fall back to proc.terminate() then proc.kill()."""
    backend = await _started(fake_proc)
    calls = _record_process_calls(fake_proc, monkeypatch)
    _force_timeouts(fake_proc, monkeypatch)
    monkeypatch.delattr(os, "killpg", raising=False)
    monkeypatch.delattr(os, "getpgid", raising=False)
    monkeypatch.delattr(signal, "SIGKILL", raising=False)

    await backend.close()  # would raise AttributeError before the fix

    assert calls == ["terminate", "kill"]
    assert fake_proc.returncode == -9


async def test_close_signals_the_process_group_on_posix(fake_proc, monkeypatch):
    """With process groups available the signals go to the group, not the process."""
    backend = await _started(fake_proc)
    fake_proc.pid = 4242
    calls = _record_process_calls(fake_proc, monkeypatch)
    _force_timeouts(fake_proc, monkeypatch)
    sent: list[tuple[int, int]] = []
    monkeypatch.setattr(os, "getpgid", lambda pid: pid + 1, raising=False)
    monkeypatch.setattr(os, "killpg", lambda pgid, sig: sent.append((pgid, sig)), raising=False)
    monkeypatch.setattr(signal, "SIGTERM", signal.SIGTERM, raising=False)
    monkeypatch.setattr(signal, "SIGKILL", getattr(signal, "SIGKILL", 9), raising=False)

    await backend.close()

    assert sent == [(4243, signal.SIGTERM), (4243, signal.SIGKILL)]
    assert calls == []  # the group path handled it; no per-process fallback


async def test_close_falls_back_to_process_when_group_is_gone(fake_proc, monkeypatch):
    backend = await _started(fake_proc)
    fake_proc.pid = 4242
    calls = _record_process_calls(fake_proc, monkeypatch)
    _force_timeouts(fake_proc, monkeypatch)

    def _gone(pid):
        raise ProcessLookupError

    monkeypatch.setattr(os, "getpgid", _gone, raising=False)
    monkeypatch.setattr(os, "killpg", lambda pgid, sig: None, raising=False)

    await backend.close()  # must not raise

    assert calls == ["terminate", "kill"]


async def test_close_is_idempotent(fake_proc):
    backend = await _started(fake_proc)
    await backend.close()
    await backend.close()  # second close: no process left, must not raise
    assert backend._proc is None


async def test_close_does_not_signal_a_process_that_already_exited(fake_proc, monkeypatch):
    backend = await _started(fake_proc)
    calls = _record_process_calls(fake_proc, monkeypatch)
    fake_proc.returncode = 0  # exited on its own; wait() returns immediately

    await backend.close()

    assert calls == []


async def _answer_nth(
    proc, method: str, n: int, result: dict | None = None, *, error: dict | None = None
) -> None:
    """Reply to the n-th client request of ``method`` (1-based), with a result or an error.

    Fails loudly if that request is never sent, instead of hanging the suite.
    """
    deadline = asyncio.get_running_loop().time() + 2.0
    while True:
        matching = [m for m in proc.stdin.lines if m.get("method") == method]
        if len(matching) >= n:
            reply = {"error": error} if error is not None else {"result": result or {}}
            proc.stdout.feed({"jsonrpc": "2.0", "id": matching[n - 1]["id"], **reply})
            return
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"the client never sent request #{n} of {method!r}")
        await asyncio.sleep(0.001)


async def test_reset_session_opens_a_new_session_and_the_next_turn_uses_it(fake_proc):
    backend = await _started(fake_proc)
    assert backend._session_id == "sess-1"

    answer = asyncio.create_task(_answer_nth(fake_proc, "session/new", 2, {"sessionId": "sess-2"}))
    await backend.reset_session()
    await answer
    assert backend._session_id == "sess-2"

    async def _drive_turn() -> None:
        prompt = await fake_proc.stdin.wait_for(lambda m: m.get("method") == "session/prompt")
        assert prompt["params"]["sessionId"] == "sess-2"  # the fresh session, not sess-1
        fake_proc.stdout.feed(
            {"jsonrpc": "2.0", "id": prompt["id"], "result": {"stopReason": "end_turn"}}
        )

    driver = asyncio.create_task(_drive_turn())
    events = [e async for e in backend.prompt("hello")]
    await driver
    assert isinstance(events[-1], TurnEnd)
    await backend.close()


async def test_reset_session_reapplies_the_model_on_engine_v2(fake_proc):
    backend = KiroBackend("fake-agent", engine="v2", model="m-1", permission=AllowAll())
    setup = asyncio.create_task(_drive_setup(fake_proc))
    first_model = asyncio.create_task(_answer_nth(fake_proc, "session/set_model", 1, {}))
    await backend.start()  # engine v2 with a model also sends session/set_model
    await setup
    await first_model

    answer = asyncio.create_task(_answer_nth(fake_proc, "session/new", 2, {"sessionId": "sess-2"}))
    reset = asyncio.create_task(backend.reset_session())
    await answer
    await _answer_nth(fake_proc, "session/set_model", 2, {})
    await reset

    models = [m for m in fake_proc.stdin.lines if m.get("method") == "session/set_model"]
    assert [m["params"]["sessionId"] for m in models] == ["sess-1", "sess-2"]
    assert all(m["params"]["modelId"] == "m-1" for m in models)
    await backend.close()


async def test_reset_session_before_start_is_an_error():
    backend = KiroBackend("fake-agent", engine="v2", permission=AllowAll())
    with pytest.raises(BackendError, match="start"):
        await backend.reset_session()


async def test_reset_session_during_a_turn_is_an_error_and_keeps_the_session(fake_proc):
    backend = await _started(fake_proc)
    consumer = asyncio.create_task(_collect(backend.prompt("hello")))
    prompt = await fake_proc.stdin.wait_for(lambda m: m.get("method") == "session/prompt")

    with pytest.raises(BackendError, match="in progress"):
        await backend.reset_session()
    assert backend._session_id == "sess-1"
    sent = [m for m in fake_proc.stdin.lines if m.get("method") == "session/new"]
    assert len(sent) == 1  # no second session/new was sent

    fake_proc.stdout.feed(
        {"jsonrpc": "2.0", "id": prompt["id"], "result": {"stopReason": "end_turn"}}
    )
    await consumer
    await backend.close()


async def _collect(aiter):
    return [e async for e in aiter]


async def _return_at(aiter, kind):
    """Read a turn only up to the first event of ``kind`` and walk away, like
    ``for ev in ...: if isinstance(ev, TurnEnd): return ev``. The generator is NOT drained."""
    async for ev in aiter:
        if isinstance(ev, kind):
            return ev
    return None


async def _soon(awaitable):
    """Bound an await so that a regression fails the test instead of hanging the suite."""
    return await asyncio.wait_for(awaitable, timeout=2)


def _methods(proc) -> list[str]:
    return [m["method"] for m in proc.stdin.lines if "method" in m]


async def test_stopping_at_turn_end_does_not_block_the_next_turn(fake_proc):
    """The turn is over at TurnEnd. A consumer that returns there, without draining the
    generator, must be able to start the next turn at once."""
    backend = await _started(fake_proc)
    for n in (1, 2):
        answer = asyncio.create_task(
            _answer_nth(fake_proc, "session/prompt", n, {"stopReason": "end_turn"})
        )
        end = await _return_at(backend.prompt(f"turn {n}"), TurnEnd)  # no await in between turns
        await answer
        assert isinstance(end, TurnEnd) and end.stop_reason == "end_turn"
    await backend.close()


async def test_reset_session_right_after_stopping_at_turn_end(fake_proc):
    backend = await _started(fake_proc)
    answer = asyncio.create_task(
        _answer_nth(fake_proc, "session/prompt", 1, {"stopReason": "end_turn"})
    )
    await _return_at(backend.prompt("hi"), TurnEnd)
    await answer

    new = asyncio.create_task(_answer_nth(fake_proc, "session/new", 2, {"sessionId": "sess-2"}))
    await backend.reset_session()  # was: BackendError("... a turn is already in progress")
    await new
    assert backend._session_id == "sess-2"
    await backend.close()


async def test_a_late_finalised_generator_cannot_disturb_a_later_turn(fake_proc):
    """Turn one's generator is abandoned at TurnEnd and finalised only AFTER turn two has
    started. Its cleanup must not take over turn two's event routing."""
    backend = await _started(fake_proc)
    first = backend.prompt("one")
    answer = asyncio.create_task(
        _answer_nth(fake_proc, "session/prompt", 1, {"stopReason": "end_turn"})
    )
    async for ev in first:
        if isinstance(ev, TurnEnd):
            break  # abandoned, still not finalised
    await answer

    second_events: list = []

    async def run_second() -> None:
        async for ev in backend.prompt("two"):
            second_events.append(ev)

    second = asyncio.create_task(run_second())
    prompt_two = await _soon(
        fake_proc.stdin.wait_for(
            lambda m: (
                m.get("method") == "session/prompt" and m["params"]["prompt"][0]["text"] == "two"
            )
        )
    )

    await first.aclose()  # the late finalisation, while turn two is in flight

    fake_proc.stdout.feed(
        {
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "update": {"sessionUpdate": "agent_message_chunk", "content": {"text": "hello"}}
            },
        }
    )
    fake_proc.stdout.feed(
        {"jsonrpc": "2.0", "id": prompt_two["id"], "result": {"stopReason": "end_turn"}}
    )
    await asyncio.wait_for(second, timeout=2)

    assert [e.text for e in second_events if isinstance(e, TextChunk)] == ["hello"]
    assert isinstance(second_events[-1], TurnEnd)
    await backend.close()


async def test_stopping_before_turn_end_still_leaves_the_turn_in_progress(fake_proc):
    """Only TurnEnd frees the backend: if the consumer walks away mid-turn the agent is
    still working, and starting another turn on the same session is refused."""
    backend = await _started(fake_proc)
    first = backend.prompt("one")
    consumer = asyncio.create_task(_return_at(first, TextChunk))
    await fake_proc.stdin.wait_for(lambda m: m.get("method") == "session/prompt")
    fake_proc.stdout.feed(
        {
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "update": {"sessionUpdate": "agent_message_chunk", "content": {"text": "partial"}}
            },
        }
    )
    assert isinstance(await consumer, TextChunk)  # walked away mid-turn

    with pytest.raises(BackendError, match="in progress"):
        async for _ in backend.prompt("two"):
            pass
    await first.aclose()
    await backend.close()


async def test_reset_session_keeps_the_previous_session_by_default(fake_proc):
    backend = await _started(fake_proc)
    answer = asyncio.create_task(_answer_nth(fake_proc, "session/new", 2, {"sessionId": "sess-2"}))
    await backend.reset_session()
    await answer
    assert "session/delete" not in _methods(fake_proc)
    await backend.close()


async def test_reset_session_delete_previous_deletes_after_the_new_one_exists(fake_proc):
    backend = await _started(fake_proc)
    new = asyncio.create_task(_answer_nth(fake_proc, "session/new", 2, {"sessionId": "sess-2"}))
    gone = asyncio.create_task(_answer_nth(fake_proc, "session/delete", 1, {}))
    await _soon(backend.reset_session(delete_previous=True))
    await new
    await gone

    assert backend._session_id == "sess-2"
    methods = _methods(fake_proc)
    assert methods.index("session/delete") > len(methods) - 1 - methods[::-1].index("session/new")
    deletes = [m for m in fake_proc.stdin.lines if m.get("method") == "session/delete"]
    assert [d["params"]["sessionId"] for d in deletes] == ["sess-1"]  # the OLD session only
    await backend.close()


async def test_delete_previous_comes_after_set_model_on_engine_v2(fake_proc):
    backend = KiroBackend("fake-agent", engine="v2", model="m-1", permission=AllowAll())
    setup = asyncio.create_task(_drive_setup(fake_proc))
    first_model = asyncio.create_task(_answer_nth(fake_proc, "session/set_model", 1, {}))
    await backend.start()
    await setup
    await first_model

    tasks = [
        asyncio.create_task(_answer_nth(fake_proc, "session/new", 2, {"sessionId": "sess-2"})),
        asyncio.create_task(_answer_nth(fake_proc, "session/set_model", 2, {})),
        asyncio.create_task(_answer_nth(fake_proc, "session/delete", 1, {})),
    ]
    await _soon(backend.reset_session(delete_previous=True))
    await asyncio.gather(*tasks)

    assert _methods(fake_proc)[-3:] == ["session/new", "session/set_model", "session/delete"]
    await backend.close()


async def test_delete_failure_propagates_but_the_new_session_is_already_active(fake_proc):
    backend = await _started(fake_proc)
    new = asyncio.create_task(_answer_nth(fake_proc, "session/new", 2, {"sessionId": "sess-2"}))
    refused = asyncio.create_task(
        _answer_nth(fake_proc, "session/delete", 1, error={"code": -32601, "message": "nope"})
    )
    with pytest.raises(BackendTransportError, match="RPC error"):
        await _soon(backend.reset_session(delete_previous=True))
    await new
    await refused
    assert backend._session_id == "sess-2"  # the reset itself had succeeded
    await backend.close()


async def test_failed_session_creation_keeps_the_old_session_and_deletes_nothing(fake_proc):
    backend = await _started(fake_proc)
    refused = asyncio.create_task(
        _answer_nth(fake_proc, "session/new", 2, error={"code": -32000, "message": "busy"})
    )
    with pytest.raises(BackendTransportError, match="RPC error"):
        await _soon(backend.reset_session(delete_previous=True))
    await refused
    assert backend._session_id == "sess-1"  # still usable
    assert "session/delete" not in _methods(fake_proc)  # the old session was not touched
    await backend.close()


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


def _cancels(proc) -> list[dict]:
    return [m for m in proc.stdin.lines if m.get("method") == "session/cancel"]


async def _walk_away_mid_turn(fake_proc, backend):
    gen = backend.prompt("one")
    consumer = asyncio.create_task(_return_at(gen, TextChunk))
    await _soon(fake_proc.stdin.wait_for(lambda m: m.get("method") == "session/prompt"))
    fake_proc.stdout.feed(
        {
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "update": {"sessionUpdate": "agent_message_chunk", "content": {"text": "partial"}}
            },
        }
    )
    assert isinstance(await _soon(consumer), TextChunk)
    return gen


async def test_closing_the_generator_before_turn_end_cancels_the_agent_turn(fake_proc):
    backend = await _started(fake_proc)
    gen = await _walk_away_mid_turn(fake_proc, backend)
    assert _cancels(fake_proc) == []  # merely walking away is not a close

    await _soon(gen.aclose())

    [cancel] = _cancels(fake_proc)
    assert cancel["params"] == {"sessionId": backend._session_id}
    assert "id" not in cancel  # a notification: no response is awaited
    await backend.close()


async def test_the_backend_is_free_again_after_the_cancelling_close(fake_proc):
    backend = await _started(fake_proc)
    gen = await _walk_away_mid_turn(fake_proc, backend)
    await _soon(gen.aclose())

    answer = asyncio.create_task(
        _answer_nth(fake_proc, "session/prompt", 2, {"stopReason": "end_turn"})
    )
    end = await _soon(_return_at(backend.prompt("two"), TurnEnd))
    await answer
    assert isinstance(end, TurnEnd)
    await backend.close()


async def test_no_cancel_after_a_normal_turn_end(fake_proc):
    backend = await _started(fake_proc)
    answer = asyncio.create_task(
        _answer_nth(fake_proc, "session/prompt", 1, {"stopReason": "end_turn"})
    )
    gen = backend.prompt("one")
    await _soon(_return_at(gen, TurnEnd))
    await answer
    await gen.aclose()  # finalised after TurnEnd
    assert _cancels(fake_proc) == []
    await backend.close()


async def test_a_late_finalised_generator_does_not_cancel_a_later_turn(fake_proc):
    backend = await _started(fake_proc)
    first = backend.prompt("one")
    answer = asyncio.create_task(
        _answer_nth(fake_proc, "session/prompt", 1, {"stopReason": "end_turn"})
    )
    await _soon(_return_at(first, TurnEnd))
    await answer

    second = asyncio.create_task(_collect(backend.prompt("two")))
    prompt_two = await _soon(
        fake_proc.stdin.wait_for(
            lambda m: (
                m.get("method") == "session/prompt" and m["params"]["prompt"][0]["text"] == "two"
            )
        )
    )
    await first.aclose()  # turn two is in flight; turn one's cleanup must not touch it
    assert _cancels(fake_proc) == []

    fake_proc.stdout.feed(
        {"jsonrpc": "2.0", "id": prompt_two["id"], "result": {"stopReason": "end_turn"}}
    )
    await _soon(second)
    await backend.close()


async def test_cancel_is_best_effort_when_the_process_is_gone(fake_proc):
    backend = await _started(fake_proc)
    gen = await _walk_away_mid_turn(fake_proc, backend)
    backend._proc = None  # transport gone: the write would raise
    await _soon(gen.aclose())  # must not raise
    assert _cancels(fake_proc) == []


async def test_a_raised_error_inside_the_consumer_also_cancels(fake_proc):
    backend = await _started(fake_proc)

    async def consume() -> None:
        async for ev in backend.prompt("one"):
            if isinstance(ev, TextChunk):
                raise RuntimeError("consumer blew up")

    task = asyncio.create_task(consume())
    await _soon(fake_proc.stdin.wait_for(lambda m: m.get("method") == "session/prompt"))
    fake_proc.stdout.feed(
        {
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {"update": {"sessionUpdate": "agent_message_chunk", "content": {"text": "x"}}},
        }
    )
    with pytest.raises(RuntimeError):
        await _soon(task)
    await asyncio.sleep(0)  # let the async generator's finalizer run
    assert len(_cancels(fake_proc)) == 1
    await backend.close()
