"""PersistentClaudeBackend and the generic BackendPool, against a scripted fake ``claude``."""

from __future__ import annotations

import asyncio
import json

import pytest
from agentflow.backends.base import AllowAll, DenyAll, ToolAllowlist
from agentflow.backends.claude_code import PersistentClaudeBackend
from agentflow.backends.pool import BackendPool, BackendSpec
from agentflow.errors import BackendError, BackendTransportError
from agentflow.events import TextChunk, TurnEnd


class _Stream:
    def __init__(self) -> None:
        self.q: asyncio.Queue[bytes] = asyncio.Queue()

    async def readline(self) -> bytes:
        return await self.q.get()


class _Stdin:
    def __init__(self, proc: _Claude) -> None:
        self.proc = proc
        self.closed = False

    def is_closing(self) -> bool:
        return self.closed

    def close(self) -> None:
        self.closed = True
        self.proc.stdout.q.put_nowait(b"")  # EOF

    async def drain(self) -> None:
        return None

    def write(self, data: bytes) -> None:
        for raw in data.decode().splitlines():
            if raw.strip():
                self.proc.handle(json.loads(raw))


class _Claude:
    """A fake ``claude -p --input-format stream-json``: remembers the conversation."""

    def __init__(self, cmd: list[str]) -> None:
        self.cmd = cmd
        self.stdout = _Stream()
        self.stderr = _Stream()
        self.stdin = _Stdin(self)
        self.pid = 1
        self.returncode: int | None = None
        self.sessions = 1
        self.history: list[str] = []
        self.received: list[str] = []
        self.hang_after_first_event = False

    def _emit(self, obj: dict) -> None:
        self.stdout.q.put_nowait((json.dumps(obj) + "\n").encode())

    def handle(self, msg: dict) -> None:
        text = msg["message"]["content"]
        self.received.append(text)
        if text == "/clear":
            self.history.clear()
            self.sessions += 1
            self._emit({"type": "conversation_reset", "session_id": f"s{self.sessions}"})
            self._emit({"type": "result", "result": "", "session_id": f"s{self.sessions}"})
            return
        self.history.append(text)
        sid = f"s{self.sessions}"
        self._emit({"type": "system", "subtype": "init", "session_id": sid})
        self._emit(
            {
                "type": "stream_event",
                "event": {
                    "type": "content_block_delta",
                    "delta": {"type": "text_delta", "text": f"seen {len(self.history)}"},
                },
            }
        )
        if self.hang_after_first_event:
            return
        self._emit(
            {
                "type": "result",
                "result": f"seen {len(self.history)}",
                "session_id": sid,
                "stop_reason": "end_turn",
            }
        )

    def terminate(self) -> None:
        self.returncode = -15
        self.stdout.q.put_nowait(b"")

    def kill(self) -> None:
        self.returncode = -9
        self.stdout.q.put_nowait(b"")

    async def wait(self) -> int:
        while self.returncode is None:
            if self.stdin.closed:
                self.returncode = 0
            await asyncio.sleep(0.001)
        return self.returncode


@pytest.fixture
def claudes(monkeypatch) -> list[_Claude]:
    spawned: list[_Claude] = []

    async def _exec(*cmd, **kwargs):
        proc = _Claude(list(cmd))
        spawned.append(proc)
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _exec)
    return spawned


async def _ask(backend, text: str) -> str:
    out = ""
    async for ev in backend.prompt(text):
        if isinstance(ev, TurnEnd):
            out = ev.text
    return out


async def test_one_process_serves_many_turns_and_keeps_the_conversation(claudes):
    backend = PersistentClaudeBackend(permission=DenyAll())
    await backend.start()

    assert await _ask(backend, "a") == "seen 1"
    assert await _ask(backend, "b") == "seen 2"  # same process, memory kept

    assert len(claudes) == 1
    cmd = claudes[0].cmd
    assert cmd[:2] == ["claude", "-p"]
    assert cmd[cmd.index("--input-format") + 1] == "stream-json"
    assert "--resume" not in cmd
    await backend.close()


async def test_text_is_streamed_as_events(claudes):
    backend = PersistentClaudeBackend(permission=DenyAll())
    await backend.start()
    events = [e async for e in backend.prompt("a")]
    assert [type(e).__name__ for e in events] == ["TextChunk", "TurnEnd"]
    assert events[0] == TextChunk("seen 1")
    await backend.close()


async def test_reset_session_clears_in_place(claudes):
    backend = PersistentClaudeBackend(permission=DenyAll())
    await backend.start()
    await _ask(backend, "a")
    await backend.reset_session()

    assert claudes[0].received == ["a", "/clear"]
    assert await _ask(backend, "b") == "seen 1"  # the conversation started over
    assert len(claudes) == 1
    await backend.close()


async def test_reset_session_before_any_turn_sends_nothing(claudes):
    backend = PersistentClaudeBackend(permission=DenyAll())
    await backend.start()
    await backend.reset_session()
    assert claudes[0].received == []
    await backend.close()


async def test_flags_come_from_agent_model_and_permission(claudes):
    backend = PersistentClaudeBackend(
        agent="rev", model="sonnet", permission=ToolAllowlist({"Read"})
    )
    await backend.start()
    cmd = claudes[0].cmd
    assert cmd[cmd.index("--agent") + 1] == "rev"
    assert cmd[cmd.index("--model") + 1] == "sonnet"
    assert cmd[cmd.index("--allowed-tools") + 1] == "Read"
    await backend.close()


async def test_a_turn_abandoned_midway_stops_the_process(claudes):
    backend = PersistentClaudeBackend(permission=DenyAll())
    await backend.start()
    claudes[0].hang_after_first_event = True
    stream = backend.prompt("a")
    first = await stream.__anext__()  # a TextChunk; the result never comes
    assert isinstance(first, TextChunk)
    await stream.aclose()

    assert backend.reusable is False
    assert claudes[0].returncode == -9
    with pytest.raises(BackendError, match="not running"):
        await _ask(backend, "b")


async def test_process_exit_mid_turn_is_a_transport_error(claudes):
    backend = PersistentClaudeBackend(permission=DenyAll())
    await backend.start()
    claudes[0].hang_after_first_event = True
    task = asyncio.create_task(_ask(backend, "a"))
    await asyncio.sleep(0.02)
    claudes[0].stdout.q.put_nowait(b"")  # the process died
    with pytest.raises(BackendTransportError, match="exited mid-turn"):
        await task
    assert backend.reusable is False


async def test_session_resume_is_refused(claudes):
    backend = PersistentClaudeBackend(permission=DenyAll())
    await backend.start()
    with pytest.raises(BackendError, match="cannot resume"):
        async for _ in backend.prompt("a", session="old"):
            pass
    await backend.close()


async def test_only_one_turn_at_a_time(claudes):
    backend = PersistentClaudeBackend(permission=DenyAll())
    await backend.start()
    claudes[0].hang_after_first_event = True
    stream = backend.prompt("a")
    await stream.__anext__()
    with pytest.raises(BackendError, match="already in progress"):
        async for _ in backend.prompt("b"):
            pass
    await stream.aclose()


# ---- the generic pool ---------------------------------------------------------------


def _spec(kind="claude_code", agent=None, permission=None, cwd="."):
    return BackendSpec(kind, cwd, permission or DenyAll(), agent=agent)


async def test_pool_reuses_a_persistent_claude_across_steps(claudes):
    async with BackendPool() as pool:
        for text, expected in [("a", "seen 1"), ("b", "seen 1")]:  # /clear in between
            async with pool.lease(_spec()) as be:
                assert await _ask(be, text) == expected
        assert len(claudes) == 1
    assert claudes[0].returncode is not None  # closed with the pool


async def test_pool_keys_claude_by_agent_and_permissions(claudes):
    async with BackendPool() as pool:
        for spec in [
            _spec(agent="a"),
            _spec(agent="b"),  # another agent: another process (a flag)
            _spec(agent="a", permission=AllowAll()),  # other permissions: another process
            _spec(agent="a"),  # back to the first: reused
        ]:
            async with pool.lease(spec):
                pass
        assert len(claudes) == 3


async def test_pool_treats_equal_allowlists_as_the_same_key(claudes):
    async with BackendPool() as pool:
        for tools in [{"Read", "Grep"}, {"Grep", "Read"}]:
            async with pool.lease(_spec(permission=ToolAllowlist(tools))):
                pass
        assert len(claudes) == 1


async def test_pool_drops_a_claude_whose_turn_was_abandoned(claudes):
    async with BackendPool() as pool:
        async with pool.lease(_spec()) as be:
            claudes[0].hang_after_first_event = True
            stream = be.prompt("a")
            await stream.__anext__()
            await stream.aclose()
        async with pool.lease(_spec()):
            pass
        assert len(claudes) == 2  # the broken one was not handed out again


async def test_pool_never_keeps_codex(monkeypatch):
    created: list[object] = []

    class _Codex:
        reusable = False

        def __init__(self, **kw) -> None:
            created.append(self)
            self.closed = False

        async def start(self) -> None: ...

        async def close(self) -> None:
            self.closed = True

    monkeypatch.setattr("agentflow.backends.codex.CodexBackend", _Codex)
    async with BackendPool() as pool:
        for _ in range(2):
            async with pool.lease(_spec("codex")):
                pass
    assert len(created) == 2 and all(c.closed for c in created)


async def test_pool_refuses_an_unknown_kind():
    async with BackendPool() as pool:
        assert pool.supports("claude_code") and not pool.supports("ollama")
        with pytest.raises(BackendError, match="no driver"):
            async with pool.lease(_spec("ollama")):
                pass


async def test_workflow_runner_shares_one_claude_process_inside_agent_pool(claudes):
    from agentflow.workflow.runner import agent_pool, backend_runner

    run = backend_runner(cwd=".")
    step = {"id": "s", "permission": "read_only"}
    entry = {"backend": "claude_code", "agent": None}
    async with agent_pool():
        assert await run(step, entry, "one") == "seen 1"
        assert await run(step, entry, "two") == "seen 1"  # cleared between steps
        assert len(claudes) == 1
    assert claudes[0].returncode is not None
