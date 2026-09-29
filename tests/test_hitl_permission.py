"""Human-in-the-loop permission, end-to-end against the real KiroBackend
permission machinery driven by a scripted fake subprocess.

- AllowAll / DenyAll resolve automatically in the node's frame.
- Interactive suspends the run via a genuine engine interrupt; resume(value)
  supplies the human's Allow/Deny.
"""

from __future__ import annotations

import asyncio
import json
from typing import Annotated

import pytest

from agentflow import END, START, Graph, MemoryCheckpointer, State, last
from agentflow.backends.base import AllowAll, DenyAll, Interactive
from agentflow.backends.kiro import KiroBackend
from agentflow.events import Allow, Deny, PermissionRequest, TurnEnd


class _Stdin:
    def __init__(self):
        self.lines = []
        self._event = asyncio.Event()
        self.closed = False

    def write(self, data: bytes):
        for raw in data.decode().splitlines():
            if raw.strip():
                self.lines.append(json.loads(raw))
        self._event.set()

    def is_closing(self):
        return self.closed

    def close(self):
        self.closed = True

    async def wait_for(self, predicate):
        seen = 0
        while True:
            while seen < len(self.lines):
                m = self.lines[seen]
                seen += 1
                if predicate(m):
                    return m
            self._event.clear()
            await self._event.wait()


class _Stdout:
    def __init__(self):
        self._q: asyncio.Queue[bytes] = asyncio.Queue()

    def feed(self, msg: dict):
        self._q.put_nowait((json.dumps(msg) + "\n").encode())

    async def readline(self) -> bytes:
        return await self._q.get()


class _Proc:
    def __init__(self):
        self.stdin = _Stdin()
        self.stdout = _Stdout()
        self.returncode = None

    def terminate(self): self.returncode = -15
    def kill(self): self.returncode = -9
    async def wait(self): return self.returncode or 0


class GState(State):
    stop: Annotated[str, last]


async def _start(proc, policy) -> KiroBackend:
    backend = KiroBackend("a", engine="v2", permission=policy)

    async def setup():
        i = await proc.stdin.wait_for(lambda m: m.get("method") == "initialize")
        proc.stdout.feed({"jsonrpc": "2.0", "id": i["id"], "result": {}})
        n = await proc.stdin.wait_for(lambda m: m.get("method") == "session/new")
        proc.stdout.feed({"jsonrpc": "2.0", "id": n["id"], "result": {"sessionId": "s"}})

    task = asyncio.create_task(setup())
    await backend.start()
    await task
    return backend


def _spawn_permission_driver(proc, turns: int = 1):
    """Drive ``turns`` prompt turns. Each turn: on session/prompt, send a
    permission request with a unique id; once the client answers it (or after a
    short wait if it never does, e.g. the turn was abandoned by an interrupt),
    finish that prompt with the outcome encoded in the stop reason.

    A HITL run needs two turns: the pre-interrupt turn (abandoned, permission
    left unanswered) and the post-resume re-run (answered), because the engine
    re-runs the node on resume.
    """
    async def run():
        for i in range(turns):
            perm_id = 7001 + i
            p = await proc.stdin.wait_for(lambda m: m.get("method") == "session/prompt"
                                          and m.get("_seen") is not True)
            p["_seen"] = True
            proc.stdout.feed({
                "jsonrpc": "2.0", "id": perm_id, "method": "session/request_permission",
                "params": {"toolName": "fs_write",
                           "options": [{"optionId": "ok", "name": "o", "kind": "allow_once"}]},
            })
            try:
                ans = await asyncio.wait_for(
                    proc.stdin.wait_for(lambda m: m.get("id") == perm_id and "result" in m), 1)
                outcome = ans["result"]["outcome"]["outcome"]
            except asyncio.TimeoutError:
                outcome = "unanswered"
            proc.stdout.feed({"jsonrpc": "2.0", "id": p["id"],
                              "result": {"stopReason": f"done:{outcome}"}})
    return asyncio.create_task(run())


def _graph(backend, checkpointer=None):
    async def act(state, ctx):
        stop = None
        async for ev in backend.prompt("write a file"):
            if isinstance(ev, TurnEnd):
                stop = ev.stop_reason
        return {"stop": stop or ""}

    g = Graph(GState)
    g.add_node("act", act)
    g.add_edge(START, "act")
    g.add_edge("act", END)
    return g.compile(checkpointer=checkpointer)


@pytest.fixture
def fake_proc(monkeypatch):
    proc = _Proc()

    async def fake_exec(*a, **k):
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    return proc


async def test_allow_all_auto_approves(fake_proc):
    backend = await _start(fake_proc, AllowAll())
    driver = _spawn_permission_driver(fake_proc)
    out = await _graph(backend).invoke({})
    await driver
    await backend.close()
    assert out["stop"] == "done:selected"


async def test_deny_all_cancels(fake_proc):
    backend = await _start(fake_proc, DenyAll())
    driver = _spawn_permission_driver(fake_proc)
    out = await _graph(backend).invoke({})
    await driver
    await backend.close()
    assert out["stop"] == "done:cancelled"


async def test_interactive_suspends_then_resume_allows(fake_proc):
    backend = await _start(fake_proc, Interactive())
    driver = _spawn_permission_driver(fake_proc, turns=2)
    app = _graph(backend, checkpointer=MemoryCheckpointer())

    await app.invoke({}, thread="hitl")
    state = await app.get_state("hitl")
    assert state.interrupted
    assert isinstance(state.interrupt_payload, PermissionRequest)
    assert state.interrupt_payload.tool == "fs_write"

    out = await app.resume("hitl", value=Allow())
    await driver
    await backend.close()
    assert out["stop"] == "done:selected"


async def test_interactive_resume_deny(fake_proc):
    backend = await _start(fake_proc, Interactive())
    driver = _spawn_permission_driver(fake_proc, turns=2)
    app = _graph(backend, checkpointer=MemoryCheckpointer())

    await app.invoke({}, thread="h2")
    out = await app.resume("h2", value=Deny())
    await driver
    await backend.close()
    assert out["stop"] == "done:cancelled"
