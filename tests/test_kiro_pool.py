"""KiroBackend.use_agent and KiroPool, against a scripted fake ``kiro-cli acp`` process."""

from __future__ import annotations

import asyncio
import json

import pytest
from agentflow.backends.base import AllowAll, DenyAll
from agentflow.backends.kiro import KiroBackend
from agentflow.backends.pool import KiroPool
from agentflow.errors import BackendError

MODES = ["alpha", "beta", "gamma"]


class _Stdout:
    def __init__(self) -> None:
        self.q: asyncio.Queue[bytes] = asyncio.Queue()

    async def readline(self) -> bytes:
        return await self.q.get()


class _Stdin:
    """Answers every request the client writes, like a well-behaved ACP agent."""

    def __init__(self, proc: _Proc) -> None:
        self.proc = proc
        self.closed = False

    def is_closing(self) -> bool:
        return self.closed

    def close(self) -> None:
        self.closed = True

    async def drain(self) -> None:
        return None

    def write(self, data: bytes) -> None:
        for raw in data.decode().splitlines():
            if raw.strip():
                self.proc.handle(json.loads(raw))


class _Proc:
    def __init__(self) -> None:
        self.stdout = _Stdout()
        self.stdin = _Stdin(self)
        self.returncode: int | None = None
        self.requests: list[dict] = []
        self.sessions = 0
        self.mode: str | None = None
        self.crash_on_prompt = False

    def _reply(self, msg: dict, result: dict) -> None:
        line = json.dumps({"jsonrpc": "2.0", "id": msg["id"], "result": result}) + "\n"
        self.stdout.q.put_nowait(line.encode())

    def handle(self, msg: dict) -> None:
        self.requests.append(msg)
        method = msg.get("method")
        if method == "initialize":
            self._reply(msg, {"agentInfo": {"name": "fake"}})
        elif method == "session/new":
            self.sessions += 1
            self.mode = "default"
            modes = {"currentModeId": "default", "availableModes": [{"id": m} for m in MODES]}
            self._reply(msg, {"sessionId": f"s{self.sessions}", "modes": modes})
        elif method == "session/set_mode":
            self.mode = msg["params"]["modeId"]
            self._reply(msg, {})
        elif method == "session/prompt":
            if self.crash_on_prompt:  # the process dies without answering
                self.returncode = -9
                self.stdout.q.put_nowait(b"")
                return
            self._reply(msg, {"stopReason": "end_turn"})

    def terminate(self) -> None:
        self.returncode = -15

    def kill(self) -> None:
        self.returncode = -9

    async def wait(self) -> int:
        if self.returncode is None:
            self.returncode = 0  # a clean exit once stdin is closed
        return self.returncode


@pytest.fixture
def procs(monkeypatch) -> list[_Proc]:
    spawned: list[_Proc] = []

    async def _exec(*args, **kwargs):
        proc = _Proc()
        spawned.append(proc)
        return proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _exec)
    return spawned


async def test_use_agent_switches_mode_on_the_same_process(procs):
    backend = KiroBackend("alpha", engine="v3", permission=AllowAll())
    await backend.start()
    await backend.use_agent("beta")

    assert len(procs) == 1
    assert backend.agent == "beta"
    assert procs[0].sessions == 2  # a fresh session, not the old one
    assert procs[0].mode == "beta"
    await backend.close()


async def test_use_agent_unknown_agent_keeps_the_previous_name(procs):
    backend = KiroBackend("alpha", engine="v3", permission=AllowAll())
    await backend.start()
    with pytest.raises(BackendError, match="not in available modes"):
        await backend.use_agent("nope")
    assert backend.agent == "alpha"
    await backend.close()


async def test_use_agent_needs_engine_v3(procs):
    backend = KiroBackend("alpha", engine="v2", permission=AllowAll())
    with pytest.raises(BackendError, match="engine v3"):
        await backend.use_agent("beta")


async def test_pool_reuses_one_process_across_agents(procs):
    async with KiroPool() as pool:
        for agent in ["alpha", "beta", "gamma", "alpha"]:
            async with pool.lease(agent, cwd=".", permission=AllowAll()) as be:
                assert be.agent == agent
        assert len(procs) == 1
        assert procs[0].sessions == 4  # every lease got its own fresh session
    assert procs[0].returncode is not None  # closed with the pool


async def test_pool_gives_each_lease_its_own_permission(procs):
    deny, allow = DenyAll(), AllowAll()
    async with KiroPool() as pool:
        async with pool.lease("alpha", cwd=".", permission=deny) as be:
            assert be.permission is deny
        async with pool.lease("alpha", cwd=".", permission=allow) as be:
            assert be.permission is allow


async def test_pool_runs_concurrent_leases_on_separate_processes(procs):
    async with KiroPool(max_size=2) as pool:
        async with (
            pool.lease("alpha", cwd=".", permission=AllowAll()) as a,
            pool.lease("beta", cwd=".", permission=AllowAll()) as b,
        ):
            assert a is not b
        assert len(procs) == 2


async def test_pool_waits_when_full(procs):
    order: list[str] = []
    async with KiroPool(max_size=1) as pool:

        async def worker(name: str, hold: float) -> None:
            async with pool.lease(name, cwd=".", permission=AllowAll()):
                order.append(f"in {name}")
                await asyncio.sleep(hold)
                order.append(f"out {name}")

        await asyncio.gather(worker("alpha", 0.05), worker("beta", 0))
    assert order == ["in alpha", "out alpha", "in beta", "out beta"]
    assert len(procs) == 1


async def test_pool_discards_a_backend_whose_lease_failed(procs):
    async with KiroPool() as pool:
        with pytest.raises(RuntimeError):
            async with pool.lease("alpha", cwd=".", permission=AllowAll()):
                raise RuntimeError("step failed")
        assert procs[0].returncode is not None  # closed, not returned to the pool
        async with pool.lease("beta", cwd=".", permission=AllowAll()):
            pass
        assert len(procs) == 2


async def test_pool_keeps_directories_apart(procs, tmp_path):
    async with KiroPool() as pool:
        async with pool.lease("alpha", cwd=tmp_path / "a", permission=AllowAll()):
            pass
        async with pool.lease("alpha", cwd=tmp_path / "b", permission=AllowAll()):
            pass
        assert len(procs) == 2


async def test_pool_v2_never_reuses(procs):
    async with KiroPool(engine="v2") as pool:
        for _ in range(2):
            async with pool.lease("alpha", cwd=".", permission=AllowAll()):
                pass
        assert len(procs) == 2
        assert all(p.returncode is not None for p in procs)


async def test_closed_pool_refuses_leases(procs):
    pool = KiroPool()
    await pool.aclose()
    with pytest.raises(BackendError, match="closed"):
        async with pool.lease("alpha", cwd=".", permission=AllowAll()):
            pass


async def test_backend_runner_uses_the_pool_inside_agent_pool(procs):
    from agentflow.workflow.runner import agent_pool, backend_runner

    run = backend_runner(cwd=".")
    step = {"id": "s", "permission": "read_only"}
    async with agent_pool():
        await run(step, {"backend": "kiro", "agent": "alpha"}, "hi")
        await run(step, {"backend": "kiro", "agent": "beta"}, "hi")
        assert len(procs) == 1
    assert procs[0].returncode is not None  # closed when the run's block ended


async def test_backend_runner_without_a_pool_starts_one_process_per_step(procs):
    from agentflow.workflow.runner import backend_runner

    run = backend_runner(cwd=".")
    step = {"id": "s", "permission": "read_only"}
    await run(step, {"backend": "kiro", "agent": "alpha"}, "hi")
    await run(step, {"backend": "kiro", "agent": "beta"}, "hi")
    assert len(procs) == 2


async def test_a_crashed_shared_process_fails_only_that_step(procs):
    from agentflow.workflow.runner import agent_pool, backend_runner

    run = backend_runner(cwd=".")
    step = {"id": "s", "permission": "read_only"}
    async with agent_pool():
        await run(step, {"backend": "kiro", "agent": "alpha"}, "hi")
        procs[0].crash_on_prompt = True
        with pytest.raises(BackendError):
            await run(step, {"backend": "kiro", "agent": "beta"}, "hi")
        # The dead process is not handed out again: the next step gets a new one and works.
        await run(step, {"backend": "kiro", "agent": "gamma"}, "hi")
        assert len(procs) == 2
        assert procs[1].returncode is None


async def test_agent_pool_can_be_turned_off(procs, monkeypatch):
    from agentflow.workflow.runner import agent_pool, backend_runner

    run = backend_runner(cwd=".")
    step = {"id": "s", "permission": "read_only"}
    async with agent_pool(enabled=False) as pool:
        assert pool is None
        await run(step, {"backend": "kiro", "agent": "alpha"}, "hi")
        await run(step, {"backend": "kiro", "agent": "beta"}, "hi")
    assert len(procs) == 2

    monkeypatch.setenv("AGENTFLOW_NO_POOL", "1")
    async with agent_pool() as pool:
        assert pool is None
    monkeypatch.setenv("AGENTFLOW_NO_POOL", "0")
    async with agent_pool() as pool:
        assert pool is not None
