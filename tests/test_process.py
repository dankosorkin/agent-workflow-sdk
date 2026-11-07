"""ManagedProcess: spawn, stop ladder, tree kill, Windows Job Object, live registry."""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys

import pytest
from agentflow import _winjob, process
from agentflow.process import ManagedProcess, kill_all, live_processes


class _Stdin:
    def __init__(self) -> None:
        self.closed = False

    def is_closing(self) -> bool:
        return self.closed

    def close(self) -> None:
        self.closed = True


class _Proc:
    """Stands in for asyncio.subprocess.Process; exits only when told to."""

    def __init__(self, *, exits_on: str | None = None) -> None:
        self.pid = 4242
        self.stdin = _Stdin()
        self.stdout = self.stderr = None
        self.returncode: int | None = None
        self.exits_on = exits_on  # "stdin" | "terminate" | "kill" | None
        self.calls: list[str] = []

    def terminate(self) -> None:
        self.calls.append("terminate")
        if self.exits_on == "terminate":
            self.returncode = -15

    def kill(self) -> None:
        self.calls.append("kill")
        if self.exits_on in ("terminate", "kill"):
            self.returncode = -9

    async def wait(self) -> int:
        while self.returncode is None:
            if self.exits_on == "stdin" and self.stdin.closed:
                self.returncode = 0
            await asyncio.sleep(0.001)
        return self.returncode


@pytest.fixture(autouse=True)
def _clean_registry():
    process._live.clear()
    yield
    process._live.clear()


@pytest.fixture
def no_posix(monkeypatch):
    monkeypatch.delattr(os, "killpg", raising=False)
    monkeypatch.delattr(os, "getpgid", raising=False)
    monkeypatch.delattr(signal, "SIGKILL", raising=False)


async def test_spawn_starts_a_new_session_and_registers(monkeypatch):
    seen: dict = {}

    async def _exec(*args, **kwargs):
        seen.update(args=args, **kwargs)
        return _Proc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _exec)

    proc = await ManagedProcess.spawn(["tool", "--x"], cwd="/work", env={"A": "1"})

    assert seen["args"] == ("tool", "--x")
    assert seen["start_new_session"] is True
    assert seen["cwd"] == "/work" and seen["env"] == {"A": "1"}
    assert seen["limit"] == 8 * 1024 * 1024
    assert proc.pid == 4242 and live_processes() == [proc]


async def test_spawn_failure_registers_nothing(monkeypatch):
    async def _boom(*a, **k):
        raise FileNotFoundError("nope")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _boom)
    with pytest.raises(OSError):
        await ManagedProcess.spawn(["missing"], cwd=".")
    assert live_processes() == []


async def test_stop_ladder_stops_at_the_first_step_that_works(no_posix):
    clean = _Proc(exits_on="stdin")
    await ManagedProcess(clean).stop(grace=0.05)
    assert clean.calls == []  # stdin close was enough

    polite = _Proc(exits_on="terminate")
    await ManagedProcess(polite).stop(grace=0.02)
    assert polite.calls == ["terminate"]

    stubborn = _Proc(exits_on="kill")
    await ManagedProcess(stubborn).stop(grace=0.02)
    assert stubborn.calls == ["terminate", "kill"]


async def test_stop_releases_even_when_nothing_works(no_posix):
    proc = _Proc(exits_on=None)
    managed = ManagedProcess(proc)
    await managed.stop(grace=0.01)
    assert live_processes() == []


def test_posix_signals_go_to_the_process_group(monkeypatch):
    sent: list[tuple[int, int]] = []
    monkeypatch.setattr(os, "getpgid", lambda pid: pid + 1, raising=False)
    monkeypatch.setattr(os, "killpg", lambda g, s: sent.append((g, s)), raising=False)
    monkeypatch.setattr(signal, "SIGKILL", 9, raising=False)
    proc = _Proc()
    managed = ManagedProcess(proc)

    managed.terminate()
    managed.kill()

    assert sent == [(4243, signal.SIGTERM), (4243, 9)]
    assert proc.calls == []  # the group path handled it


def test_missing_group_falls_back_to_the_process(monkeypatch):
    def _gone(pid):
        raise ProcessLookupError

    monkeypatch.setattr(os, "getpgid", _gone, raising=False)
    monkeypatch.setattr(os, "killpg", lambda g, s: None, raising=False)
    proc = _Proc()
    managed = ManagedProcess(proc)
    managed.terminate()
    managed.kill()
    assert proc.calls == ["terminate", "kill"]


@pytest.fixture
def windows(monkeypatch, no_posix):
    monkeypatch.setattr(sys, "platform", "win32")
    events: list[str] = []
    monkeypatch.setattr(subprocess, "run", lambda cmd, **kw: events.append(" ".join(cmd[:3])))
    return events


async def test_windows_spawn_puts_the_child_in_a_job(monkeypatch, windows):
    attached: list[int] = []
    monkeypatch.setattr(_winjob, "attach", lambda pid: attached.append(pid) or "JOB")

    async def _exec(*a, **k):
        return _Proc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _exec)
    proc = await ManagedProcess.spawn(["tool"], cwd=".")

    assert attached == [4242] and proc._job == "JOB"


def test_windows_kill_terminates_the_job_not_taskkill(monkeypatch, windows):
    monkeypatch.setattr(_winjob, "terminate", lambda job: windows.append("job") or True)
    proc = _Proc()
    ManagedProcess(proc, job="JOB").kill()
    assert windows == ["job"]
    assert proc.calls == ["kill"]  # the harmless repeat


def test_windows_kill_falls_back_to_taskkill(monkeypatch, windows):
    monkeypatch.setattr(_winjob, "terminate", lambda job: windows.append("job") or False)
    ManagedProcess(_Proc(), job="JOB").kill()
    assert windows == ["job", "taskkill /F /T"]


def test_windows_kill_without_a_job_uses_taskkill(windows):
    ManagedProcess(_Proc()).kill()
    assert windows == ["taskkill /F /T"]


def test_windows_soft_terminate_never_runs_taskkill(windows):
    proc = _Proc()
    ManagedProcess(proc, job="JOB").terminate()
    assert windows == [] and proc.calls == ["terminate"]


def test_release_closes_the_job_once(monkeypatch):
    closed: list[str] = []
    monkeypatch.setattr(_winjob, "close", lambda job: closed.append(job))
    managed = ManagedProcess(_Proc(), job="JOB")
    managed.release()
    managed.release()
    assert closed == ["JOB"] and live_processes() == []


def test_kill_all_stops_and_releases_everything(no_posix):
    a, b = _Proc(), _Proc()
    ManagedProcess(a)
    ManagedProcess(b)

    kill_all()

    assert a.calls == ["kill"] and b.calls == ["kill"]
    assert live_processes() == []


def test_kill_all_survives_a_process_that_raises(no_posix):
    class _Bad(_Proc):
        def kill(self) -> None:
            raise RuntimeError("boom")

    good = _Proc()
    ManagedProcess(_Bad())
    ManagedProcess(good)
    kill_all()  # must not raise
    assert good.calls == ["kill"]


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX process groups")
async def test_real_tree_is_reaped_including_grandchildren(tmp_path):
    """A child that spawns its own background child: killing the managed process
    must end the grandchild too (the kiro-cli / kiro-cli-chat shape)."""
    pidfile = tmp_path / "grandchild.pid"
    script = f"sleep 60 & echo $! > {pidfile}; wait"
    proc = await ManagedProcess.spawn(["sh", "-c", script], cwd=tmp_path)
    for _ in range(200):
        if pidfile.exists() and pidfile.read_text().strip():
            break
        await asyncio.sleep(0.01)
    grandchild = int(pidfile.read_text())
    os.kill(grandchild, 0)  # alive

    await proc.stop(grace=0.05)
    await asyncio.sleep(0.2)

    with pytest.raises(ProcessLookupError):
        os.kill(grandchild, 0)
    assert live_processes() == []
