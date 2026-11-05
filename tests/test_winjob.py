"""Job Object wrapper, against a fake kernel32 (the real one exists only on Windows)."""

from __future__ import annotations

import sys

from agentflow.backends import _winjob


class _K32:
    def __init__(self, *, fail: str | None = None) -> None:
        self.fail = fail
        self.calls: list[str] = []

    def CreateJobObjectW(self, *a):  # noqa: N802
        self.calls.append("create")
        return None if self.fail == "create" else 11

    def SetInformationJobObject(self, job, cls, info, size):  # noqa: N802
        self.calls.append(f"set:{cls}:{info._obj.BasicLimitInformation.LimitFlags:#x}")
        return 0 if self.fail == "set" else 1

    def OpenProcess(self, access, inherit, pid):  # noqa: N802
        self.calls.append(f"open:{access:#x}:{pid}")
        return None if self.fail == "open" else 22

    def AssignProcessToJobObject(self, job, process):  # noqa: N802
        self.calls.append(f"assign:{job}:{process}")
        return 0 if self.fail == "assign" else 1

    def TerminateJobObject(self, job, code):  # noqa: N802
        self.calls.append(f"terminate:{job}")
        return 1

    def CloseHandle(self, handle):  # noqa: N802
        self.calls.append(f"close:{handle}")
        return 1


def test_everything_is_a_noop_off_windows(monkeypatch):
    monkeypatch.setattr(sys, "platform", "linux")
    assert _winjob.attach(1) is None
    assert _winjob.terminate(object()) is False
    _winjob.close(object())  # must not raise


def test_attach_builds_a_kill_on_close_job_and_assigns_the_process(monkeypatch):
    k32 = _K32()
    monkeypatch.setattr(_winjob, "_kernel32", lambda: k32)

    job = _winjob.attach(4242)

    assert job == 11
    assert k32.calls == [
        "create",
        "set:9:0x2000",  # extended limits, KILL_ON_JOB_CLOSE
        "open:0x101:4242",  # SET_QUOTA | TERMINATE
        "assign:11:22",
        "close:22",  # the process handle is released, the job handle is kept
    ]


def test_attach_cleans_up_after_each_failure(monkeypatch):
    for step, expected_tail in [
        ("create", []),
        ("set", ["close:11"]),
        ("open", ["close:11"]),
        ("assign", ["close:22", "close:11"]),
    ]:
        k32 = _K32(fail=step)
        monkeypatch.setattr(_winjob, "_kernel32", lambda k=k32: k)
        assert _winjob.attach(1) is None, step
        assert k32.calls[len(k32.calls) - len(expected_tail) :] == expected_tail, step


def test_terminate_and_close_use_the_job_handle(monkeypatch):
    k32 = _K32()
    monkeypatch.setattr(_winjob, "_kernel32", lambda: k32)

    assert _winjob.terminate(11) is True
    _winjob.close(11)

    assert k32.calls == ["terminate:11", "close:11"]
