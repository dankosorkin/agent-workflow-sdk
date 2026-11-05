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

"""Windows Job Objects: keep a child and everything it spawns on a leash.

Windows has no process groups. A Job Object created with
``JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`` contains a process and its descendants:
terminating the job kills the whole tree, and when the last handle to the job
closes (including because *our* process crashed or was killed) the OS kills the
tree itself, so no ``kiro-cli-chat`` is left behind.

Standard library (``ctypes``) only. Every function is a safe no-op returning
``None``/``False`` off Windows or when the API call fails, so callers can fall
back to ``taskkill``. A process is assigned after it starts, so a grandchild
spawned in that first instant is not covered; the ``taskkill /T`` fallback exists
for that.

Known limit: a process already inside a job that forbids nesting cannot be
assigned (Windows 8+ allows nested jobs, so this is rare).
"""

from __future__ import annotations

import sys
from typing import Any

_KILL_ON_JOB_CLOSE = 0x2000
_JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9
_PROCESS_TERMINATE = 0x0001
_PROCESS_SET_QUOTA = 0x0100


def _kernel32() -> Any:
    """kernel32 with 64-bit-safe signatures, or ``None`` when not on Windows."""
    if sys.platform != "win32":
        return None
    import ctypes
    from ctypes import wintypes

    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    k32.CreateJobObjectW.argtypes = [wintypes.LPVOID, wintypes.LPCWSTR]
    k32.CreateJobObjectW.restype = wintypes.HANDLE
    k32.SetInformationJobObject.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    ]
    k32.SetInformationJobObject.restype = wintypes.BOOL
    k32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    k32.OpenProcess.restype = wintypes.HANDLE
    k32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    k32.AssignProcessToJobObject.restype = wintypes.BOOL
    k32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
    k32.TerminateJobObject.restype = wintypes.BOOL
    k32.CloseHandle.argtypes = [wintypes.HANDLE]
    k32.CloseHandle.restype = wintypes.BOOL
    return k32


def _limit_info() -> Any:
    """A JOBOBJECT_EXTENDED_LIMIT_INFORMATION with KILL_ON_JOB_CLOSE set."""
    import ctypes
    from ctypes import wintypes

    class BasicLimits(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_int64),
            ("PerJobUserTimeLimit", ctypes.c_int64),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class IoCounters(ctypes.Structure):
        _fields_ = [(n, ctypes.c_uint64) for n in ("r_ops", "w_ops", "o_ops", "r_b", "w_b", "o_b")]

    class ExtendedLimits(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", BasicLimits),
            ("IoInfo", IoCounters),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    info = ExtendedLimits()
    info.BasicLimitInformation.LimitFlags = _KILL_ON_JOB_CLOSE
    return info


def attach(pid: int) -> Any | None:
    """Put process ``pid`` in a new kill-on-close job and return the job handle.

    Returns ``None`` (nothing to clean up) off Windows or if any step fails.
    """
    import ctypes

    k32 = _kernel32()
    if k32 is None:
        return None
    job = k32.CreateJobObjectW(None, None)
    if not job:
        return None
    info = _limit_info()
    ok = k32.SetInformationJobObject(
        job, _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION, ctypes.byref(info), ctypes.sizeof(info)
    )
    if not ok:
        k32.CloseHandle(job)
        return None
    process = k32.OpenProcess(_PROCESS_SET_QUOTA | _PROCESS_TERMINATE, False, pid)
    if not process:
        k32.CloseHandle(job)
        return None
    try:
        assigned = k32.AssignProcessToJobObject(job, process)
    finally:
        k32.CloseHandle(process)
    if not assigned:
        k32.CloseHandle(job)
        return None
    return job


def terminate(job: Any) -> bool:
    """Kill every process in ``job``. ``True`` if Windows accepted it."""
    k32 = _kernel32()
    return bool(k32 is not None and job and k32.TerminateJobObject(job, 1))


def close(job: Any) -> None:
    """Release the job handle; with kill-on-close this also kills any survivor."""
    k32 = _kernel32()
    if k32 is not None and job:
        k32.CloseHandle(job)
