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

"""Process supervisor: start a child with its whole tree, stop it as a unit.

Every agent backend that runs an external CLI needs the same things: start a
child that owns its descendants, stop that whole tree (gracefully, then by force),
survive Windows having no signals or process groups, and not leave orphans
behind when the host exits. Those live here once, instead of in each backend.

- POSIX: the child leads its own session, so a signal to its process group reaches
  every descendant.
- Windows: the child is placed in a kill-on-close Job Object; ``taskkill /F /T`` is
  the fallback.
- Every live process is registered; :func:`kill_all` (also run at interpreter exit)
  force-stops whatever is left.

Standard library only.
"""

from __future__ import annotations

import asyncio
import atexit
import contextlib
import os
import signal
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from agentflow import _winjob

_live: set[ManagedProcess] = set()


def live_processes() -> list[ManagedProcess]:
    """The supervised processes that have not been released yet."""
    return list(_live)


def kill_all() -> None:
    """Force-stop every live supervised process (and its tree). Never raises."""
    for proc in list(_live):
        with contextlib.suppress(Exception):
            proc.kill()
        with contextlib.suppress(Exception):
            proc.release()


atexit.register(kill_all)


class ManagedProcess:
    """A child process and its descendants, started and stopped as one unit.

    Wraps an :class:`asyncio.subprocess.Process`; ``stdin``/``stdout``/``stderr``,
    ``pid``, ``returncode`` and ``wait()`` behave as on the wrapped process.
    """

    def __init__(self, proc: Any, job: Any = None) -> None:
        self._proc = proc
        self._job = job
        _live.add(self)

    @classmethod
    async def spawn(
        cls,
        cmd: Sequence[str],
        *,
        cwd: Path | str,
        env: dict[str, str] | None = None,
        stdin: int = asyncio.subprocess.PIPE,
        stderr: int | None = None,
        limit: int = 8 * 1024 * 1024,
    ) -> ManagedProcess:
        """Start ``cmd`` supervised. ``stdout`` is always a pipe.

        ``limit`` is the stream line buffer; agent CLIs emit very large single lines
        (a session listing every tool), well past asyncio's 64 KiB default.
        Raises :class:`OSError` if the program cannot be started.
        """
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(cwd),
            env=env,
            stdin=stdin,
            stdout=asyncio.subprocess.PIPE,
            stderr=stderr,
            limit=limit,
            # Own session = own process group on POSIX, so the whole tree can be
            # signalled. Ignored where it does not apply.
            start_new_session=True,
        )
        pid = getattr(proc, "pid", None)  # test doubles may not carry one
        job = None
        if sys.platform == "win32" and isinstance(pid, int):
            job = _winjob.attach(pid)
        return cls(proc, job)

    # -- the wrapped process ------------------------------------------------

    @property
    def pid(self) -> int:
        return self._proc.pid

    @property
    def returncode(self) -> int | None:
        return self._proc.returncode

    @property
    def stdin(self) -> Any:
        return self._proc.stdin

    @property
    def stdout(self) -> Any:
        return self._proc.stdout

    @property
    def stderr(self) -> Any:
        return self._proc.stderr

    async def wait(self) -> int:
        return await self._proc.wait()

    # -- stopping -----------------------------------------------------------

    def terminate(self) -> None:
        """Ask the process (and, where possible, its tree) to stop. Never raises.

        POSIX: ``SIGTERM`` to the process group. Windows has no softer step than
        ``TerminateProcess``, so this is already forceful there.
        """
        self._stop(hard=False)

    def kill(self) -> None:
        """Force-stop the process and its whole tree. Never raises."""
        self._stop(hard=True)

    async def stop(self, grace: float = 0.5) -> None:
        """Graceful ladder: close stdin, wait ``grace``, terminate, wait, kill.

        Returns once the ladder is done and releases the process. Agent CLIs often
        ignore a closed stdin, so each phase is short.
        """
        try:
            stdin = self._proc.stdin
            if stdin is not None and not stdin.is_closing():
                stdin.close()
            try:
                await asyncio.wait_for(self._proc.wait(), timeout=grace)
            except TimeoutError:
                self.terminate()
                try:
                    await asyncio.wait_for(self._proc.wait(), timeout=grace)
                except TimeoutError:
                    self.kill()
        finally:
            self.release()

    def release(self) -> None:
        """Stop supervising. Closing the Windows job also reaps any straggler."""
        _live.discard(self)
        if self._job is not None:
            _winjob.close(self._job)
            self._job = None

    def _stop(self, *, hard: bool) -> None:
        proc = self._proc
        killpg = getattr(os, "killpg", None)
        getpgid = getattr(os, "getpgid", None)
        sig = getattr(signal, "SIGKILL" if hard else "SIGTERM", None)
        if killpg is not None and getpgid is not None and sig is not None:
            with contextlib.suppress(ProcessLookupError, PermissionError, OSError, AttributeError):
                killpg(getpgid(proc.pid), sig)
                return
        job = self._job
        if hard and sys.platform == "win32" and not (job is not None and _winjob.terminate(job)):
            # No job (or Windows refused): kill the whole tree by pid.
            with contextlib.suppress(OSError, subprocess.SubprocessError):
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=5,
                    check=False,
                )
        with contextlib.suppress(ProcessLookupError, OSError):
            if hard:
                proc.kill()
            else:
                proc.terminate()
