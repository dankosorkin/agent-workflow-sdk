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

"""Shared base for one-shot CLI agent backends (Codex, Claude Code).

Unlike Kiro's persistent JSON-RPC session, these agents run non-interactively
as a fresh subprocess per turn and stream newline-delimited JSON on stdout.
A "session" is a resumable id the CLI prints on the first turn and accepts
back via a resume flag, so the base tracks that id and hands it to the
subclass when it builds the next command.

Subclasses implement two things:

- :meth:`build_command` — the argv for one turn, given the prompt text and the
  current session id (``None`` on the first turn).
- :meth:`parse_line` — map one parsed JSON line to zero or more
  :class:`BackendEvent`s, and optionally capture the session id / final text.

The base owns process spawning, the async stdout read loop, event
aggregation, and the terminal :class:`TurnEnd`.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from agentflow.backends.base import BaseAgentBackend, PermissionPolicy
from agentflow.errors import BackendError, BackendTransportError
from agentflow.events import (
    BackendEvent,
    BackendRequest,
    ErrorEvent,
    TextRequest,
    TurnEnd,
)

__all__ = ["CLIExecBackend", "TurnAccumulator"]


@dataclass
class TurnAccumulator:
    """Mutable per-turn scratch a subclass fills while parsing lines."""

    text_parts: list[str] = field(default_factory=list)
    stop_reason: str | None = None
    session_id: str | None = None
    is_error: bool = False

    def add_text(self, text: str) -> None:
        if text:
            self.text_parts.append(text)

    @property
    def text(self) -> str:
        return "".join(self.text_parts)


class CLIExecBackend(BaseAgentBackend):
    """Base for agents driven by one subprocess per turn with JSONL output."""

    #: Human-readable name of the underlying CLI, for error messages.
    cli_name: str = "cli-agent"

    def __init__(
        self,
        *,
        cwd: Path | str = ".",
        permission: PermissionPolicy,
        timeout: float | None = 600.0,
    ) -> None:
        super().__init__(permission=permission)
        self.cwd = Path(cwd)
        self.timeout = timeout
        self._session_id: str | None = None

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------

    def build_command(self, prompt: str, session_id: str | None) -> list[str]:
        """Return the argv for one turn. Prompt is passed on argv or stdin per
        subclass; if via stdin, put the prompt in :attr:`_stdin_text`."""
        raise NotImplementedError

    def parse_line(self, obj: dict[str, Any], acc: TurnAccumulator) -> list[BackendEvent]:
        """Map one JSON object to events, updating ``acc`` in place."""
        raise NotImplementedError

    #: Optional prompt text to feed on stdin instead of argv (set in build_command).
    _stdin_text: str | None = None

    def subprocess_env(self) -> dict[str, str] | None:
        """Environment for the child process. ``None`` inherits the parent's.

        Override to add or remove variables — e.g. a CLI that must not see an
        API key exported for a different backend.
        """
        return None

    # ------------------------------------------------------------------
    # Lifecycle — no persistent process, so start/close are trivial
    # ------------------------------------------------------------------

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    # ------------------------------------------------------------------
    # One turn
    # ------------------------------------------------------------------

    async def invoke(
        self, request: BackendRequest, *, session: str | None = None
    ) -> AsyncIterator[BackendEvent]:
        if not isinstance(request, TextRequest):
            raise BackendError(
                f"{type(self).__name__} accepts TextRequest, got {type(request).__name__}"
            )

        session_id = session or self._session_id
        self._stdin_text = None
        cmd = self.build_command(request.text, session_id)
        stdin_text = self._stdin_text

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=str(self.cwd),
                stdin=(
                    asyncio.subprocess.PIPE
                    if stdin_text is not None
                    else asyncio.subprocess.DEVNULL
                ),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=self.subprocess_env(),
                # Some CLIs emit very large single lines (e.g. Claude's init
                # event lists every tool/skill). Raise the line buffer well
                # past asyncio's 64KiB default so readline() doesn't overflow.
                limit=8 * 1024 * 1024,
            )
        except OSError as exc:
            raise BackendTransportError(f"could not start {self.cli_name}: {exc}") from exc

        if stdin_text is not None and proc.stdin is not None:
            proc.stdin.write(stdin_text.encode("utf-8"))
            proc.stdin.close()

        acc = TurnAccumulator()
        try:
            async for event in self._read_stream(proc, acc):
                yield event
        finally:
            await self._reap(proc)

        # Persist the session id the subclass captured for the next turn.
        if acc.session_id:
            self._session_id = acc.session_id

        yield TurnEnd(
            text=acc.text,
            stop_reason=acc.stop_reason or ("error" if acc.is_error else "end_turn"),
        )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _read_stream(
        self, proc: asyncio.subprocess.Process, acc: TurnAccumulator
    ) -> AsyncIterator[BackendEvent]:
        assert proc.stdout is not None
        while True:
            try:
                line = await _readline(proc.stdout, self.timeout)
            except TimeoutError as exc:
                proc.kill()
                raise BackendTransportError(
                    f"{self.cli_name} produced no output within {self.timeout}s"
                ) from exc
            if line == b"":
                break
            text = line.strip()
            if not text:
                continue
            try:
                obj = json.loads(text)
            except json.JSONDecodeError:
                # Some CLIs interleave non-JSON banner lines; surface as data.
                yield ErrorEvent(
                    message=f"non-JSON line from {self.cli_name}",
                    detail={"line": text.decode("utf-8", "replace")},
                )
                continue
            if not isinstance(obj, dict):
                continue
            for event in self.parse_line(obj, acc):
                yield event

    async def _reap(self, proc: asyncio.subprocess.Process) -> None:
        if proc.returncode is None:
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except TimeoutError:
                proc.kill()
                await proc.wait()
        # Drain stderr for diagnostics on a nonzero exit.
        if proc.returncode not in (0, None) and proc.stderr is not None:
            err = (await proc.stderr.read()).decode("utf-8", "replace").strip()
            if err:
                raise BackendTransportError(
                    f"{self.cli_name} exited {proc.returncode}: {err[:2000]}"
                )


async def _readline(stream: asyncio.StreamReader, timeout: float | None) -> bytes:
    if timeout is None:
        return await stream.readline()
    return await asyncio.wait_for(stream.readline(), timeout=timeout)
