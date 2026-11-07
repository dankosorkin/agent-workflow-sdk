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

"""Claude Code agent backend — one-shot ``claude -p`` per turn (stream-json).

Claude Code's print mode streams newline-delimited JSON. Observed shapes
(Claude Code 2.1):

    {"type":"system","subtype":"init","session_id":"...","model":"...","tools":[...]}
    {"type":"stream_event","event":{"type":"content_block_delta",
        "delta":{"type":"text_delta","text":"..."}}}
    {"type":"assistant","message":{"content":[
        {"type":"text","text":"..."},
        {"type":"tool_use","id":"...","name":"Bash","input":{...}}]}}
    {"type":"result","subtype":"success","result":"...","stop_reason":"end_turn",
        "session_id":"...","is_error":false}

A session is the ``session_id``; resume it with ``--resume <id>``. Streaming
deltas require ``--include-partial-messages``. Requires ``claude`` on PATH.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections import deque
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from agentflow.backends.base import PermissionPolicy, policy_intent
from agentflow.backends.cli_exec import CLIExecBackend, TurnAccumulator, _readline
from agentflow.errors import BackendError, BackendTransportError
from agentflow.events import (
    BackendEvent,
    BackendRequest,
    ErrorEvent,
    TextChunk,
    TextRequest,
    ToolCall,
    TurnEnd,
)
from agentflow.process import ManagedProcess

__all__ = ["ClaudeCodeBackend", "PersistentClaudeBackend"]


class ClaudeCodeBackend(CLIExecBackend):
    cli_name = "claude"

    def __init__(
        self,
        *,
        model: str | None = None,
        agent: str | None = None,
        allowed_tools: list[str] | None = None,
        disallowed_tools: list[str] | None = None,
        permission_mode: str | None = None,
        skip_permissions: bool = False,
        cwd: Path | str = ".",
        permission: PermissionPolicy,
        timeout: float | None = 600.0,
        extra_args: list[str] | None = None,
        use_api_key_env: bool = False,
    ) -> None:
        super().__init__(cwd=cwd, permission=permission, timeout=timeout)
        self.model = model
        self.agent = agent
        self.allowed_tools = list(allowed_tools or [])
        self.disallowed_tools = list(disallowed_tools or [])
        self.permission_mode = permission_mode
        self.skip_permissions = skip_permissions
        self.extra_args = list(extra_args or [])
        # The claude CLI authenticates via its own claude.ai login by default
        # and errors when ANTHROPIC_API_KEY is present in the environment
        # ("connectors are disabled because ANTHROPIC_API_KEY ... takes
        # precedence"). Strip it from the child env unless the caller opts in
        # to key-based auth.
        self.use_api_key_env = use_api_key_env
        # Track streamed text so we don't double-count the final assistant
        # message block, which repeats the same text as the deltas.
        self._streamed_any = False

    def subprocess_env(self):
        import os

        if self.use_api_key_env:
            return None  # inherit parent env, including ANTHROPIC_* vars
        # Claude Code uses its own claude.ai login and its own model config.
        # Any ANTHROPIC_* var in the environment (API key, or a model pin like
        # ANTHROPIC_MODEL) either disables its connectors or forces a
        # possibly-deprecated model, so scrub them all for the child process.
        return {k: v for k, v in os.environ.items() if not k.startswith("ANTHROPIC_")}

    _HEAD = [
        "claude",
        "-p",
        "--output-format",
        "stream-json",
        "--verbose",
        "--include-partial-messages",
    ]

    def _flags(self) -> list[str]:
        """Process-level flags: model, agent, tools and permissions. Fixed for the life of
        one process, which is why a persistent process is keyed by them."""
        cmd: list[str] = []
        if self.model:
            cmd += ["--model", self.model]
        if self.agent:
            cmd += ["--agent", self.agent]

        # Explicit tool flags win; otherwise derive them from the permission
        # policy. Claude Code has no per-tool runtime gate we can hook, so a
        # ToolAllowlist maps to --allowed-tools and a deny-all policy maps to
        # the read-only "plan" permission mode (no edits/commands). AllowAll maps
        # to "acceptEdits": headless ``claude -p`` refuses writes with no flag at all,
        # and acceptEdits lets it edit files without opening the shell. It is
        # deliberately NOT bypassPermissions: commands outside the working set still
        # need an explicit ``allowed_tools``/``permission_mode`` from the caller.
        allowed = list(self.allowed_tools)
        permission_mode = self.permission_mode
        if not allowed and permission_mode is None:
            intent = policy_intent(self.permission)
            if intent.allowlist is not None and intent.allowlist:
                allowed = sorted(intent.allowlist)
            elif intent.deny_all:
                permission_mode = "plan"
            elif intent.allow_all:
                permission_mode = "acceptEdits"

        if allowed:
            cmd += ["--allowed-tools", " ".join(allowed)]
        if self.disallowed_tools:
            cmd += ["--disallowed-tools", " ".join(self.disallowed_tools)]
        if permission_mode:
            cmd += ["--permission-mode", permission_mode]
        if self.skip_permissions:
            cmd.append("--allow-dangerously-skip-permissions")
        return cmd

    def build_command(self, prompt: str, session_id: str | None) -> list[str]:
        cmd = [*self._HEAD, *self._flags()]
        if session_id:
            cmd += ["--resume", session_id]
        cmd += self.extra_args
        # Feed the prompt on stdin to avoid argv length/quoting limits.
        self._stdin_text = prompt
        self._streamed_any = False
        return cmd

    def parse_line(self, obj: dict[str, Any], acc: TurnAccumulator) -> list[BackendEvent]:
        kind = obj.get("type")

        if kind == "system" and obj.get("subtype") == "init":
            acc.session_id = obj.get("session_id")
            return []

        if kind == "stream_event":
            return self._parse_stream_event(obj.get("event") or {}, acc)

        if kind == "assistant":
            return self._parse_assistant(obj.get("message") or {}, acc)

        if kind == "result":
            acc.session_id = obj.get("session_id") or acc.session_id
            acc.stop_reason = obj.get("stop_reason") or obj.get("subtype")
            acc.is_error = bool(obj.get("is_error"))
            # If nothing streamed (partial messages unsupported), fall back to
            # the final result text.
            if not self._streamed_any and isinstance(obj.get("result"), str):
                acc.add_text(obj["result"])
                return [TextChunk(text=obj["result"])]
            return []

        return []

    def _parse_stream_event(
        self, event: dict[str, Any], acc: TurnAccumulator
    ) -> list[BackendEvent]:
        if event.get("type") == "content_block_delta":
            delta = event.get("delta") or {}
            if delta.get("type") == "text_delta":
                text = delta.get("text") or ""
                if text:
                    acc.add_text(text)
                    self._streamed_any = True
                    return [TextChunk(text=text)]
        return []

    def _parse_assistant(self, message: dict[str, Any], acc: TurnAccumulator) -> list[BackendEvent]:
        events: list[BackendEvent] = []
        for block in message.get("content") or []:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "tool_use":
                events.append(
                    ToolCall(
                        id=str(block.get("id") or ""),
                        name=block.get("name") or "",
                        args=block.get("input") or {},
                        title=block.get("name"),
                    )
                )
            # Text blocks here duplicate the streamed deltas; skip them when we
            # already streamed. If nothing streamed, capture as a fallback.
            elif block.get("type") == "text" and not self._streamed_any:
                text = block.get("text") or ""
                if text:
                    acc.add_text(text)
                    events.append(TextChunk(text=text))
        return events


class PersistentClaudeBackend(ClaudeCodeBackend):
    """Claude Code as ONE long-lived process serving many turns.

    ``claude -p --input-format stream-json`` reads user messages from stdin and keeps
    its MCP servers, tools and caches between them, so only the first turn pays the
    start-up cost. A conversation is cleared in place with ``/clear`` (new session id,
    no memory of earlier turns), which is what :meth:`reset_session` does.

    The agent, model, tools and permissions are command-line flags, so they are fixed
    for the process: pool processes by them (see ``agentflow.backends.pool``). A turn
    abandoned before its result leaves the output stream at an unknown position, so
    the process is stopped and the backend reports ``reusable = False``.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._proc: ManagedProcess | None = None
        self._stderr_tail: deque[str] = deque(maxlen=40)
        self._stderr_task: asyncio.Task[None] | None = None
        self._busy = False
        self._dirty = False  # a turn has run since the last clear
        self._broken = False

    @property
    def reusable(self) -> bool:
        """False once the process died or a turn was abandoned mid-stream."""
        return not self._broken and self._proc is not None

    async def start(self) -> None:
        if self._proc is not None:
            return
        cmd = [*self._HEAD, "--input-format", "stream-json", *self._flags(), *self.extra_args]
        try:
            self._proc = await ManagedProcess.spawn(
                cmd, cwd=self.cwd, env=self.subprocess_env(), stderr=asyncio.subprocess.PIPE
            )
        except OSError as exc:
            raise BackendTransportError(f"could not start {self.cli_name}: {exc}") from exc
        self._stderr_task = asyncio.create_task(self._drain_stderr())

    async def close(self) -> None:
        proc, self._proc = self._proc, None
        self._broken = True
        if proc is not None:
            await proc.stop(grace=0.5)
        task, self._stderr_task = self._stderr_task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    async def reset_session(self) -> None:
        await super().reset_session()
        if self._proc is None or not self._dirty or self._broken:
            return
        await self._run_to_result("/clear", acc=TurnAccumulator())
        self._dirty = False

    async def invoke(
        self, request: BackendRequest, *, session: str | None = None
    ) -> AsyncIterator[BackendEvent]:
        if not isinstance(request, TextRequest):
            raise BackendError(
                f"{type(self).__name__} accepts TextRequest, got {type(request).__name__}"
            )
        if session is not None:
            raise BackendError("a persistent Claude process cannot resume a session; reset it")
        if self._proc is None or self._broken:
            raise BackendError("PersistentClaudeBackend is not running (start() was not called)")
        if self._busy:
            raise BackendError("a turn is already in progress")
        self._busy = True
        acc = TurnAccumulator()
        self._streamed_any = False
        finished = False
        try:
            async for event in self._stream_turn(request.text, acc):
                yield event
            finished = True
            self._dirty = True
            # Free the backend BEFORE the terminal event, so a consumer that stops at
            # TurnEnd cannot leave it busy (same rule as KiroBackend).
            self._busy = False
            yield TurnEnd(
                text=acc.text,
                stop_reason=acc.stop_reason or ("error" if acc.is_error else "end_turn"),
                usage=acc.usage,
            )
        finally:
            self._busy = False
            if not finished:
                await self._abandon()

    # -- internals ------------------------------------------------------------

    async def _run_to_result(self, text: str, *, acc: TurnAccumulator) -> None:
        try:
            async for _ in self._stream_turn(text, acc):
                pass
        except BaseException:
            await self._abandon()
            raise

    async def _stream_turn(self, text: str, acc: TurnAccumulator) -> AsyncIterator[BackendEvent]:
        proc = self._proc
        assert proc is not None and proc.stdin is not None and proc.stdout is not None
        message = {"type": "user", "message": {"role": "user", "content": text}}
        try:
            proc.stdin.write((json.dumps(message) + "\n").encode("utf-8"))
            await proc.stdin.drain()
        except (ConnectionError, OSError) as exc:
            raise BackendTransportError(f"{self.cli_name} is gone: {self._tail()}") from exc
        while True:
            try:
                line = await _readline(proc.stdout, self.timeout)
            except TimeoutError as exc:
                raise BackendTransportError(
                    f"{self.cli_name} produced no output within {self.timeout}s"
                ) from exc
            if line == b"":
                raise BackendTransportError(f"{self.cli_name} exited mid-turn: {self._tail()}")
            raw = line.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                yield ErrorEvent(
                    message=f"non-JSON line from {self.cli_name}",
                    detail={"line": raw.decode("utf-8", "replace")},
                )
                continue
            if not isinstance(obj, dict):
                continue
            for event in self.parse_line(obj, acc):
                yield event
            if obj.get("type") == "result":
                return

    async def _abandon(self) -> None:
        """The stream position is unknown: stop the process rather than reuse it."""
        self._broken = True
        proc = self._proc
        if proc is not None:
            proc.kill()
            proc.release()

    async def _drain_stderr(self) -> None:
        proc = self._proc
        if proc is None or proc.stderr is None:
            return
        while True:
            line = await proc.stderr.readline()
            if not line:
                return
            self._stderr_tail.append(line.decode("utf-8", "replace").rstrip())

    def _tail(self) -> str:
        return " | ".join(self._stderr_tail)[-2000:] or "no stderr"
