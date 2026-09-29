# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""Codex agent backend — one-shot ``codex exec --json`` per turn.

Codex exposes a non-interactive mode that streams JSONL events. Observed
event shapes (codex-cli 0.157):

    {"type":"thread.started","thread_id":"..."}
    {"type":"turn.started"}
    {"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"..."}}
    {"type":"item.completed","item":{"type":"command_execution","command":"...","exit_code":0}}
    {"type":"turn.completed","usage":{...}}

A session is the ``thread_id``; subsequent turns resume it via
``codex exec resume <thread_id>``. Requires ``codex`` on PATH.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from agentflow.backends.base import PermissionPolicy
from agentflow.backends.cli_exec import CLIExecBackend, TurnAccumulator
from agentflow.events import BackendEvent, TextChunk, ToolCall, ToolResult

__all__ = ["CodexBackend"]


class CodexBackend(CLIExecBackend):
    cli_name = "codex"

    def __init__(
        self,
        *,
        model: str | None = None,
        sandbox: str = "read-only",
        skip_git_repo_check: bool = True,
        cwd: Path | str = ".",
        permission: PermissionPolicy,
        timeout: float | None = 600.0,
        extra_args: list[str] | None = None,
    ) -> None:
        super().__init__(cwd=cwd, permission=permission, timeout=timeout)
        self.model = model
        self.sandbox = sandbox
        self.skip_git_repo_check = skip_git_repo_check
        self.extra_args = list(extra_args or [])

    def build_command(self, prompt: str, session_id: str | None) -> list[str]:
        cmd = ["codex", "exec", "--json"]
        if self.skip_git_repo_check:
            cmd.append("--skip-git-repo-check")
        if self.sandbox:
            cmd += ["-s", self.sandbox]
        if self.model:
            cmd += ["-m", self.model]
        cmd += self.extra_args
        if session_id:
            # Resume an existing thread; the prompt is the next instruction.
            cmd += ["resume", session_id, prompt]
        else:
            cmd.append(prompt)
        return cmd

    def parse_line(self, obj: dict[str, Any], acc: TurnAccumulator) -> list[BackendEvent]:
        kind = obj.get("type")

        if kind == "thread.started":
            acc.session_id = obj.get("thread_id")
            return []

        if kind == "item.completed":
            return self._parse_item(obj.get("item") or {}, acc)

        if kind == "item.started":
            # Emitted for long-running items; we act on completion instead.
            return []

        if kind == "turn.completed":
            acc.stop_reason = "end_turn"
            return []

        if kind == "turn.failed" or kind == "error":
            acc.is_error = True
            acc.stop_reason = "error"
            return []

        return []

    def _parse_item(self, item: dict[str, Any], acc: TurnAccumulator) -> list[BackendEvent]:
        itype = item.get("type")

        if itype == "agent_message":
            text = item.get("text") or ""
            acc.add_text(text)
            return [TextChunk(text=text)] if text else []

        if itype == "command_execution":
            call_id = str(item.get("id") or "")
            command = item.get("command") or ""
            exit_code = item.get("exit_code")
            status: Literal["ok", "error"] = "ok" if exit_code in (0, None) else "error"
            return [
                ToolCall(id=call_id, name="shell", args={"command": command}, title=command),
                ToolResult(id=call_id, status=status, content=item.get("aggregated_output")),
            ]

        if itype in ("file_change", "patch_apply"):
            call_id = str(item.get("id") or "")
            return [ToolCall(id=call_id, name=itype, args=item, title=itype)]

        # reasoning, todo_list, and other item types are informational.
        return []
