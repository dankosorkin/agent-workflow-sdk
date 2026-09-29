# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

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

from pathlib import Path
from typing import Any

from agentflow.backends.base import PermissionPolicy
from agentflow.backends.cli_exec import CLIExecBackend, TurnAccumulator
from agentflow.events import BackendEvent, TextChunk, ToolCall

__all__ = ["ClaudeCodeBackend"]


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

    def build_command(self, prompt: str, session_id: str | None) -> list[str]:
        cmd = [
            "claude",
            "-p",
            "--output-format",
            "stream-json",
            "--verbose",
            "--include-partial-messages",
        ]
        if self.model:
            cmd += ["--model", self.model]
        if self.agent:
            cmd += ["--agent", self.agent]
        if self.allowed_tools:
            cmd += ["--allowed-tools", " ".join(self.allowed_tools)]
        if self.disallowed_tools:
            cmd += ["--disallowed-tools", " ".join(self.disallowed_tools)]
        if self.permission_mode:
            cmd += ["--permission-mode", self.permission_mode]
        if self.skip_permissions:
            cmd.append("--allow-dangerously-skip-permissions")
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
