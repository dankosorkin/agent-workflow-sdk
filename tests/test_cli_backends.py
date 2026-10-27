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

"""Offline parser tests for Codex and Claude Code backends.

Feeds the real JSONL event shapes captured from `codex exec --json` and
`claude -p --output-format stream-json` through each backend's parse_line,
and drives a full turn against a fake subprocess for the CLIExecBackend base.
"""

from __future__ import annotations

import asyncio
import json

from agentflow.backends.base import AllowAll
from agentflow.backends.claude_code import ClaudeCodeBackend
from agentflow.backends.cli_exec import TurnAccumulator
from agentflow.backends.codex import CodexBackend
from agentflow.events import TextChunk, ToolCall, ToolResult, TurnEnd

# --- captured Codex event stream (trimmed to the essentials) ---
CODEX_LINES = [
    {"type": "thread.started", "thread_id": "01a0ebda-5b74-7c51"},
    {"type": "turn.started"},
    {"type": "item.completed", "item": {"id": "item_0", "type": "agent_message", "text": "pong"}},
    {"type": "turn.completed", "usage": {"output_tokens": 5}},
]

# --- captured Claude Code stream (trimmed) ---
CLAUDE_LINES = [
    {"type": "system", "subtype": "init", "session_id": "e8d6202d", "model": "claude-sonnet-5"},
    {
        "type": "stream_event",
        "event": {"type": "content_block_delta", "delta": {"type": "text_delta", "text": "po"}},
    },
    {
        "type": "stream_event",
        "event": {"type": "content_block_delta", "delta": {"type": "text_delta", "text": "ng"}},
    },
    {
        "type": "result",
        "subtype": "success",
        "result": "pong",
        "stop_reason": "end_turn",
        "session_id": "e8d6202d",
        "is_error": False,
    },
]


def _feed(backend, lines):
    acc = TurnAccumulator()
    events = []
    for obj in lines:
        events.extend(backend.parse_line(obj, acc))
    return events, acc


def test_codex_parses_agent_message():
    backend = CodexBackend(permission=AllowAll())
    events, acc = _feed(backend, CODEX_LINES)
    assert acc.session_id == "01a0ebda-5b74-7c51"
    assert acc.stop_reason == "end_turn"
    text_chunks = [e for e in events if isinstance(e, TextChunk)]
    assert "".join(c.text for c in text_chunks) == "pong"


def test_codex_parses_command_execution():
    backend = CodexBackend(permission=AllowAll())
    acc = TurnAccumulator()
    events = backend.parse_line(
        {
            "type": "item.completed",
            "item": {
                "id": "c1",
                "type": "command_execution",
                "command": "ls",
                "exit_code": 0,
                "aggregated_output": "file.txt",
            },
        },
        acc,
    )
    calls = [e for e in events if isinstance(e, ToolCall)]
    results = [e for e in events if isinstance(e, ToolResult)]
    assert calls and calls[0].name == "shell" and calls[0].args["command"] == "ls"
    assert results and results[0].status == "ok"


def test_claude_parses_streamed_text():
    backend = ClaudeCodeBackend(permission=AllowAll())
    events, acc = _feed(backend, CLAUDE_LINES)
    assert acc.session_id == "e8d6202d"
    assert acc.stop_reason == "end_turn"
    assert acc.text == "pong"
    # result must NOT re-emit text because deltas already streamed
    text_chunks = [e for e in events if isinstance(e, TextChunk)]
    assert "".join(c.text for c in text_chunks) == "pong"


def test_claude_result_fallback_when_no_stream():
    """If partial messages didn't stream, the result text is the fallback."""
    backend = ClaudeCodeBackend(permission=AllowAll())
    acc = TurnAccumulator()
    events = []
    events += backend.parse_line({"type": "system", "subtype": "init", "session_id": "s"}, acc)
    events += backend.parse_line(
        {"type": "result", "result": "hello", "stop_reason": "end_turn", "is_error": False}, acc
    )
    text = [e for e in events if isinstance(e, TextChunk)]
    assert acc.text == "hello"
    assert text and text[0].text == "hello"


def test_claude_parses_tool_use():
    backend = ClaudeCodeBackend(permission=AllowAll())
    acc = TurnAccumulator()
    # stream some text first so tool_use isn't treated as text fallback
    backend.parse_line(
        {
            "type": "stream_event",
            "event": {"type": "content_block_delta", "delta": {"type": "text_delta", "text": "ok"}},
        },
        acc,
    )
    events = backend.parse_line(
        {
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "text", "text": "ok"},
                    {"type": "tool_use", "id": "t1", "name": "Bash", "input": {"command": "ls"}},
                ]
            },
        },
        acc,
    )
    calls = [e for e in events if isinstance(e, ToolCall)]
    assert len(calls) == 1
    assert calls[0].name == "Bash" and calls[0].args == {"command": "ls"}


# --- full-turn drive of the CLIExecBackend base against a fake subprocess ---


class _FakeStdout:
    def __init__(self, lines: list[dict]):
        self._data = [(json.dumps(o) + "\n").encode() for o in lines] + [b""]
        self._i = 0

    async def readline(self) -> bytes:
        item = self._data[self._i]
        self._i += 1
        return item


class _FakeStderr:
    async def read(self) -> bytes:
        return b""


class _FakeProc:
    def __init__(self, lines):
        self.stdout = _FakeStdout(lines)
        self.stderr = _FakeStderr()
        self.stdin = None
        self.returncode = 0

    def kill(self):
        self.returncode = -9

    async def wait(self):
        return self.returncode


async def test_cli_exec_full_turn(monkeypatch):
    backend = CodexBackend(permission=AllowAll())

    async def fake_exec(*args, **kwargs):
        return _FakeProc(CODEX_LINES)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)

    events = [e async for e in backend.prompt("say pong")]
    kinds = [type(e).__name__ for e in events]
    assert kinds[-1] == "TurnEnd"
    end = events[-1]
    assert isinstance(end, TurnEnd) and end.text == "pong" and end.stop_reason == "end_turn"
    # session id captured for resume
    assert backend._session_id == "01a0ebda-5b74-7c51"


# --- PermissionPolicy -> CLI flag bridge ------------------------------------


def test_codex_sandbox_from_allow_all():
    from agentflow.backends.base import AllowAll

    cmd = CodexBackend(permission=AllowAll()).build_command("hi", None)
    assert "-s" in cmd and cmd[cmd.index("-s") + 1] == "workspace-write"


def test_codex_sandbox_from_deny_all():
    from agentflow.backends.base import DenyAll

    cmd = CodexBackend(permission=DenyAll()).build_command("hi", None)
    assert cmd[cmd.index("-s") + 1] == "read-only"


def test_codex_sandbox_from_allowlist_is_readonly():
    from agentflow.backends.base import ToolAllowlist

    cmd = CodexBackend(permission=ToolAllowlist({"read_file"})).build_command("hi", None)
    assert cmd[cmd.index("-s") + 1] == "read-only"


def test_codex_explicit_sandbox_wins_over_policy():
    from agentflow.backends.base import AllowAll

    # AllowAll would derive workspace-write, but an explicit sandbox overrides.
    cmd = CodexBackend(sandbox="read-only", permission=AllowAll()).build_command("hi", None)
    assert cmd[cmd.index("-s") + 1] == "read-only"


def test_claude_allowlist_maps_to_allowed_tools():
    from agentflow.backends.base import ToolAllowlist

    cmd = ClaudeCodeBackend(permission=ToolAllowlist({"Read", "Grep"})).build_command("hi", None)
    assert "--allowed-tools" in cmd
    tools = cmd[cmd.index("--allowed-tools") + 1]
    assert "Read" in tools and "Grep" in tools
    assert "--permission-mode" not in cmd


def test_claude_deny_all_maps_to_plan_mode():
    from agentflow.backends.base import DenyAll

    cmd = ClaudeCodeBackend(permission=DenyAll()).build_command("hi", None)
    assert "--permission-mode" in cmd and cmd[cmd.index("--permission-mode") + 1] == "plan"


def test_claude_explicit_tools_win_over_policy():
    from agentflow.backends.base import DenyAll

    # Explicit allowed_tools set -> policy derivation is skipped.
    cmd = ClaudeCodeBackend(allowed_tools=["Bash"], permission=DenyAll()).build_command("hi", None)
    assert cmd[cmd.index("--allowed-tools") + 1] == "Bash"
    assert "--permission-mode" not in cmd


# --- reset_session: reuse one instance without leaking the previous conversation ---


def _capture_exec(monkeypatch, lines=CODEX_LINES):
    """Fake subprocess spawner that records every argv it is asked to start."""
    argvs: list[list[str]] = []

    async def fake_exec(*args, **kwargs):
        argvs.append(list(args))
        return _FakeProc(lines)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    return argvs


async def _drain(backend, text, **kw):
    return [e async for e in backend.prompt(text, **kw)]


async def test_next_turn_resumes_the_previous_session_by_default(monkeypatch):
    # The behaviour reset_session exists to switch off.
    argvs = _capture_exec(monkeypatch)
    backend = CodexBackend(permission=AllowAll())
    await _drain(backend, "one")
    await _drain(backend, "two")
    assert "resume" not in argvs[0]
    assert argvs[1][-3:] == ["resume", "01a0ebda-5b74-7c51", "two"]


async def test_reset_session_makes_the_next_turn_start_fresh(monkeypatch):
    argvs = _capture_exec(monkeypatch)
    backend = CodexBackend(permission=AllowAll())
    await _drain(backend, "one")
    await backend.reset_session()
    assert backend._session_id is None
    await _drain(backend, "two")
    assert "resume" not in argvs[1]  # a brand-new conversation, no leaked context
    await _drain(backend, "three")
    assert argvs[2][-3:] == ["resume", "01a0ebda-5b74-7c51", "three"]  # and it resumes again


async def test_reset_session_works_for_claude_code_too(monkeypatch):
    argvs = _capture_exec(monkeypatch, CLAUDE_LINES)
    backend = ClaudeCodeBackend(permission=AllowAll())
    await _drain(backend, "one")
    await backend.reset_session()
    await _drain(backend, "two")
    assert "--resume" not in argvs[1]


async def test_explicit_session_is_still_honoured_after_reset(monkeypatch):
    argvs = _capture_exec(monkeypatch)
    backend = CodexBackend(permission=AllowAll())
    await backend.reset_session()
    await _drain(backend, "x", session="chosen-by-caller")
    assert argvs[0][-3:] == ["resume", "chosen-by-caller", "x"]


async def test_reset_during_a_running_turn_is_not_undone_when_it_ends(monkeypatch):
    """A turn that began before the reset must not bring its old session id back."""
    release = asyncio.Event()

    class _GatedStdout(_FakeStdout):
        async def readline(self) -> bytes:
            await release.wait()
            return await super().readline()

    class _GatedProc(_FakeProc):
        def __init__(self, lines):
            super().__init__(lines)
            self.stdout = _GatedStdout(lines)

    async def fake_exec(*args, **kwargs):
        return _GatedProc(CODEX_LINES)

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)
    backend = CodexBackend(permission=AllowAll())

    running = asyncio.create_task(_drain(backend, "slow"))
    await asyncio.sleep(0.01)  # the turn has started and is waiting on the process
    await backend.reset_session()
    release.set()
    await running

    assert backend._session_id is None


async def test_agent_backend_default_reset_is_a_noop():
    from agentflow.backends.base import BaseAgentBackend

    class Stateless(BaseAgentBackend):
        async def start(self):
            return None

        async def close(self):
            return None

        async def invoke(self, request, *, session=None):
            if False:
                yield None

    await Stateless(permission=AllowAll()).reset_session()  # nothing to forget, must not raise


def test_claude_allow_all_maps_to_accept_edits_never_bypass():
    cmd = ClaudeCodeBackend(permission=AllowAll()).build_command("hi", None)
    assert cmd[cmd.index("--permission-mode") + 1] == "acceptEdits"
    assert "--allowed-tools" not in cmd
    assert "--allow-dangerously-skip-permissions" not in cmd
    assert "bypassPermissions" not in cmd


def test_claude_allow_all_yields_to_explicit_settings():
    by_mode = ClaudeCodeBackend(permission_mode="plan", permission=AllowAll())
    cmd = by_mode.build_command("hi", None)
    assert cmd[cmd.index("--permission-mode") + 1] == "plan"
    by_tools = ClaudeCodeBackend(allowed_tools=["Read"], permission=AllowAll())
    cmd = by_tools.build_command("hi", None)
    assert cmd[cmd.index("--allowed-tools") + 1] == "Read"
    assert "--permission-mode" not in cmd


def test_claude_interactive_policy_adds_no_permission_flags():
    from agentflow.backends.base import Interactive

    cmd = ClaudeCodeBackend(permission=Interactive()).build_command("hi", None)
    assert "--permission-mode" not in cmd and "--allowed-tools" not in cmd
