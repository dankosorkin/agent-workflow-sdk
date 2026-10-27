"""Pin the catalog to what the SDK's real backend classes do, so it cannot drift silently."""

from __future__ import annotations

from agentflow.backends import AllowAll, DenyAll, ToolAllowlist
from agentflow.backends.claude_code import ClaudeCodeBackend
from agentflow.backends.codex import CodexBackend
from agentflow.workflow import CATALOG


def flag(cmd: list[str], name: str) -> str | None:
    return cmd[cmd.index(name) + 1] if name in cmd else None


def test_codex_allowlist_silently_becomes_read_only():
    """The reason the catalog says codex cannot enforce an allowlist."""
    cmd = CodexBackend(permission=ToolAllowlist({"write"})).build_command("p", None)
    assert flag(cmd, "-s") == flag(
        CodexBackend(permission=DenyAll()).build_command("p", None), "-s"
    )
    assert CATALOG["codex"].allowlist is False


def test_claude_code_levels_map_to_flags():
    assert CATALOG["claude_code"].allowlist is True
    read_only = ClaudeCodeBackend(permission=DenyAll()).build_command("p", None)
    assert flag(read_only, "--permission-mode") == "plan"
    allow = ClaudeCodeBackend(permission=ToolAllowlist({"Read"})).build_command("p", None)
    assert flag(allow, "--allowed-tools") == "Read"
    full = ClaudeCodeBackend(permission=AllowAll()).build_command("p", None)
    assert flag(full, "--permission-mode") == "acceptEdits"
    assert "bypassPermissions" not in full


def test_claude_code_emits_no_tool_results():
    from agentflow.backends import claude_code

    assert not hasattr(claude_code, "ToolResult")
    assert CATALOG["claude_code"].tool_results == "no"
