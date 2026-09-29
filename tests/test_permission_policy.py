"""Secure-by-default authorization: permission is required; allowlist works."""

from __future__ import annotations

import pytest

from agentflow.backends.base import (
    AllowAll, DenyAll, ToolAllowlist, BaseAgentBackend,
)
from agentflow.events import Allow, Deny, PermissionOption, PermissionRequest


def _req(tool: str) -> PermissionRequest:
    return PermissionRequest(id="1", tool=tool,
                             options=(PermissionOption("a", "Allow", "allow_once"),))


def test_agent_backend_requires_explicit_permission():
    # Constructing the base mixin without a policy is a loud error.
    with pytest.raises(TypeError):
        BaseAgentBackend(None)  # type: ignore[arg-type]


def test_kiro_requires_permission():
    from agentflow.backends.kiro import KiroBackend
    with pytest.raises(TypeError):
        KiroBackend("vibe")  # missing required keyword-only permission


def test_codex_requires_permission():
    from agentflow.backends.codex import CodexBackend
    with pytest.raises(TypeError):
        CodexBackend(sandbox="read-only")  # missing permission


async def test_allowlist_allows_listed_tool():
    policy = ToolAllowlist({"read_file", "list_directory"})
    assert isinstance(await policy.decide(_req("read_file")), Allow)


async def test_allowlist_denies_unlisted_by_default():
    policy = ToolAllowlist({"read_file"})
    decision = await policy.decide(_req("fs_write"))
    assert isinstance(decision, Deny)


async def test_allowlist_custom_fallback():
    # Unlisted tools fall through to the configured fallback (here AllowAll).
    policy = ToolAllowlist({"read_file"}, fallback=AllowAll())
    assert isinstance(await policy.decide(_req("anything")), Allow)


async def test_allowlist_deny_fallback_explicit():
    policy = ToolAllowlist(set(), fallback=DenyAll())
    assert isinstance(await policy.decide(_req("x")), Deny)
