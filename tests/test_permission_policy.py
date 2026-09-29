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

"""Secure-by-default authorization: permission is required; allowlist works."""

from __future__ import annotations

import pytest
from agentflow.backends.base import (
    AllowAll,
    BaseAgentBackend,
    DenyAll,
    ToolAllowlist,
)
from agentflow.events import Allow, Deny, PermissionOption, PermissionRequest


def _req(tool: str) -> PermissionRequest:
    return PermissionRequest(
        id="1", tool=tool, options=(PermissionOption("a", "Allow", "allow_once"),)
    )


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


async def test_callback_policy_allow_and_deny():
    from agentflow.backends.base import Callback

    async def yes(req):
        return "y"

    async def no(req):
        return "n"

    assert isinstance(await Callback(yes).decide(_req("fs_write")), Allow)
    assert isinstance(await Callback(no).decide(_req("fs_write")), Deny)


async def test_callback_policy_passes_request_to_resolver():
    from agentflow.backends.base import Callback

    seen = {}

    async def resolver(req):
        seen["tool"] = req.tool
        return Allow()  # a real decision passes through unchanged

    decision = await Callback(resolver).decide(_req("execute_bash"))
    assert isinstance(decision, Allow)
    assert seen["tool"] == "execute_bash"


async def test_callback_policy_normalizes_none_to_deny():
    from agentflow.backends.base import Callback

    async def nothing(req):
        return None

    assert isinstance(await Callback(nothing).decide(_req("x")), Deny)
