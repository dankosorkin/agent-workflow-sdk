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

"""Backend capability catalog and the plan-versus-backend checks.

Facts about what each backend can do, verified against the real agents (see the
SDK repository's design notes, section 6.2). The catalog is data so a drift test
can pin it to the SDK's real behaviour.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from agentflow.workflow.issues import Issue

__all__ = ["CATALOG", "PERMISSIONS", "Capabilities", "check_step_against_backend"]

PERMISSIONS = ("read_only", "allowlist", "full")


@dataclass(frozen=True)
class Capabilities:
    enforcement: str  # "runtime" | "launch_flags" | "sandbox_only" | "construction"
    allowlist: bool  # can restrict to a list of named tools
    agent_by_name: bool  # a named agent definition can be selected
    mcp: bool  # the SDK can pass MCP servers
    skills: bool  # the SDK can pass skills
    tool_results: str  # "yes" | "partial" | "no": does it emit ToolResult events
    usage: bool  # reports token usage
    parallel_on_instance: bool  # several concurrent turns on one instance are allowed


CATALOG: dict[str, Capabilities] = {
    "kiro": Capabilities("runtime", True, True, False, False, "yes", False, False),
    "claude_code": Capabilities("launch_flags", True, True, False, False, "no", False, True),
    "codex": Capabilities("sandbox_only", False, False, False, False, "partial", True, True),
    "ollama": Capabilities("construction", True, False, False, False, "no", True, True),
    "openai": Capabilities("construction", True, False, False, False, "no", False, True),
    "anthropic": Capabilities("construction", True, False, False, False, "no", False, True),
}


def check_step_against_backend(
    step: dict[str, Any], backend: str, *, agent_is_named: bool = True
) -> list[Issue]:
    """Return every reason ``step`` cannot run on ``backend`` (errors block, warnings do not)."""
    sid = step["id"]
    cap = CATALOG.get(backend)
    if cap is None:
        return [Issue("unknown_backend", f"backend {backend!r} is not in the catalog", step=sid)]
    out: list[Issue] = []

    def add(code: str, msg: str, severity: str = "error") -> None:
        out.append(Issue(code, msg, severity=severity, step=sid))  # type: ignore[arg-type]

    if step.get("permission", "read_only") == "allowlist" and not cap.allowlist:
        add(
            "backend_cannot_enforce",
            f"{backend} cannot restrict to a tool list (it would silently become read-only)",
        )
    if step.get("mcp") and not cap.mcp:
        add("backend_no_mcp", f"the SDK cannot pass MCP servers to {backend}")
    if step.get("skills") and not cap.skills:
        add("backend_no_skills", f"the SDK cannot pass skills to {backend}")
    if agent_is_named and not cap.agent_by_name:
        add("backend_no_agent_by_name", f"{backend} cannot select a named agent")
    if step.get("requires_evidence"):
        if cap.tool_results == "no":
            add("evidence_unavailable", f"{backend} emits no ToolResult events")
        elif cap.tool_results == "partial":
            add("evidence_partial", f"{backend} reports only some tool results", "warning")
    if (step.get("budget") or {}).get("tokens") and not cap.usage:
        add("usage_unavailable", f"{backend} reports no token usage")
    return out
