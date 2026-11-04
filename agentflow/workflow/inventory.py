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

"""The agent inventory: which agents exist, what they may do, and where they run.

The plan names agents; the inventory (environment, not plan) says which backend
serves each one and which permission levels are allowed. Read-only in v1: a
change of content changes :func:`inventory_digest`, which invalidates approvals.

    {
      "agents": {
        "researcher": {
          "description": "reads code, never writes",
          "permissions": ["read_only", "allowlist"],
          "backends": [{"backend": "kiro", "agent": "wf-researcher"}]
        }
      }
    }

``backends`` is an ordered preference list. The first backend on which a step is
executable is used; there is no smarter selection in v1.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from agentflow.prebuilt import artifact_key
from agentflow.workflow.capabilities import CATALOG, PERMISSIONS, check_step_against_backend
from agentflow.workflow.issues import Issue, has_errors

__all__ = [
    "inventory_digest",
    "load_inventory",
    "resolve_backend",
    "validate_inventory",
]


def load_inventory(path: Path | str) -> dict[str, Any]:
    """Load one ``inventory.json``, or merge every ``*.json`` in a directory by file name."""
    p = Path(path)
    if p.is_dir():
        merged: dict[str, Any] = {"agents": {}}
        for f in sorted(p.glob("*.json")):
            part = json.loads(f.read_text(encoding="utf-8"))
            for name, agent in (part.get("agents") or {}).items():
                if name in merged["agents"]:
                    raise ValueError(f"agent {name!r} is defined twice (second time in {f.name})")
                merged["agents"][name] = agent
        return merged
    data = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("inventory must be a JSON object")
    return data


def inventory_digest(inventory: dict[str, Any]) -> str:
    """Content hash of the inventory; any change to it changes the digest.

    A provider-neutral inventory (``"neutral": true``, built by
    :func:`agentflow.workflow.environment.build_catalog`) is hashed by what the plan depends on:
    each role's description, permissions and preamble. Which backend serves the roles, and which
    extension files are bound, are chosen when a run starts, so they must not change the digest;
    an approval stays valid for the same plan on any backend.
    """
    if inventory.get("neutral"):
        inventory = {
            "neutral": True,
            "agents": {
                name: {
                    "description": agent.get("description", ""),
                    "permissions": list(agent.get("permissions") or []),
                    "preamble": agent.get("preamble", ""),
                }
                for name, agent in sorted((inventory.get("agents") or {}).items())
            },
        }
    return artifact_key(inventory)


def validate_inventory(inventory: Any) -> list[Issue]:
    if not isinstance(inventory, dict) or not isinstance(inventory.get("agents"), dict):
        return [Issue("bad_inventory", "inventory must be an object with an 'agents' object")]
    issues: list[Issue] = []
    for name, agent in inventory["agents"].items():
        if not isinstance(agent, dict):
            issues.append(Issue("bad_inventory", f"agent {name!r} must be an object"))
            continue
        perms = agent.get("permissions")
        if not isinstance(perms, list) or not perms or any(p not in PERMISSIONS for p in perms):
            issues.append(
                Issue(
                    "bad_inventory",
                    f"agent {name!r}: permissions must be a non-empty subset "
                    f"of {list(PERMISSIONS)}",
                )
            )
        backends = agent.get("backends")
        if not isinstance(backends, list) or not backends:
            issues.append(
                Issue("bad_inventory", f"agent {name!r}: backends must be a non-empty list")
            )
            continue
        for entry in backends:
            ok = isinstance(entry, dict) and isinstance(entry.get("backend"), str)
            if not ok:
                issues.append(
                    Issue("bad_inventory", f"agent {name!r}: bad backends entry {entry!r}")
                )
            elif entry["backend"] not in CATALOG:
                issues.append(
                    Issue(
                        "unknown_backend",
                        f"agent {name!r}: backend {entry['backend']!r} is not in the catalog",
                    )
                )
    return issues


def resolve_backend(
    step: dict[str, Any], inventory: dict[str, Any]
) -> tuple[dict[str, Any] | None, list[Issue]]:
    """Pick the backend entry for ``step``, or explain why none works.

    Returns ``(entry, issues)``. ``entry`` is ``{"backend", "agent"}`` or ``None``.
    ``issues`` holds warnings for the chosen backend, or the errors that ruled out
    every backend (one ``no_executable_backend`` error summarising them).
    """
    sid = step["id"]
    agent = (inventory.get("agents") or {}).get(step.get("agent"))
    if agent is None:
        return None, [
            Issue("agent_missing", f"agent {step.get('agent')!r} is not in the inventory", step=sid)
        ]
    permission = step.get("permission", "read_only")
    if permission not in (agent.get("permissions") or []):
        return None, [
            Issue(
                "permission_not_allowed",
                f"agent {step['agent']!r} is not allowed permission {permission!r} "
                f"(allowed: {agent.get('permissions')})",
                step=sid,
            )
        ]
    reasons: list[str] = []
    for entry in agent.get("backends") or []:
        found = check_step_against_backend(
            step, entry["backend"], agent_is_named=bool(entry.get("agent"))
        )
        if not has_errors(found):
            return entry, found  # warnings only
        reasons.append(
            f"{entry['backend']}: " + "; ".join(i.code for i in found if i.severity == "error")
        )
    return None, [
        Issue(
            "no_executable_backend",
            "no backend can run this step (" + " | ".join(reasons) + ")",
            step=sid,
        )
    ]
