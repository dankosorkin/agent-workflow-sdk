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

"""plan.json: validation, hashing, and the approval checks.

``validate_plan`` is the deterministic validator of the design (OQ-5): it decides
whether a plan can be assembled, and reports EVERY problem at once as
:class:`~agentflow.workflow.issues.Issue` objects with stable codes. It never
raises on bad plan data. The semantic review of a plan (does it cover the brief,
can its checks detect a wrong result) is a separate step done by ``wf-verifier``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from agentflow.prebuilt import artifact_key
from agentflow.workflow.bindings import TYPES, find_cycle, validate_bindings
from agentflow.workflow.capabilities import PERMISSIONS
from agentflow.workflow.checks import validate_check
from agentflow.workflow.inventory import inventory_digest, resolve_backend
from agentflow.workflow.issues import Issue, has_errors

__all__ = [
    "SCHEMA_VERSION",
    "auto_approval_blockers",
    "check_approval",
    "load_schema",
    "plan_hash",
    "resolve_backends",
    "validate_plan",
]

SCHEMA_VERSION = "1"
# Parts of the document that describe the plan's history, not its content. They are
# excluded from the hash, otherwise an approval could never match the plan it signs.
_HASH_EXCLUDED = ("approval", "verification", "provenance")

# Unknown keys are errors, not noise: a typo such as ``depend_on`` would otherwise
# silently drop a dependency. Keep in sync with schema/plan.schema.json (a test does).
_PLAN_KEYS = {
    "schema_version",
    "plan_version",
    "brief",
    "parameters",
    "graph",
    "final_outputs",
    "run_limits",
    "budget",
    "approval",
    "verification",
    "provenance",
}
_STEP_KEYS = {
    "id",
    "agent",
    "prompt",
    "permission",
    "tools",
    "skills",
    "mcp",
    "inputs",
    "outputs",
    "artifacts",
    "side_effects",
    "requires_evidence",
    "may_ask",
    "checks",
    "semantic_check",
    "budget",
    "timeout",
    "max_attempts",
    "depends_on",
}


def load_schema() -> dict[str, Any]:
    """The packaged JSON Schema for plan.json (the UI and designer agents read it)."""
    return json.loads((Path(__file__).parent / "plan.schema.json").read_text(encoding="utf-8"))


def plan_hash(plan: dict[str, Any]) -> str:
    """Stable content hash of the plan, ignoring approval, verification and provenance."""
    return artifact_key({k: v for k, v in plan.items() if k not in _HASH_EXCLUDED})


# ---------------------------------------------------------------------------
# Structure
# ---------------------------------------------------------------------------


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _steps_of(plan: dict[str, Any]) -> list[Any] | None:
    graph = plan.get("graph")
    steps = graph.get("steps") if isinstance(graph, dict) else None
    return steps if isinstance(steps, list) and steps else None


def _check_step_shape(s: Any, index: int, issues: list[Issue]) -> bool:
    """Structural checks for one step. Returns False if the step is unusable."""
    if not isinstance(s, dict):
        issues.append(Issue("bad_step", f"steps[{index}] must be an object"))
        return False
    sid = s.get("id")
    if not isinstance(sid, str) or not sid:
        issues.append(Issue("missing_field", f"steps[{index}]: 'id' must be a non-empty string"))
        return False

    def err(code: str, message: str) -> None:
        issues.append(Issue(code, message, step=sid))

    unknown = sorted(set(s) - _STEP_KEYS)
    if unknown:
        err("unknown_key", f"unknown step key(s) {unknown}")
    for key in ("agent", "prompt"):
        if not isinstance(s.get(key), str) or not s[key].strip():
            err("missing_field", f"'{key}' must be a non-empty string")
    permission = s.get("permission", "read_only")
    if permission not in PERMISSIONS:
        err("bad_permission", f"permission must be one of {list(PERMISSIONS)}, got {permission!r}")
    tools = s.get("tools") or []
    if not isinstance(tools, list) or any(not isinstance(t, str) for t in tools):
        err("bad_field", "'tools' must be a list of strings")
    elif permission == "allowlist" and not tools:
        err("allowlist_empty", "permission 'allowlist' needs at least one tool")
    elif permission != "allowlist" and tools:
        err("tools_without_allowlist", "'tools' is only meaningful with permission 'allowlist'")
    for key in ("skills", "mcp"):
        if s.get(key):
            err("reserved_field", f"'{key}' is reserved: the SDK cannot pass it to a backend yet")
    deps = s.get("depends_on", [])
    if not isinstance(deps, list) or any(not isinstance(d, str) for d in deps):
        err("bad_field", "'depends_on' must be a list of step ids")
    if s.get("inputs") is not None and not isinstance(s["inputs"], dict):
        err("bad_field", "'inputs' must be an object")
    outputs = s.get("outputs", {})
    if not isinstance(outputs, dict) or any(t not in TYPES for t in outputs.values()):
        err("bad_outputs", f"'outputs' must map field names to one of {sorted(TYPES)}")
    checks = s.get("checks", [])
    if not isinstance(checks, list) or any(
        not isinstance(c, dict) or not isinstance(c.get("kind"), str) for c in checks
    ):
        err("bad_checks", "'checks' must be a list of objects, each with a string 'kind'")
    elif isinstance(outputs, dict):
        for c in checks:
            for problem in validate_check(c, outputs):
                err("bad_check", f"{c['kind']}: {problem}")
    attempts = s.get("max_attempts", 3)
    if not _is_int(attempts) or attempts < 1:
        err("bad_field", "'max_attempts' must be an integer >= 1")
    timeout = s.get("timeout")
    if timeout is not None and (
        isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0
    ):
        err("bad_field", "'timeout' must be a positive number or null")
    if not isinstance(s.get("side_effects", False), bool):
        err("bad_field", "'side_effects' must be a boolean")
    ask = s.get("may_ask")
    if ask is not None and (
        not isinstance(ask, dict)
        or not _is_int(ask.get("max_questions"))
        or ask["max_questions"] < 1
        or not isinstance(ask.get("default_answer"), (str, type(None)))
    ):
        err(
            "bad_may_ask",
            "'may_ask' must be null or {max_questions: int >= 1, default_answer: string|null}",
        )
    return True


def validate_plan(plan: Any, *, inventory: dict[str, Any] | None = None) -> list[Issue]:
    """Validate a plan; returns every issue (errors block assembly, warnings do not).

    Without ``inventory`` the checks are structural and graph-level. With it, agents,
    permissions and backends are resolved too (the full pre-approval check).
    """
    if not isinstance(plan, dict):
        return [Issue("bad_plan_shape", "plan must be a JSON object")]
    version = plan.get("schema_version")
    if version != SCHEMA_VERSION:
        return [
            Issue(
                "unsupported_schema_version",
                f"schema_version {version!r} is not supported (this validator reads "
                f"{SCHEMA_VERSION!r})",
            )
        ]
    steps = _steps_of(plan)
    if steps is None:
        return [Issue("bad_plan_shape", "plan.graph.steps must be a non-empty list")]

    issues: list[Issue] = []
    unknown_top = sorted(set(plan) - _PLAN_KEYS)
    if unknown_top:
        issues.append(Issue("unknown_key", f"unknown plan key(s) {unknown_top}"))
    params = plan.get("parameters", {})
    if not isinstance(params, dict) or any(t not in TYPES for t in params.values()):
        issues.append(Issue("bad_field", f"'parameters' must map names to one of {sorted(TYPES)}"))
        params = {}
    limits = plan.get("run_limits")
    if limits is not None and (
        not isinstance(limits, dict)
        or not _is_int(limits.get("max_design_rounds", 3))
        or limits.get("max_design_rounds", 3) < 1
    ):
        issues.append(Issue("bad_field", "'run_limits.max_design_rounds' must be an integer >= 1"))

    usable = [s for i, s in enumerate(steps) if _check_step_shape(s, i, issues)]
    by_id: dict[str, dict[str, Any]] = {}
    for s in usable:
        if s["id"] in by_id:
            issues.append(Issue("duplicate_id", f"step id {s['id']!r} is used twice", step=s["id"]))
        by_id.setdefault(s["id"], s)

    deps: dict[str, list[str]] = {}
    for sid, s in by_id.items():
        raw = s.get("depends_on", [])
        deps[sid] = [d for d in raw if isinstance(d, str)] if isinstance(raw, list) else []
    for sid, ds in deps.items():
        for d in ds:
            if d not in by_id:
                issues.append(
                    Issue("unknown_dependency", f"depends on unknown step {d!r}", step=sid)
                )
    cycle = find_cycle({sid: [d for d in ds if d in by_id] for sid, ds in deps.items()})
    if cycle:
        issues.append(Issue("cycle", f"dependency cycle among {cycle}", step=",".join(cycle)))

    issues += validate_bindings(list(by_id.values()), params, deps)
    issues += _check_final_outputs(plan, by_id)

    if inventory is not None:
        for s in by_id.values():
            _, found = resolve_backend(s, inventory)
            issues += found
    return issues


def _check_final_outputs(plan: dict[str, Any], by_id: dict[str, dict[str, Any]]) -> list[Issue]:
    finals = plan.get("final_outputs")
    if not isinstance(finals, list) or not finals:
        return [Issue("no_final_outputs", "'final_outputs' must list at least one result")]
    out: list[Issue] = []
    for ref in finals:
        if (
            not isinstance(ref, dict)
            or not isinstance(ref.get("step"), str)
            or not isinstance(ref.get("field"), str)
        ):
            out.append(Issue("bad_final_output", f"bad final_outputs entry {ref!r}"))
            continue
        step = by_id.get(ref["step"])
        if step is None:
            out.append(
                Issue("unknown_step", f"final output refers to unknown step {ref['step']!r}")
            )
        elif ref["field"] not in (step.get("outputs") or {}):
            out.append(
                Issue(
                    "unknown_output_field",
                    f"{ref['step']!r} declares no output {ref['field']!r}",
                    step=ref["step"],
                )
            )
    return out


# ---------------------------------------------------------------------------
# Backends, approval
# ---------------------------------------------------------------------------


def resolve_backends(plan: dict[str, Any], inventory: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """step id -> chosen ``{"backend", "agent"}`` entry, for steps that have one."""
    chosen: dict[str, dict[str, Any]] = {}
    for s in _steps_of(plan) or []:
        if isinstance(s, dict) and isinstance(s.get("id"), str):
            entry, _ = resolve_backend(s, inventory)
            if entry is not None:
                chosen[s["id"]] = entry
    return chosen


def auto_approval_blockers(
    plan: dict[str, Any],
    inventory: dict[str, Any],
    *,
    command_allowlist: frozenset[str] = frozenset(),
) -> list[str]:
    """Reasons automatic approval is NOT allowed (empty means it is). OQ-4 threshold.

    All of these must hold: no step with permission ``full``; every ``command_succeeds``
    check runs only allow-listed executables; every step resolves to a backend; no
    ``may_ask`` without a ``default_answer``.
    """
    reasons: list[str] = []
    for s in _steps_of(plan) or []:
        if not isinstance(s, dict):
            continue
        sid = s.get("id")
        if s.get("permission") == "full":
            reasons.append(f"{sid}: permission 'full'")
        for c in s.get("checks") or []:
            if isinstance(c, dict) and c.get("kind") == "command_succeeds":
                argv = c.get("argv") or []
                if not argv or argv[0] not in command_allowlist:
                    reasons.append(f"{sid}: command {argv[:1]} is not on the allow-list")
        ask = s.get("may_ask")
        if isinstance(ask, dict) and ask.get("default_answer") is None:
            reasons.append(f"{sid}: may_ask without default_answer")
        entry, found = resolve_backend(s, inventory)
        if entry is None or has_errors(found):
            reasons.append(f"{sid}: no executable backend")
    return reasons


def check_approval(plan: dict[str, Any], inventory: dict[str, Any]) -> list[Issue]:
    """Is the approval on this plan still valid for this exact plan and inventory?"""
    approval = plan.get("approval")
    if not isinstance(approval, dict) or approval.get("status") != "approved":
        return [Issue("not_approved", "the plan has no approval")]
    issues: list[Issue] = []
    if approval.get("plan_hash") != plan_hash(plan):
        issues.append(Issue("approval_stale", "the plan changed after it was approved"))
    digest = (plan.get("provenance") or {}).get("inventory_digest")
    if digest != inventory_digest(inventory):
        issues.append(
            Issue("inventory_changed", "the inventory changed after the plan was approved")
        )
    return issues
