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

"""Deterministic result checks and the parsing of an agent's structured answer.

Only the primitives the executor can run live in ``CHECKS``. A plan that names any
other kind is reported by :func:`agentflow.workflow.runner.unsupported_features`
rather than silently passing. Failures are STRICT: a missing target is a failure,
and ``0`` / ``False`` are present values, not "empty".
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from agentflow.workflow.bindings import type_ok

__all__ = ["CHECKS", "extract_json_object", "extract_question", "parse_output", "run_checks"]


def _not_empty(output: dict[str, Any], check: dict[str, Any]) -> str | None:
    target = check.get("target")
    if target not in output:
        return f"target {target!r} missing"
    value = output[target]
    if value is None or (hasattr(value, "__len__") and len(value) == 0):
        return f"target {target!r} is empty"
    return None


def _has_fields(output: dict[str, Any], check: dict[str, Any]) -> str | None:
    target = check.get("target")
    if target not in output:
        return f"target {target!r} missing"
    items = output[target]
    if not isinstance(items, list):
        return f"target {target!r} is not a list"
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            return f"item {i} is not an object"
        missing = [f for f in check.get("fields") or [] if f not in item]
        if missing:
            return f"item {i} missing fields {missing}"
    return None


# kind -> function returning a failure reason, or None when the check passes
CHECKS: dict[str, Callable[[dict[str, Any], dict[str, Any]], str | None]] = {
    "not_empty": _not_empty,
    "has_fields": _has_fields,
}


def run_checks(output: dict[str, Any], checks: list[dict[str, Any]]) -> tuple[bool, list[str]]:
    """Run declarative checks; return (all passed, structured failure reasons)."""
    reasons: list[str] = []
    for check in checks:
        fn = CHECKS.get(check["kind"])
        if fn is None:
            reasons.append(f"{check['kind']}: unsupported check kind")
            continue
        why = fn(output, check)
        if why:
            reasons.append(f"{check['kind']}({check.get('target')}): {why}")
    return (not reasons), reasons


def extract_json_object(text: str) -> dict[str, Any] | None:
    """The last JSON object in ``text`` (whole text, a fenced block, or trailing prose)."""
    decoder = json.JSONDecoder()
    starts = [i for i, ch in enumerate(text) if ch == "{"]
    for i in reversed(starts):
        try:
            obj, end = decoder.raw_decode(text, i)
        except ValueError:
            continue
        rest = text[end:].strip()
        if isinstance(obj, dict) and rest.strip("`").strip() == "":
            return obj
    return None


def parse_output(text: str, outputs: dict[str, str]) -> tuple[dict[str, Any] | None, list[str]]:
    """Parse an agent's final text against a step's ``outputs`` contract.

    Returns ``(fields, reasons)``; ``fields`` is ``None`` when the contract is not met.
    Extra fields the agent adds are dropped: only the declared contract flows downstream.
    """
    obj = extract_json_object(text)
    if obj is None:
        return None, ["the answer must end with a single JSON object"]
    reasons = []
    fields: dict[str, Any] = {}
    for name, expected in outputs.items():
        if name not in obj:
            reasons.append(f"output field {name!r} is missing")
        elif not type_ok(obj[name], expected):
            reasons.append(
                f"output field {name!r} must be {expected}, got {type(obj[name]).__name__}"
            )
        else:
            fields[name] = obj[name]
    return (None, reasons) if reasons else (fields, [])


def extract_question(text: str) -> dict[str, Any] | None:
    """A clarifying question the agent asked instead of answering, or ``None``.

    The agent signals it with ``{"needs_clarification": {"question": str, "options": [str]}}``.
    ``options`` is optional (free text when absent or empty).
    """
    obj = extract_json_object(text)
    ask = obj.get("needs_clarification") if obj else None
    if not isinstance(ask, dict) or not isinstance(ask.get("question"), str) or not ask["question"]:
        return None
    options = ask.get("options") or []
    if not isinstance(options, list) or any(not isinstance(o, str) for o in options):
        return None
    return {"question": ask["question"], "options": options}
