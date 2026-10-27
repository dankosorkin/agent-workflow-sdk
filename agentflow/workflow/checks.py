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
import re
from collections.abc import Callable
from typing import Any

from agentflow.workflow.bindings import type_ok

__all__ = [
    "CHECKS",
    "CHECK_KEYS",
    "extract_json_object",
    "extract_question",
    "parse_output",
    "parse_path",
    "run_checks",
    "validate_check",
]


_Missing = object()
_PATH_TOKEN = re.compile(r"\.?([A-Za-z_][\w-]*)|\[(\*|\d+)\]")


def parse_path(target: Any) -> list[str | int] | None:
    """``"items[*].name"`` -> ``["items", "*", "name"]``; ``None`` when it is not a valid path.

    Only a field name, ``.key``, ``[n]`` and ``[*]`` (every item) exist: no expressions.
    """
    if not isinstance(target, str) or not target or len(target) > 200:
        return None
    tokens: list[str | int] = []
    pos = 0
    while pos < len(target):
        m = _PATH_TOKEN.match(target, pos)
        if m is None or (pos == 0 and m.group(1) is None):
            return None
        if m.group(1) is not None:
            tokens.append(m.group(1))
        else:
            tokens.append("*" if m.group(2) == "*" else int(m.group(2)))
        pos = m.end()
    return tokens


def _resolve(output: dict[str, Any], target: Any) -> tuple[list[Any], bool, str | None]:
    """The value(s) a path points at: ``(values, used [*], error)``. A step that is missing
    anywhere is an error, never an empty match."""
    tokens = parse_path(target)
    if tokens is None:
        return [], False, f"bad path {target!r}"
    wild = False
    current: list[Any] = [output]
    for i, tok in enumerate(tokens):
        nxt: list[Any] = []
        for item in current:
            if tok == "*":
                wild = True
                if not isinstance(item, list):
                    return [], wild, f"{_where(tokens, i)} is not a list"
                nxt.extend(item)
            elif isinstance(tok, int):
                if not isinstance(item, list) or tok >= len(item):
                    return [], wild, f"{_where(tokens, i + 1)} does not exist"
                nxt.append(item[tok])
            else:
                if not isinstance(item, dict) or tok not in item:
                    return [], wild, f"{_where(tokens, i + 1)} is missing"
                nxt.append(item[tok])
        current = nxt
    return current, wild, None


def _where(tokens: list[str | int], upto: int) -> str:
    out = ""
    for tok in tokens[:upto]:
        out += f"[{tok}]" if tok == "*" or isinstance(tok, int) else (f".{tok}" if out else tok)
    return out or "output"


def _is_number(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


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


def _length(value: Any) -> int | None:
    return len(value) if isinstance(value, (list, str, dict)) else None


def _count(output: dict[str, Any], check: dict[str, Any]) -> str | None:
    values, _, err = _resolve(output, check.get("target"))
    if err:
        return err
    if len(values) != 1:
        return "count needs a path that points at one value (no [*])"
    n = _length(values[0])
    if n is None:
        return f"{type(values[0]).__name__} has no length"
    if "min" in check and n < check["min"]:
        return f"count {n} is below the minimum {check['min']}"
    if "max" in check and n > check["max"]:
        return f"count {n} is above the maximum {check['max']}"
    if "equals" in check and n != check["equals"]:
        return f"count {n}, expected exactly {check['equals']}"
    if "same_count_as" in check:
        other, _, err = _resolve(output, check["same_count_as"])
        if err:
            return err
        m = _length(other[0]) if len(other) == 1 else None
        if m is None:
            return f"{check['same_count_as']!r} has no length"
        if n != m:
            return f"count {n} differs from the count of {check['same_count_as']!r} ({m})"
    return None


def _compare_one(op: str, actual: Any, expected: Any) -> bool:
    if op == "eq":
        return bool(actual == expected and _is_number(actual) == _is_number(expected))
    if op == "ne":
        return not _compare_one("eq", actual, expected)
    if op == "in":
        return isinstance(expected, list) and any(_compare_one("eq", actual, e) for e in expected)
    if op == "not_in":
        return not _compare_one("in", actual, expected)
    if op == "contains":
        if isinstance(actual, str) and isinstance(expected, str):
            return expected in actual
        return isinstance(actual, list) and any(_compare_one("eq", a, expected) for a in actual)
    if op in ("starts_with", "ends_with"):
        if not (isinstance(actual, str) and isinstance(expected, str)):
            return False
        return actual.startswith(expected) if op == "starts_with" else actual.endswith(expected)
    both_numbers = _is_number(actual) and _is_number(expected)
    both_strings = isinstance(actual, str) and isinstance(expected, str)
    if not (both_numbers or both_strings):
        return False
    return {"gt": actual > expected, "ge": actual >= expected,
            "lt": actual < expected, "le": actual <= expected}[op]  # fmt: skip


def _compare(output: dict[str, Any], check: dict[str, Any]) -> str | None:
    target, op = check.get("target"), str(check.get("op"))
    values, wild, err = _resolve(output, target)
    if op == "exists":
        return None if err is None else err
    if err:
        return err
    if "other" in check:
        other, _, err = _resolve(output, check["other"])
        if err:
            return err
        if len(other) != 1:
            return "'other' must point at one value (no [*])"
        expected = other[0]
    else:
        expected = check.get("value")
    if wild and not values and not check.get("allow_empty"):
        return f"{target!r} matched no items (set allow_empty to accept that)"
    for i, actual in enumerate(values):
        if not _compare_one(op, actual, expected):
            at = f" (item {i})" if wild else ""
            return f"{target!r}{at} is {actual!r}, which fails {op} {expected!r}"
    return None


def _unique(output: dict[str, Any], check: dict[str, Any]) -> str | None:
    values, wild, err = _resolve(output, check.get("target"))
    if err:
        return err
    if not wild and len(values) == 1 and isinstance(values[0], list):
        values = values[0]  # a list target: its items must be distinct
    seen: list[Any] = []
    for v in values:
        if any(v == s and _is_number(v) == _is_number(s) for s in seen):
            return f"duplicate value {v!r}"
        seen.append(v)
    return None


def _sorted(output: dict[str, Any], check: dict[str, Any]) -> str | None:
    values, _, err = _resolve(output, check.get("target"))
    if err:
        return err
    if len(values) != 1 or not isinstance(values[0], list):
        return "target must point at one list"
    key = check.get("key")
    keys: list[Any] = []
    for i, item in enumerate(values[0]):
        got, _, err = _resolve({"v": item}, "v" + (f".{key}" if key else ""))
        if err or len(got) != 1:
            return f"item {i}: cannot read the sort key"
        keys.append(got[0])
    desc = check.get("order") == "desc"
    for a, b in zip(keys, keys[1:], strict=False):
        ok = (_is_number(a) and _is_number(b)) or (isinstance(a, str) and isinstance(b, str))
        if not ok:
            return f"sort keys {a!r} and {b!r} cannot be ordered"
        if (a < b) if desc else (a > b):
            return f"not in {'descending' if desc else 'ascending'} order: {a!r} then {b!r}"
    return None


# kind -> function returning a failure reason, or None when the check passes
CHECKS: dict[str, Callable[[dict[str, Any], dict[str, Any]], str | None]] = {
    "not_empty": _not_empty,
    "has_fields": _has_fields,
    "count": _count,
    "compare": _compare,
    "unique": _unique,
    "sorted": _sorted,
}

#: kind -> (required keys, optional keys), besides ``kind`` itself.
CHECK_KEYS: dict[str, tuple[frozenset[str], frozenset[str]]] = {
    "not_empty": (frozenset({"target"}), frozenset()),
    "has_fields": (frozenset({"target", "fields"}), frozenset()),
    "count": (
        frozenset({"target"}),
        frozenset({"min", "max", "equals", "same_count_as"}),
    ),
    "compare": (frozenset({"target", "op"}), frozenset({"value", "other", "allow_empty"})),
    "unique": (frozenset({"target"}), frozenset()),
    "sorted": (frozenset({"target"}), frozenset({"key", "order"})),
}
COMPARE_OPS = frozenset(
    {"eq", "ne", "gt", "ge", "lt", "le", "in", "not_in", "contains", "starts_with", "ends_with",
     "exists"}
)  # fmt: skip


def validate_check(check: dict[str, Any], outputs: dict[str, Any]) -> list[str]:
    """Problems with a check's own parameters (empty when it is well formed or the kind is
    unknown; an unknown kind is reported elsewhere). ``outputs`` is the step's declared
    output fields: a check must look at something the step actually returns."""
    kind = check.get("kind")
    if kind not in CHECK_KEYS:
        return []
    required, optional = CHECK_KEYS[str(kind)]
    problems = [f"unknown key {k!r}" for k in sorted(set(check) - required - optional - {"kind"})]
    problems += [f"missing {k!r}" for k in sorted(required - set(check))]
    for key in ("target", "same_count_as", "other"):
        if key not in check:
            continue
        path = parse_path(check[key])
        if path is None:
            problems.append(f"{key} {check[key]!r} is not a valid path")
        elif path[0] not in outputs:
            problems.append(f"{key} {check[key]!r} is not an output field of the step")
    if kind == "has_fields" and (not isinstance(check.get("fields"), list) or not check["fields"]):
        problems.append("'fields' must be a non-empty list of names")
    for key in ("min", "max", "equals"):
        if key in check and (not isinstance(check[key], int) or isinstance(check[key], bool)):
            problems.append(f"{key!r} must be an integer")
    if kind == "count" and not ({"min", "max", "equals", "same_count_as"} & set(check)):
        problems.append("count needs at least one of min, max, equals, same_count_as")
    if kind == "compare":
        op = check.get("op")
        if op not in COMPARE_OPS:
            problems.append(f"op must be one of {sorted(COMPARE_OPS)}")
        elif op == "exists":
            if "value" in check or "other" in check:
                problems.append("'exists' takes no value")
        elif ("value" in check) == ("other" in check):
            problems.append("give exactly one of 'value' and 'other'")
        elif op in ("in", "not_in") and not isinstance(check.get("value"), list):
            problems.append(f"'{op}' needs a list in 'value'")
    if kind == "sorted" and check.get("order", "asc") not in ("asc", "desc"):
        problems.append("order must be 'asc' or 'desc'")
    return problems


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
