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

"""Shared helpers for the spikes in this directory (not part of the SDK).

Deterministic, model-free check primitives in miniature, strict in the same way
as ``plan_as_data_spike.py``: a missing target is a FAILURE, and ``0`` / ``False``
are present values, not "empty". Every failure returns a structured reason that
a retry can be fed.
"""

from __future__ import annotations

import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from agentflow import MemoryCheckpointer
from agentflow.checkpoint.sqlite import SqliteCheckpointer


def check_not_empty(output: dict, target: str) -> tuple[bool, str | None]:
    if target not in output:
        return False, f"target {target!r} missing"
    value = output[target]
    empty = value is None or (hasattr(value, "__len__") and len(value) == 0)
    return (not empty), None if not empty else f"target {target!r} is empty"


def check_has_fields(output: dict, target: str, fields: list[str]) -> tuple[bool, str | None]:
    if target not in output:
        return False, f"target {target!r} missing"
    items = output[target]
    if not isinstance(items, list):
        return False, f"target {target!r} is not a list"
    for i, it in enumerate(items):
        missing = [f for f in fields if f not in it]
        if missing:
            return False, f"item {i} missing fields {missing}"
    return True, None


def run_checks(output: dict, checks: list[dict[str, Any]]) -> tuple[bool, list[str]]:
    """Run declarative checks; return (all passed, structured failure reasons)."""
    reasons: list[str] = []
    for c in checks:
        kind = c["kind"]
        if kind == "not_empty":
            ok, why = check_not_empty(output, c["target"])
        elif kind == "has_fields":
            ok, why = check_has_fields(output, c["target"], c["fields"])
        else:
            ok, why = False, f"unknown check kind {kind!r}"
        if not ok and why:
            reasons.append(f"{kind}({c['target']}): {why}")
    return (not reasons), reasons


def checkpointer_factories() -> list[tuple[str, Callable[[], Any]]]:
    """Fresh checkpointers to run every scenario against.

    MemoryCheckpointer forgives non-JSON state; SqliteCheckpointer does not, so
    running on both catches serialisation mistakes the in-memory one would hide.
    The SQLite files live in a temp dir that is created per call.
    """
    tmp = tempfile.mkdtemp(prefix="spike-")
    counter = {"n": 0}

    def sqlite() -> SqliteCheckpointer:
        counter["n"] += 1
        return SqliteCheckpointer(Path(tmp) / f"cp{counter['n']}.db")

    return [("memory", MemoryCheckpointer), ("sqlite", sqlite)]
