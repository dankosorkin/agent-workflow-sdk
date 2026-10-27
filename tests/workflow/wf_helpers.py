from __future__ import annotations

import copy
from typing import Any


def make_plan() -> dict[str, Any]:
    return {
        "schema_version": "1",
        "plan_version": 1,
        "parameters": {"window": "string"},
        "graph": {
            "steps": [
                {
                    "id": "collect",
                    "agent": "collector",
                    "prompt": "Collect the raw log records.",
                    "outputs": {"logs": "array", "count": "integer"},
                    "artifacts": ["dump"],
                    "checks": [{"kind": "not_empty", "target": "logs"}],
                },
                {
                    "id": "analyze",
                    "agent": "analyst",
                    "prompt": "Find anomalies.",
                    "depends_on": ["collect"],
                    "inputs": {
                        "logs": {"from": "collect.output.logs", "type": "array"},
                        "window": {"from": "$input.window", "type": "string"},
                    },
                    "outputs": {"findings": "array"},
                    "checks": [{"kind": "not_empty", "target": "findings"}],
                },
            ]
        },
        "final_outputs": [{"step": "analyze", "field": "findings"}],
    }


def make_inventory() -> dict[str, Any]:
    return {
        "agents": {
            "collector": {
                "description": "reads logs",
                "permissions": ["read_only", "allowlist"],
                "backends": [{"backend": "kiro", "agent": "wf-collector"}],
            },
            "analyst": {
                "description": "analyses data",
                "permissions": ["read_only"],
                "backends": [
                    {"backend": "codex", "agent": None},
                    {"backend": "claude_code", "agent": "wf-analyst"},
                ],
            },
        }
    }


def codes(issues) -> set[str]:
    return {i.code for i in issues}


def step(plan: dict[str, Any], sid: str) -> dict[str, Any]:
    return next(s for s in plan["graph"]["steps"] if s["id"] == sid)


def clone(v):
    return copy.deepcopy(v)
