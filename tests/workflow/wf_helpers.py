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


def approve(plan: dict[str, Any], inventory: dict[str, Any]) -> dict[str, Any]:
    from agentflow.workflow import inventory_digest, plan_hash

    plan["approval"] = {"mode": "manual", "status": "approved", "plan_hash": plan_hash(plan)}
    plan["provenance"] = {"inventory_digest": inventory_digest(inventory)}
    return plan


class FakeRunner:
    """Scripted stand-in for the agent: one list of answers per step id, consumed in order."""

    def __init__(self, scripts: dict[str, list[Any]]) -> None:
        self.scripts = {k: list(v) for k, v in scripts.items()}
        self.calls: list[tuple[str, str]] = []

    async def __call__(self, step: dict[str, Any], entry: dict[str, Any], prompt: str) -> str:
        sid = step["id"]
        self.calls.append((sid, prompt))
        queue = self.scripts[sid]
        assert queue, f"no scripted answer left for {sid}"
        item = queue.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    def count(self, sid: str) -> int:
        return sum(1 for s, _ in self.calls if s == sid)


GOOD_COLLECT = 'Done.\n{"logs": [{"id": 1}, {"id": 2}], "count": 2}'
GOOD_ANALYZE = '```json\n{"findings": ["spike at 03:00"]}\n```'
