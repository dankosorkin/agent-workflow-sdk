"""The JSON Schema and validate_plan must agree on STRUCTURE (graph rules are validate_plan's)."""

from __future__ import annotations

import jsonschema
import pytest
from agentflow.workflow import load_schema, validate_plan
from wf_helpers import clone, step

SCHEMA = load_schema()
VALIDATOR = jsonschema.Draft202012Validator(SCHEMA)


def schema_ok(plan) -> bool:
    return VALIDATOR.is_valid(plan)


def test_the_schema_itself_is_valid():
    jsonschema.Draft202012Validator.check_schema(SCHEMA)


def test_good_plan_passes_both(plan):
    assert schema_ok(plan) and validate_plan(plan) == []


def mutate(path, value):
    def apply(plan):
        target = plan
        for key in path[:-1]:
            target = target[key] if not isinstance(key, tuple) else step(plan, key[0])
        if isinstance(path[-1], str):
            target[path[-1]] = value
        return plan

    return apply


STRUCTURAL = {
    "schema_version": lambda p: p.update(schema_version="2"),
    "no final_outputs": lambda p: p.update(final_outputs=[]),
    "unknown top-level key": lambda p: p.update(surprise=1),
    "bad permission": lambda p: step(p, "collect").update(permission="root"),
    "empty prompt": lambda p: step(p, "collect").update(prompt=""),
    "skills reserved": lambda p: step(p, "collect").update(skills=["x"]),
    "mcp reserved": lambda p: step(p, "collect").update(mcp=["x"]),
    "max_attempts zero": lambda p: step(p, "collect").update(max_attempts=0),
    "bad output type": lambda p: step(p, "collect").update(outputs={"x": "text"}),
    "bad may_ask": lambda p: step(p, "collect").update(may_ask={"max_questions": 0}),
    "unknown step key": lambda p: step(p, "collect").update(surprise=1),
    "check without kind": lambda p: step(p, "collect").update(checks=[{"target": "x"}]),
    "bad timeout": lambda p: step(p, "collect").update(timeout=0),
    "bad parameter type": lambda p: p.update(parameters={"w": "text"}),
}


@pytest.mark.parametrize("name", list(STRUCTURAL))
def test_both_reject_structural_mistakes(plan, name):
    STRUCTURAL[name](plan)
    assert not schema_ok(plan), f"schema accepted: {name}"
    assert validate_plan(plan), f"validator accepted: {name}"


def test_schema_does_not_claim_graph_rules(plan):
    """A dependency cycle is structurally fine; only validate_plan can see it."""
    step(plan, "collect")["depends_on"] = ["analyze"]
    assert schema_ok(plan)
    assert any(i.code == "cycle" for i in validate_plan(plan))


def test_approval_blocks_are_accepted_by_the_schema(plan):
    plan = clone(plan)
    plan["approval"] = {
        "mode": "manual",
        "status": "approved",
        "plan_hash": "h",
        "by": "me",
        "at": "now",
    }
    plan["verification"] = {
        "iterations": 2,
        "issues": [
            {"code": "hidden_assumption", "severity": "warning", "claim": "x", "step_id": None}
        ],
    }
    plan["provenance"] = {"inventory_digest": "d", "engine_version": "0.7.0", "plan_hash": "h"}
    assert schema_ok(plan)
