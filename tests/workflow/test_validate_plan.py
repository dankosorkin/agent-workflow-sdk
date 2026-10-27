from __future__ import annotations

import pytest
from agentflow.workflow import has_errors, validate_plan
from wf_helpers import codes, step


def test_a_good_plan_has_no_issues(plan):
    assert validate_plan(plan) == []


def test_a_good_plan_passes_with_an_inventory(plan, inventory):
    assert validate_plan(plan, inventory=inventory) == []


def test_unknown_schema_version_is_the_only_finding(plan):
    plan["schema_version"] = "2"
    issues = validate_plan(plan)
    assert [i.code for i in issues] == ["unsupported_schema_version"]


@pytest.mark.parametrize("bad", [None, [], "x", 3])
def test_a_non_object_plan_is_reported_not_raised(bad):
    assert codes(validate_plan(bad)) == {"bad_plan_shape"}


@pytest.mark.parametrize("graph", [None, {}, {"steps": []}, {"steps": "x"}])
def test_graph_must_hold_steps(plan, graph):
    plan["graph"] = graph
    assert codes(validate_plan(plan)) == {"bad_plan_shape"}


def test_all_problems_are_reported_at_once(plan):
    step(plan, "collect")["permission"] = "root"
    step(plan, "analyze")["depends_on"] = ["ghost"]
    plan["final_outputs"] = []
    assert {"bad_permission", "unknown_dependency", "no_final_outputs"} <= codes(
        validate_plan(plan)
    )


def test_malformed_step_does_not_crash_the_rest(plan):
    plan["graph"]["steps"].append("not a step")
    plan["graph"]["steps"].append({"agent": "x", "prompt": "p"})
    found = codes(validate_plan(plan))
    assert {"bad_step", "missing_field"} <= found


def test_duplicate_ids(plan):
    plan["graph"]["steps"].append(dict(step(plan, "collect")))
    assert "duplicate_id" in codes(validate_plan(plan))


def test_cycle_is_reported(plan):
    step(plan, "collect")["depends_on"] = ["analyze"]
    assert "cycle" in codes(validate_plan(plan))


def test_self_dependency_is_a_cycle(plan):
    step(plan, "collect")["depends_on"] = ["collect"]
    assert "cycle" in codes(validate_plan(plan))


@pytest.mark.parametrize(
    ("field", "value", "code"),
    [
        ("prompt", "  ", "missing_field"),
        ("agent", "", "missing_field"),
        ("max_attempts", 0, "bad_field"),
        ("max_attempts", True, "bad_field"),
        ("timeout", -1, "bad_field"),
        ("side_effects", "yes", "bad_field"),
        ("depends_on", "collect", "bad_field"),
        ("outputs", {"x": "text"}, "bad_outputs"),
        ("checks", [{"target": "x"}], "bad_checks"),
        ("may_ask", {"max_questions": 0}, "bad_may_ask"),
        ("may_ask", {"max_questions": 1, "default_answer": 3}, "bad_may_ask"),
        ("may_ask", "yes", "bad_may_ask"),
    ],
)
def test_field_rules(plan, field, value, code):
    step(plan, "collect")[field] = value
    assert code in codes(validate_plan(plan))


def test_permission_and_tools_must_agree(plan):
    s = step(plan, "collect")
    s["permission"] = "allowlist"
    assert "allowlist_empty" in codes(validate_plan(plan))
    s["tools"] = ["Read"]
    assert validate_plan(plan) == []
    s["permission"] = "read_only"
    assert "tools_without_allowlist" in codes(validate_plan(plan))


@pytest.mark.parametrize("key", ["skills", "mcp"])
def test_skills_and_mcp_are_reserved(plan, key):
    step(plan, "collect")[key] = ["something"]
    assert "reserved_field" in codes(validate_plan(plan))


def test_may_ask_is_accepted_when_well_formed(plan):
    step(plan, "collect")["may_ask"] = {"max_questions": 2, "default_answer": None}
    assert validate_plan(plan) == []


def test_final_outputs_must_resolve(plan):
    plan["final_outputs"] = [
        {"step": "ghost", "field": "x"},
        {"step": "analyze", "field": "nope"},
        "junk",
    ]
    assert {"unknown_step", "unknown_output_field", "bad_final_output"} <= codes(
        validate_plan(plan)
    )


def test_parameters_must_be_typed(plan):
    plan["parameters"] = {"window": "text"}
    assert "bad_field" in codes(validate_plan(plan))


def test_binding_errors_surface_through_validate_plan(plan):
    step(plan, "analyze")["inputs"]["logs"] = {"from": "ghost.output.logs"}
    issues = validate_plan(plan)
    assert any(
        i.code == "unknown_step" and i.step == "analyze" and i.input == "logs" for i in issues
    )


def test_has_errors_ignores_warnings(plan, inventory):
    assert not has_errors(validate_plan(plan, inventory=inventory))


def test_a_misspelt_key_is_an_error_not_silence(plan):
    step(plan, "analyze")["depend_on"] = ["collect"]
    plan["finnal_outputs"] = []
    found = [i for i in validate_plan(plan) if i.code == "unknown_key"]
    assert len(found) == 2
