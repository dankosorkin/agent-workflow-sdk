from __future__ import annotations

import pytest
from agentflow.workflow import (
    dataflow,
    render_prompt,
    resolve_inputs,
    step_cache_key,
    validate_plan,
)
from wf_helpers import codes, step


def bind(plan, name, binding, sid="analyze"):
    step(plan, sid)["inputs"][name] = binding
    return validate_plan(plan)


@pytest.mark.parametrize(
    ("binding", "code"),
    [
        ({"template": "x {{y}}"}, "unknown_binding_kind"),
        ({"from": "collect.output.logs", "bogus": 1}, "unknown_key"),
        ({"from": "collect.output.logs", "value": 1}, "unknown_binding_kind"),
        ({}, "unknown_binding_kind"),
        ("collect.output.logs", "unknown_binding_kind"),
        ({"from": "collect.logs"}, "bad_path"),
        ({"from": "$input"}, "bad_path"),
        ({"from": "$input.missing"}, "unknown_param"),
        ({"from": "ghost.output.x"}, "unknown_step"),
        ({"from": "collect.output.nope"}, "unknown_output_field"),
        ({"from": "analyze.output.findings"}, "not_ancestor"),
        ({"from": "collect.output.logs", "required": False}, "optional_needs_default"),
        ({"from": "collect.output.count", "type": "string"}, "type_mismatch"),
        ({"from": "collect.output.logs", "type": "text"}, "type_mismatch"),
        ({"from": ["collect.output.logs"]}, "combine_needs_list"),
        ({"from": "collect.output.logs", "combine": "concat"}, "bad_combine"),
        (
            {"from": ["collect.output.logs", "collect.output.count"], "combine": "concat"},
            "bad_combine",
        ),
        ({"from": "collect.output.logs", "then": ["shout"]}, "bad_transform"),
        ({"from": "collect.output.count", "then": ["unique"]}, "bad_transform"),
        ({"artifact": "collect.artifacts.nope"}, "unknown_artifact"),
        ({"artifact": "ghost.artifacts.x"}, "unknown_step"),
    ],
)
def test_binding_rejections(plan, binding, code):
    assert code in codes(bind(plan, "x", binding))


@pytest.mark.parametrize(
    "binding",
    [
        {"value": [1, 2]},
        {"from": "collect.output.logs", "then": ["first"]},
        {
            "from": "collect.output.logs",
            "then": [{"pick": "id"}, "unique", {"join": ","}],
            "type": "string",
        },
        {"from": "collect.output.logs", "then": ["len"], "type": "integer"},
        {"from": "collect.output.count", "required": False, "default": 0},
        {"artifact": "collect.artifacts.dump"},
        {
            "from": ["collect.output.logs", "collect.output.logs"],
            "combine": "concat",
            "type": "array",
        },
    ],
)
def test_binding_acceptances(plan, binding):
    assert bind(plan, "x", binding) == []


def test_ancestors_beyond_direct_prerequisites_are_allowed(plan):
    plan["graph"]["steps"].append(
        {
            "id": "report",
            "agent": "analyst",
            "prompt": "Report.",
            "depends_on": ["analyze"],
            "inputs": {"logs": {"from": "collect.output.logs"}},
            "outputs": {"summary": "string"},
        }
    )
    assert validate_plan(plan) == []
    edges = dataflow(plan["graph"]["steps"])
    assert {"from": "collect", "to": "report", "input": "logs", "direct": False} in edges


def test_resolution_and_missing_data(plan):
    results = {"collect": {"output": {"output": {"logs": [{"id": 2}, {"id": 1}], "count": 2}}}}
    s = {
        "inputs": {
            "ids": {"from": "collect.output.logs", "then": [{"pick": "id"}], "type": "array"},
            "w": {"from": "$input.window", "type": "string"},
            "opt": {"from": "collect.output.absent", "required": False, "default": "d"},
        }
    }
    values, problems = resolve_inputs(s, results, {"window": "24h"})
    assert problems == [] and values == {"ids": [2, 1], "w": "24h", "opt": "d"}

    values, problems = resolve_inputs(s, {}, {})
    assert len(problems) == 2 and values == {
        "opt": "d"
    }  # missing data fails the step, never raises


def test_wrong_runtime_type_is_a_problem():
    s = {"inputs": {"n": {"value": 5, "type": "string"}}}
    _, problems = resolve_inputs(s, {}, {})
    assert problems and "expected string" in problems[0]


def test_prompt_rendering_is_deterministic_and_static():
    s = {"prompt": "Do it."}
    a = render_prompt(s, {"b": [1], "a": {"z": 1, "y": 2}})
    b = render_prompt(s, {"a": {"y": 2, "z": 1}, "b": [1]})
    assert a == b and a.startswith("Do it.")


def test_cache_key_follows_the_resolved_inputs_only():
    s = {"agent": "a", "prompt": "p"}
    assert step_cache_key(s, {"x": 1}) == step_cache_key(s, {"x": 1})
    assert step_cache_key(s, {"x": 1}) != step_cache_key(s, {"x": 2})
