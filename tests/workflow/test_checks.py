from __future__ import annotations

import pytest
from agentflow.backends import AllowAll, DenyAll, ToolAllowlist
from agentflow.backends.claude_code import ClaudeCodeBackend
from agentflow.backends.codex import CodexBackend
from agentflow.backends.kiro import KiroBackend
from agentflow.workflow.checks import extract_json_object, parse_output, run_checks
from agentflow.workflow.runner import make_backend


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ('{"a": 1}', {"a": 1}),
        ('Sure!\n{"a": 1}', {"a": 1}),
        ('```json\n{"a": {"b": [1, 2]}}\n```', {"a": {"b": [1, 2]}}),
        ('first {"a": 1} then {"b": 2}', {"b": 2}),
        ('{"a": 1} trailing words', None),
        ("no json", None),
        ("[1, 2]", None),
        ('{"broken": ', None),
    ],
)
def test_extract_json_object(text, expected):
    assert extract_json_object(text) == expected


def test_parse_output_enforces_the_contract():
    outputs = {"n": "integer", "tags": "array"}
    ok, why = parse_output('{"n": 3, "tags": [], "extra": 1}', outputs)
    assert ok == {"n": 3, "tags": []} and why == []  # extra fields are dropped
    bad, why = parse_output('{"n": "3"}', outputs)
    assert bad is None and len(why) == 2  # wrong type AND a missing field, both reported
    assert parse_output('{"n": true, "tags": []}', outputs)[0] is None  # bool is not an integer


def test_checks_are_strict():
    assert run_checks({"x": 0}, [{"kind": "not_empty", "target": "x"}]) == (True, [])
    ok, why = run_checks({}, [{"kind": "not_empty", "target": "x"}])
    assert not ok and "missing" in why[0]
    assert not run_checks({"x": []}, [{"kind": "not_empty", "target": "x"}])[0]
    fields = {"kind": "has_fields", "target": "r", "fields": ["id"]}
    assert run_checks({"r": [{"id": 1}]}, [fields])[0]
    assert not run_checks({"r": [{"id": 1}, {}]}, [fields])[0]
    assert not run_checks({"r": ["x"]}, [fields])[0]
    assert not run_checks({"r": "x"}, [fields])[0]
    assert "unsupported check kind" in run_checks({}, [{"kind": "nope"}])[1][0]


@pytest.mark.parametrize(
    ("permission", "tools", "policy"),
    [("read_only", [], DenyAll), ("allowlist", ["Read"], ToolAllowlist), ("full", [], AllowAll)],
)
def test_the_step_permission_becomes_the_backend_policy(permission, tools, policy):
    step = {"id": "s", "permission": permission, "tools": tools}
    backend = make_backend({"backend": "codex", "agent": None}, step)
    assert isinstance(backend, CodexBackend) and isinstance(backend.permission, policy)


def test_backend_classes_follow_the_inventory_entry():
    step = {"id": "s"}
    assert isinstance(make_backend({"backend": "kiro", "agent": "wf-x"}, step), KiroBackend)
    claude = make_backend({"backend": "claude_code", "agent": "wf-x"}, step)
    assert isinstance(claude, ClaudeCodeBackend) and claude.agent == "wf-x"
    with pytest.raises(NotImplementedError):
        make_backend({"backend": "ollama", "agent": None}, step)
