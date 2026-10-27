from __future__ import annotations

import json

import pytest
from agentflow.workflow import (
    inventory_digest,
    load_inventory,
    resolve_backend,
    resolve_backends,
    validate_inventory,
    validate_plan,
)
from wf_helpers import clone, codes, step


def test_valid_inventory(inventory):
    assert validate_inventory(inventory) == []


@pytest.mark.parametrize(
    "bad",
    [
        None,
        [],
        {},
        {"agents": []},
        {"agents": {"a": "x"}},
        {"agents": {"a": {"permissions": [], "backends": [{"backend": "kiro"}]}}},
        {"agents": {"a": {"permissions": ["root"], "backends": [{"backend": "kiro"}]}}},
        {"agents": {"a": {"permissions": ["read_only"], "backends": []}}},
        {"agents": {"a": {"permissions": ["read_only"], "backends": [{"backend": "nope"}]}}},
    ],
)
def test_bad_inventories_are_reported(bad):
    assert validate_inventory(bad)


def test_missing_agent(plan, inventory):
    del inventory["agents"]["collector"]
    assert "agent_missing" in codes(validate_plan(plan, inventory=inventory))


def test_permission_must_be_allowed_for_the_agent(plan, inventory):
    step(plan, "analyze")["permission"] = "full"
    assert "permission_not_allowed" in codes(validate_plan(plan, inventory=inventory))


def test_first_executable_backend_wins(plan, inventory):
    entry, found = resolve_backend(step(plan, "analyze"), inventory)
    assert entry == {"backend": "codex", "agent": None} and found == []


def test_falls_through_to_a_later_backend(plan, inventory):
    s = step(plan, "analyze")
    s["requires_evidence"] = True  # codex only reports some tool results (a warning)...
    entry, found = resolve_backend(s, inventory)
    assert entry["backend"] == "codex" and [i.code for i in found] == ["evidence_partial"]
    inventory["agents"]["analyst"]["backends"] = [
        {"backend": "claude_code", "agent": "wf-analyst"},  # no ToolResult at all: an error
        {"backend": "kiro", "agent": "wf-analyst"},
    ]
    entry, _ = resolve_backend(s, inventory)
    assert entry["backend"] == "kiro"


def test_a_plan_no_backend_can_run_is_an_error(plan, inventory):
    s = step(plan, "analyze")
    s["requires_evidence"] = True
    inventory["agents"]["analyst"]["backends"] = [{"backend": "claude_code", "agent": "x"}]
    issues = validate_plan(plan, inventory=inventory)
    assert [i.code for i in issues if i.severity == "error"] == ["no_executable_backend"]
    assert "evidence_unavailable" in next(i.message for i in issues)


def test_allowlist_is_not_accepted_on_codex(plan, inventory):
    s = step(plan, "analyze")
    s["permission"], s["tools"] = "allowlist", ["x"]
    inventory["agents"]["analyst"]["permissions"].append("allowlist")
    inventory["agents"]["analyst"]["backends"] = [{"backend": "codex", "agent": None}]
    assert "no_executable_backend" in codes(validate_plan(plan, inventory=inventory))


def test_resolve_backends_lists_only_resolvable_steps(plan, inventory):
    del inventory["agents"]["analyst"]
    assert list(resolve_backends(plan, inventory)) == ["collect"]


def test_digest_tracks_content(inventory):
    other = clone(inventory)
    assert inventory_digest(inventory) == inventory_digest(other)
    other["agents"]["collector"]["permissions"].append("full")
    assert inventory_digest(inventory) != inventory_digest(other)


def test_load_inventory_merges_a_directory_and_rejects_duplicates(tmp_path, inventory):
    (tmp_path / "a.json").write_text(
        json.dumps({"agents": {"collector": inventory["agents"]["collector"]}})
    )
    (tmp_path / "b.json").write_text(
        json.dumps({"agents": {"analyst": inventory["agents"]["analyst"]}})
    )
    assert load_inventory(tmp_path) == inventory
    (tmp_path / "c.json").write_text(json.dumps({"agents": {"analyst": {}}}))
    with pytest.raises(ValueError, match="defined twice"):
        load_inventory(tmp_path)
