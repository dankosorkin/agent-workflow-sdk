from __future__ import annotations

from agentflow.workflow import (
    auto_approval_blockers,
    check_approval,
    inventory_digest,
    plan_hash,
)
from wf_helpers import codes, step


def approve(plan, inventory):
    plan["approval"] = {"mode": "manual", "status": "approved", "plan_hash": plan_hash(plan)}
    plan["provenance"] = {"inventory_digest": inventory_digest(inventory)}


def test_hash_ignores_the_history_fields(plan, inventory):
    before = plan_hash(plan)
    approve(plan, inventory)
    plan["verification"] = {"iterations": 2, "issues": []}
    assert plan_hash(plan) == before


def test_hash_changes_with_content(plan):
    before = plan_hash(plan)
    step(plan, "collect")["prompt"] = "different"
    assert plan_hash(plan) != before


def test_a_fresh_approval_is_valid(plan, inventory):
    approve(plan, inventory)
    assert check_approval(plan, inventory) == []


def test_no_approval(plan, inventory):
    assert codes(check_approval(plan, inventory)) == {"not_approved"}
    plan["approval"] = {"mode": "manual", "status": "rejected"}
    assert codes(check_approval(plan, inventory)) == {"not_approved"}


def test_editing_the_plan_revokes_the_approval(plan, inventory):
    approve(plan, inventory)
    step(plan, "analyze")["permission"] = "full"
    assert codes(check_approval(plan, inventory)) == {"approval_stale"}


def test_changing_the_inventory_revokes_the_approval(plan, inventory):
    approve(plan, inventory)
    inventory["agents"]["collector"]["permissions"].append("full")
    assert codes(check_approval(plan, inventory)) == {"inventory_changed"}


def test_a_read_only_plan_may_be_auto_approved(plan, inventory):
    assert auto_approval_blockers(plan, inventory) == []


def test_auto_approval_blockers(plan, inventory):
    inventory["agents"]["analyst"]["permissions"].append("full")
    s = step(plan, "analyze")
    s["permission"] = "full"
    s["may_ask"] = {"max_questions": 1}
    s["checks"].append({"kind": "command_succeeds", "argv": ["rm", "-rf", "/"]})
    blockers = auto_approval_blockers(plan, inventory)
    assert any("permission 'full'" in b for b in blockers)
    assert any("may_ask without default_answer" in b for b in blockers)
    assert any("not on the allow-list" in b for b in blockers)
    assert auto_approval_blockers(plan, inventory, command_allowlist=frozenset({"rm"})) != blockers


def test_unresolvable_backend_blocks_auto_approval(plan, inventory):
    del inventory["agents"]["analyst"]
    assert any("no executable backend" in b for b in auto_approval_blockers(plan, inventory))
