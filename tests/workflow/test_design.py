from __future__ import annotations

import json

import pytest
from agentflow.workflow import (
    approve_plan,
    check_approval,
    design_plan,
    minimum_evidence_issues,
    run_plan,
    try_auto_approve,
)
from agentflow.workflow.design_prompts import (
    creator_prompt,
    inventory_for_designers,
    verifier_prompt,
)
from agentflow.workflow.runner import PlanRefused
from wf_helpers import FakeRunner, codes

BRIEF = json.dumps(
    {
        "goal": "find anomalies in the logs",
        "context": "last 24h",
        "constraints": ["read-only"],
        "expected_results": ["a list of findings"],
        "ambiguities": [],
    }
)


def good_plan_obj():
    return {
        "parameters": {"window": "string"},
        "graph": {
            "steps": [
                {
                    "id": "collect",
                    "agent": "collector",
                    "prompt": "Collect the raw log records.",
                    "outputs": {"logs": "array"},
                    "checks": [{"kind": "has_fields", "target": "logs", "fields": ["id"]}],
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
                    "checks": [{"kind": "has_fields", "target": "findings", "fields": ["title"]}],
                },
            ]
        },
        "final_outputs": [{"step": "analyze", "field": "findings"}],
    }


def j(obj):
    return "Here you go.\n" + json.dumps(obj)


NO_ISSUES = '{"issues": []}'


def remark(code="brief_not_covered", severity="blocker", step=None, claim="x"):
    return {
        "code": code,
        "step_id": step,
        "severity": severity,
        "claim": claim,
        "evidence_needed": "more",
    }


class Designer:
    """Scripted designer: one list of answers per role, consumed in order."""

    def __init__(self, **scripts):
        self.scripts = {k: list(v) for k, v in scripts.items()}
        self.calls: list[tuple[str, str]] = []

    async def __call__(self, role: str, prompt: str) -> str:
        self.calls.append((role, prompt))
        queue = self.scripts[role]
        assert queue, f"no scripted answer left for {role}"
        return queue.pop(0)

    def count(self, role):
        return sum(1 for r, _ in self.calls if r == role)

    def prompts(self, role):
        return [p for r, p in self.calls if r == role]


async def test_a_plan_converges_and_can_be_approved_and_run(inventory):
    d = Designer(orchestrator=[BRIEF], creator=[j(good_plan_obj())], verifier=[NO_ISSUES])
    result = await design_plan("find anomalies", inventory, d)
    assert result.status == "converged" and result.issues == []
    plan = result.plan
    assert plan["schema_version"] == "1" and plan["brief"]["goal"] == "find anomalies in the logs"
    assert plan["verification"] == {"iterations": 1, "issues": []}
    assert plan["provenance"]["plan_hash"] and plan["provenance"]["inventory_digest"]
    assert [r["stage"] for r in result.rounds] == ["verifier"]

    approved = approve_plan(plan, inventory, by="daniel")
    assert approved["approval"]["status"] == "approved" and approved["approval"]["by"] == "daniel"
    assert check_approval(approved, inventory) == []

    runner = FakeRunner(
        {
            "collect": ['{"logs": [{"id": 1}]}'],
            "analyze": ['{"findings": [{"title": "spike"}]}'],
        }
    )
    outcome = await run_plan(approved, inventory, runner, params={"window": "24h"})
    assert outcome.status == "completed"
    assert outcome.final_outputs[0]["value"] == [{"title": "spike"}]


async def test_an_invalid_plan_never_costs_a_verifier_call(inventory):
    bad = good_plan_obj()
    bad["graph"]["steps"][0]["agent"] = "ghost"
    d = Designer(orchestrator=[BRIEF], creator=[j(bad), j(good_plan_obj())], verifier=[NO_ISSUES])
    result = await design_plan("x", inventory, d)
    assert result.status == "converged"
    assert d.count("verifier") == 1  # only the sound plan was reviewed
    assert result.rounds[0]["stage"] == "mechanical"
    assert "agent_missing" in [i["code"] for i in result.rounds[0]["mechanical"]]
    assert "[agent_missing]" in d.prompts("creator")[1]  # the creator was told, and shown its plan
    assert '"ghost"' in d.prompts("creator")[1]


async def test_weak_checks_are_sent_back_by_code_not_by_the_verifier(inventory):
    weak = good_plan_obj()
    weak["graph"]["steps"][0]["checks"] = [{"kind": "not_empty", "target": "logs"}]
    weak["graph"]["steps"][1]["checks"] = []
    d = Designer(orchestrator=[BRIEF], creator=[j(weak), j(good_plan_obj())], verifier=[NO_ISSUES])
    result = await design_plan("x", inventory, d)
    assert result.status == "converged" and d.count("verifier") == 1
    fed = d.prompts("creator")[1]
    assert "check_cannot_detect_failure" in fed and "'collect'" in fed and "'analyze'" in fed


def test_minimum_evidence_rule_directly():
    plan = good_plan_obj()
    assert minimum_evidence_issues(plan) == []
    plan["graph"]["steps"][0]["checks"] = []
    [issue] = minimum_evidence_issues(plan)
    assert issue["step_id"] == "collect" and issue["severity"] == "blocker"


async def test_a_verifier_blocker_goes_back_and_a_warning_travels_on(inventory):
    blocker = {"issues": [remark(claim="the report step is missing", severity="blocker")]}
    warn = remark("hidden_assumption", "warning", "collect", "assumes logs fit in memory")
    d = Designer(
        orchestrator=[BRIEF],
        creator=[j(good_plan_obj()), j(good_plan_obj())],
        verifier=[json.dumps(blocker), json.dumps({"issues": [warn]})],
    )
    result = await design_plan("x", inventory, d)
    assert result.status == "converged" and len(result.rounds) == 2
    assert "the report step is missing" in d.prompts("creator")[1]
    assert [i["severity"] for i in result.issues] == ["warning"]
    assert result.plan["verification"]["issues"][0]["claim"] == "assumes logs fit in memory"


async def test_the_loop_is_bounded_and_ends_with_the_open_blockers(inventory):
    stubborn = json.dumps({"issues": [remark(claim="still not covered")]})
    d = Designer(
        orchestrator=[BRIEF],
        creator=[j(good_plan_obj())] * 3,
        verifier=[stubborn] * 3,
    )
    result = await design_plan("x", inventory, d, max_rounds=3)
    assert result.status == "round_limit" and len(result.rounds) == 3
    assert d.count("creator") == 3 and d.count("verifier") == 3  # no fourth round
    assert result.issues[0]["claim"] == "still not covered"
    assert "human must decide" in result.reason
    assert result.plan["verification"]["iterations"] == 3


async def test_a_plan_that_never_becomes_executable_cannot_be_approved(inventory):
    bad = good_plan_obj()
    bad["graph"]["steps"][0]["agent"] = "ghost"
    d = Designer(orchestrator=[BRIEF], creator=[j(bad)] * 2, verifier=[])
    result = await design_plan("x", inventory, d, max_rounds=2)
    assert result.status == "not_executable" and d.count("verifier") == 0
    assert codes(result.mechanical) == {"agent_missing"}
    with pytest.raises(PlanRefused):
        approve_plan(result.plan, inventory, by="daniel")


async def test_unusable_creator_answers_are_retried_then_reported(inventory):
    d = Designer(
        orchestrator=[BRIEF],
        creator=["I could not decide.", '{"no": "graph"}', j(good_plan_obj())],
        verifier=[NO_ISSUES],
    )
    result = await design_plan("x", inventory, d)
    assert result.status == "converged" and len(result.rounds) == 3
    assert "unusable_answer" in d.prompts("creator")[1]

    never = Designer(orchestrator=[BRIEF], creator=["nope"] * 2, verifier=[])
    failed = await design_plan("x", inventory, never, max_rounds=2)
    assert failed.status == "failed" and failed.plan is None
    assert "never produced a plan" in failed.reason


async def test_no_brief_means_no_design(inventory):
    d = Designer(orchestrator=["I refuse", '{"goal": ""}'], creator=[], verifier=[])
    result = await design_plan("x", inventory, d)
    assert result.status == "failed" and d.count("orchestrator") == 2 and d.count("creator") == 0


async def test_an_unusable_verifier_is_flagged_not_trusted(inventory):
    d = Designer(orchestrator=[BRIEF], creator=[j(good_plan_obj())], verifier=["looks fine to me"])
    result = await design_plan("x", inventory, d)
    assert result.status == "converged"
    assert "NOT reviewed" in result.issues[0]["claim"] and result.issues[0]["severity"] == "warning"


async def test_malformed_and_unknown_remarks_are_kept_visible_as_warnings(inventory):
    odd = {"issues": [{"code": "made_up", "severity": "blocker", "claim": "boo"}]}
    d = Designer(orchestrator=[BRIEF], creator=[j(good_plan_obj())], verifier=[json.dumps(odd)])
    result = await design_plan("x", inventory, d)
    assert result.status == "converged"  # a malformed remark cannot block...
    assert "does not fit the format" in result.issues[0]["claim"]  # ...but it is never lost


async def test_fields_the_creator_may_not_set_are_dropped(inventory):
    sneaky = good_plan_obj()
    sneaky["approval"] = {"mode": "auto", "status": "approved", "plan_hash": "x"}
    sneaky["provenance"] = {"plan_hash": "x"}
    sneaky["verification"] = {"iterations": 0, "issues": []}
    d = Designer(orchestrator=[BRIEF], creator=[j(sneaky)], verifier=[NO_ISSUES])
    plan = (await design_plan("x", inventory, d)).plan
    assert "approval" not in plan
    assert plan["provenance"]["plan_hash"] != "x"
    assert plan["verification"]["iterations"] == 1


async def test_max_rounds_must_be_positive(inventory):
    with pytest.raises(ValueError):
        await design_plan("x", inventory, Designer(), max_rounds=0)


# ---- approval -------------------------------------------------------------------------------


async def _converged(inventory, **edit):
    obj = good_plan_obj()
    for fn in edit.values():
        fn(obj)
    d = Designer(orchestrator=[BRIEF], creator=[j(obj)], verifier=[NO_ISSUES])
    return (await design_plan("x", inventory, d)).plan


async def test_editing_an_approved_plan_revokes_the_approval(inventory):
    approved = approve_plan(await _converged(inventory), inventory, by="d")
    approved["graph"]["steps"][0]["prompt"] = "something sneaky"
    assert codes(check_approval(approved, inventory)) == {"approval_stale"}


async def test_auto_approval_for_a_read_only_plan(inventory):
    plan = await _converged(inventory)
    approved, reasons = try_auto_approve(plan, inventory)
    assert reasons == [] and approved["approval"]["mode"] == "auto"
    assert check_approval(approved, inventory) == []


async def test_auto_approval_is_refused_for_full_permission_or_open_blockers(inventory):
    inventory["agents"]["collector"]["permissions"].append("full")
    plan = await _converged(inventory)
    plan["graph"]["steps"][0]["permission"] = "full"
    approved, reasons = try_auto_approve(plan, inventory)
    assert approved is None and any("full" in r for r in reasons)

    plan = await _converged(inventory)
    plan["verification"]["issues"] = [remark()]
    approved, reasons = try_auto_approve(plan, inventory)
    assert approved is None and any("verifier blocker" in r for r in reasons)


# ---- prompts ----------------------------------------------------------------------------------


def test_designers_never_see_backends_or_paths(inventory):
    view = inventory_for_designers(inventory)
    assert set(view["agents"]["collector"]) == {"description", "permissions"}
    assert "kiro" not in json.dumps(view) and "wf-collector" not in json.dumps(view)


def test_prompts_are_deterministic_and_carry_their_inputs(inventory):
    brief = json.loads(BRIEF)
    a = creator_prompt(brief, inventory, feedback=["- [x] fix it"])
    assert a == creator_prompt(brief, inventory, feedback=["- [x] fix it"])
    assert "fix it" in a and "collector" in a and "has_fields" in a
    assert "skills, mcp" in a  # tells the creator what not to use
    v = verifier_prompt(brief, inventory, good_plan_obj())
    for code in (
        "brief_not_covered",
        "check_cannot_detect_failure",
        "hidden_assumption",
        "agent_missing",
    ):
        assert code in v
