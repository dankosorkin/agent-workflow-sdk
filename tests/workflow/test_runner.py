from __future__ import annotations

import pytest
from agentflow import MemoryStore
from agentflow.checkpoint import MemoryCheckpointer
from agentflow.errors import BackendError, NodeError
from agentflow.workflow.runner import (
    PlanRefused,
    build_workflow_graph,
    run_plan,
    summarize,
    unsupported_features,
)
from wf_helpers import GOOD_ANALYZE, GOOD_COLLECT, FakeRunner, approve, codes, make_plan, step

PARAMS = {"window": "24h"}


@pytest.fixture
def plan():
    """The shared plan minus the declared artifact: the executor cannot produce artifacts yet."""
    p = make_plan()
    step(p, "collect").pop("artifacts")
    return p


def ready(plan, inventory):
    return approve(plan, inventory)


async def run(plan, inventory, runner, **kw):
    return await run_plan(ready(plan, inventory), inventory, runner, params=PARAMS, **kw)


def scripts(**over):
    base = {"collect": [GOOD_COLLECT], "analyze": [GOOD_ANALYZE]}
    base.update(over)
    return base


async def test_a_plan_runs_end_to_end(plan, inventory):
    runner = FakeRunner(scripts())
    outcome = await run(plan, inventory, runner, thread="t1")
    assert outcome.status == "completed" and outcome.thread == "t1"
    assert outcome.final_outputs == [
        {"step": "analyze", "field": "findings", "value": ["spike at 03:00"]}
    ]
    assert outcome.failed == [] and outcome.blocked == []
    assert outcome.steps["collect"] == {"status": "ok", "attempts": 1, "cached": False}


async def test_data_flows_into_the_prompt_and_the_contract_is_stated(plan, inventory):
    runner = FakeRunner(scripts())
    await run(plan, inventory, runner)
    analyze_prompt = dict(runner.calls)["analyze"]
    assert "Find anomalies." in analyze_prompt
    assert '"id": 1' in analyze_prompt and "24h" in analyze_prompt  # resolved bindings
    assert '"findings": <array>' in analyze_prompt  # the answer format


async def test_a_second_run_reuses_finished_work_from_the_store(plan, inventory):
    store = MemoryStore()
    first = FakeRunner(scripts())
    await run(plan, inventory, first, store=store)
    second = FakeRunner({})  # any call would fail the assertion in FakeRunner
    outcome = await run(plan, inventory, second, store=store)
    assert outcome.status == "completed" and second.calls == []
    assert all(s["cached"] for s in outcome.steps.values())


async def test_changing_a_check_misses_the_cache(plan, inventory):
    store = MemoryStore()
    await run(plan, inventory, FakeRunner(scripts()), store=store)
    step(plan, "collect")["checks"].append(
        {"kind": "has_fields", "target": "logs", "fields": ["id"]}
    )
    again = FakeRunner(scripts())
    await run(plan, inventory, again, store=store)
    assert again.count("collect") == 1  # the contract changed, so the old result is not trusted


async def test_a_rejected_answer_is_retried_with_the_reason(plan, inventory):
    runner = FakeRunner(scripts(collect=["no json here", GOOD_COLLECT]))
    outcome = await run(plan, inventory, runner)
    assert outcome.status == "completed" and outcome.steps["collect"]["attempts"] == 2
    second_prompt = [p for s, p in runner.calls if s == "collect"][1]
    assert "your previous answer was rejected" in second_prompt and "JSON object" in second_prompt


async def test_checks_that_never_pass_fail_the_step_and_block_its_dependents(plan, inventory):
    empty = '{"logs": [], "count": 0}'
    runner = FakeRunner(scripts(collect=[empty, empty, empty]))
    outcome = await run(plan, inventory, runner)
    assert (
        outcome.status == "failed" and runner.count("collect") == 3 and runner.count("analyze") == 0
    )
    assert "escalated after 3" in outcome.failed[0]["error"]
    assert outcome.blocked == [
        {"step": "analyze", "reason": "blocked by failed step(s) ['collect']"}
    ]


async def test_partial_outcome_when_only_some_results_exist(plan, inventory):
    plan["graph"]["steps"].append(
        {"id": "other", "agent": "collector", "prompt": "Independent.", "outputs": {"x": "string"}}
    )
    plan["final_outputs"].append({"step": "other", "field": "x"})
    empty = '{"logs": [], "count": 0}'
    runner = FakeRunner(scripts(collect=[empty] * 3, other=['{"x": "ok"}']))
    outcome = await run(plan, inventory, runner)
    assert outcome.status == "partial"
    assert [o["step"] for o in outcome.final_outputs] == ["other"]
    assert [b["step"] for b in outcome.blocked] == ["analyze"]


async def test_a_backend_error_fails_the_step_without_aborting_the_run(plan, inventory):
    runner = FakeRunner(scripts(collect=[BackendError("agent exploded")]))
    outcome = await run(plan, inventory, runner)
    assert outcome.status == "failed"
    assert "agent exploded" in outcome.failed[0]["error"]


async def test_an_unresolvable_binding_fails_the_step_with_a_reason(plan, inventory):
    step(plan, "analyze")["inputs"]["logs"] = {"from": "collect.output.logs", "then": ["first"]}
    # an empty list has no first element, so the binding cannot resolve at run time
    step(plan, "collect")["checks"] = []
    runner = FakeRunner(scripts(collect=['{"logs": [], "count": 0}']))
    outcome = await run(plan, inventory, runner)
    assert outcome.status == "failed" and runner.count("analyze") == 0
    assert "did not resolve" in outcome.failed[0]["error"]


# ---- refusals ----------------------------------------------------------------------------


async def test_an_unapproved_plan_is_refused(plan, inventory):
    with pytest.raises(PlanRefused) as exc:
        await run_plan(plan, inventory, FakeRunner({}), params=PARAMS)
    assert codes(exc.value.issues) == {"not_approved"}


async def test_an_edited_plan_loses_its_approval(plan, inventory):
    ready(plan, inventory)
    step(plan, "analyze")["prompt"] = "Something else."
    with pytest.raises(PlanRefused) as exc:
        await run_plan(plan, inventory, FakeRunner({}), params=PARAMS)
    assert codes(exc.value.issues) == {"approval_stale"}


async def test_parameters_are_checked(plan, inventory):
    ready(plan, inventory)
    for params, code in [
        ({}, "missing_parameter"),
        ({"window": 5}, "bad_parameter"),
        ({"window": "x", "extra": 1}, "unknown_parameter"),
    ]:
        with pytest.raises(PlanRefused) as exc:
            await run_plan(plan, inventory, FakeRunner({}), params=params)
        assert code in codes(exc.value.issues)


async def test_an_invalid_plan_is_refused_before_anything_runs(plan, inventory):
    step(plan, "analyze")["depends_on"] = ["ghost"]
    ready(plan, inventory)
    runner = FakeRunner({})
    with pytest.raises(PlanRefused) as exc:
        await run_plan(plan, inventory, runner, params=PARAMS)
    assert "unknown_dependency" in codes(exc.value.issues) and runner.calls == []


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (lambda s: s.update(may_ask={"max_questions": 1}), "unsupported_feature"),
        (lambda s: s.update(timeout=5), "unsupported_feature"),
        (lambda s: s.update(budget={"tokens": 10}), "unsupported_feature"),
        (lambda s: s.update(semantic_check={"agent": "x"}), "unsupported_feature"),
        (lambda s: s.update(artifacts=["a"]), "unsupported_feature"),
        (
            lambda s: s.update(checks=[{"kind": "command_succeeds", "argv": ["ls"]}]),
            "unsupported_check_kind",
        ),
    ],
)
async def test_features_the_executor_cannot_run_are_refused_not_ignored(
    plan, inventory, mutate, code
):
    mutate(step(plan, "collect"))
    assert code in codes(unsupported_features(plan))
    with pytest.raises(PlanRefused):
        await run(plan, inventory, FakeRunner({}))


# ---- side effects and durability -----------------------------------------------------------


async def test_a_crashed_side_effect_is_not_silently_repeated(plan, inventory):
    step(plan, "collect")["side_effects"] = True
    store = MemoryStore()
    crashed = FakeRunner(scripts(collect=[BackendError("died mid-effect")]))
    first = await run(plan, inventory, crashed, store=store)
    assert first.status == "failed"

    again = FakeRunner(scripts())
    second = await run(plan, inventory, again, store=store)
    assert second.status == "failed" and again.count("collect") == 0  # the effect was NOT redone
    assert "did not finish" in second.failed[0]["error"]


async def test_a_side_effecting_step_runs_once_when_all_goes_well(plan, inventory):
    step(plan, "collect")["side_effects"] = True
    runner = FakeRunner(scripts())
    outcome = await run(plan, inventory, runner)
    assert outcome.status == "completed" and runner.count("collect") == 1


async def test_the_attempt_counter_survives_a_crash_and_resume(plan, inventory):
    """The point of running each step in a gate sub-graph on a durable sub-thread."""
    ready(plan, inventory)
    empty = '{"logs": [], "count": 0}'
    runner = FakeRunner(scripts(collect=[empty, RuntimeError("worker died"), GOOD_COLLECT]))
    cp = MemoryCheckpointer()
    graph = build_workflow_graph(plan, inventory, runner, checkpointer=cp)

    with pytest.raises(NodeError):
        await graph.invoke({"params": dict(PARAMS)}, thread="r")
    final = await graph.resume("r")

    outcome = summarize(plan, final, "r")
    assert outcome.status == "completed"
    # three agent calls happened (one crashed) but only two ATTEMPTS were counted: the
    # counter was not reset by the crash, so a 3-attempt budget was not overrun.
    assert runner.count("collect") == 3
    assert outcome.steps["collect"]["attempts"] == 2
