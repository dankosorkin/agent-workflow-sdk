"""may_ask, resume, and the approval re-check, through the real executor."""

from __future__ import annotations

import pytest
from agentflow import MemoryStore
from agentflow.checkpoint import MemoryCheckpointer
from agentflow.workflow import inventory_digest
from agentflow.workflow.runner import PlanRefused, resume_plan, run_plan
from wf_helpers import GOOD_ANALYZE, GOOD_COLLECT, FakeRunner, approve, codes, make_plan, step

PARAMS = {"window": "24h"}
ASK = '{"needs_clarification": {"question": "Which env?", "options": ["staging", "prod"]}}'
ASK_FREE = '{"needs_clarification": {"question": "Which env?"}}'


@pytest.fixture
def plan():
    p = make_plan()
    step(p, "collect").pop("artifacts")
    step(p, "analyze")["may_ask"] = {"max_questions": 1}
    return p


def scripts(**over):
    base = {"collect": [GOOD_COLLECT], "analyze": [ASK, GOOD_ANALYZE]}
    base.update(over)
    return base


def rt():
    return {"store": MemoryStore(), "checkpointer": MemoryCheckpointer()}


# ---- stop mode: the default, nobody to ask -----------------------------------------------


async def test_stop_mode_fails_the_step_with_the_question(plan, inventory):
    approve(plan, inventory)
    runner = FakeRunner(scripts())
    out = await run_plan(plan, inventory, runner, params=PARAMS)
    assert out.status == "failed" and runner.count("analyze") == 1  # asked once, not re-run
    assert out.needs_clarification == [
        {
            "step": "analyze",
            "question": "Which env?",
            "options": ["staging", "prod"],
            "context": {"step": "analyze"},
        }
    ]
    assert out.failed[0]["error"] == "needs_clarification: Which env?"


async def test_answer_and_start_a_new_run_reuses_finished_work(plan, inventory):
    """The decided path: a NEW run carries the answer; finished steps come from the cache."""
    approve(plan, inventory)
    state = rt()
    first = FakeRunner(scripts())
    stopped = await run_plan(plan, inventory, first, params=PARAMS, **state)
    assert stopped.needs_clarification

    second = FakeRunner({"analyze": [GOOD_ANALYZE]})  # collect must NOT be called again
    out = await run_plan(
        plan, inventory, second, params=PARAMS, answers={"analyze": "staging"}, **state
    )
    assert out.status == "completed" and second.count("collect") == 0
    assert out.steps["collect"]["cached"] is True
    assert out.steps["analyze"]["clarifications"] == [
        {"question": None, "answer": "staging", "source": "run_parameter"}
    ]
    assert '"answer": "staging"' in second.calls[0][1]  # the answer reached the agent
    assert second.calls[0][0] == "analyze"


async def test_the_answer_changes_the_cache_key(plan, inventory):
    approve(plan, inventory)
    state = rt()
    await run_plan(plan, inventory, FakeRunner(scripts()), params=PARAMS, **state)
    a = FakeRunner({"analyze": [GOOD_ANALYZE]})
    await run_plan(plan, inventory, a, params=PARAMS, answers={"analyze": "staging"}, **state)
    b = FakeRunner({"analyze": [GOOD_ANALYZE]})
    await run_plan(plan, inventory, b, params=PARAMS, answers={"analyze": "prod"}, **state)
    assert (
        a.count("analyze") == 1 and b.count("analyze") == 1
    )  # prod did not reuse staging's result
    c = FakeRunner({})
    await run_plan(plan, inventory, c, params=PARAMS, answers={"analyze": "prod"}, **state)
    assert c.calls == []  # the same answer is a cache hit


async def test_answers_for_unknown_or_non_asking_steps_are_refused(plan, inventory):
    approve(plan, inventory)
    for sid in ("collect", "ghost"):
        with pytest.raises(PlanRefused) as exc:
            await run_plan(plan, inventory, FakeRunner({}), params=PARAMS, answers={sid: "x"})
        assert "unknown_answer" in codes(exc.value.issues)


# ---- default answer, limits ----------------------------------------------------------------


async def test_default_answer_is_used_without_asking_anyone(plan, inventory):
    step(plan, "analyze")["may_ask"] = {"max_questions": 1, "default_answer": "staging"}
    approve(plan, inventory)
    out = await run_plan(plan, inventory, FakeRunner(scripts()), params=PARAMS)
    assert out.status == "completed"
    [c] = out.steps["analyze"]["clarifications"]
    assert (
        c["answer"] == "staging"
        and c["source"] == "default_answer"
        and c["question"] == "Which env?"
    )


async def test_a_question_beyond_max_questions_is_not_allowed(plan, inventory):
    step(plan, "analyze")["may_ask"] = {"max_questions": 1, "default_answer": "staging"}
    approve(plan, inventory)
    # asks, gets the default, then asks AGAIN: not allowed, the step must answer or fail
    runner = FakeRunner(scripts(analyze=[ASK, ASK, ASK, ASK]))
    out = await run_plan(plan, inventory, runner, params=PARAMS)
    assert out.status == "failed"
    assert (
        "may not ask another question" in runner.calls[-1][1] or "rejected" in runner.calls[-1][1]
    )


async def test_a_step_without_may_ask_cannot_ask(plan, inventory):
    step(plan, "analyze").pop("may_ask")
    approve(plan, inventory)
    out = await run_plan(plan, inventory, FakeRunner(scripts(analyze=[ASK] * 3)), params=PARAMS)
    assert out.status == "failed" and out.needs_clarification == []


# ---- interrupt mode: a human is present --------------------------------------------------


async def test_a_human_answers_through_interrupt_and_resume(plan, inventory):
    approve(plan, inventory)
    state = rt()
    runner = FakeRunner(scripts())
    waiting = await run_plan(
        plan, inventory, runner, params=PARAMS, ask_mode="interrupt", thread="r", **state
    )
    assert waiting.status == "waiting"
    assert waiting.question == {
        "type": "clarification",
        "question": "Which env?",
        "options": ["staging", "prod"],
        "context": {"step": "analyze"},
    }

    out = await resume_plan(
        plan, inventory, runner, thread="r", answer="prod", ask_mode="interrupt", **state
    )
    assert out.status == "completed"
    # the probe was NOT repeated on resume: collect once, analyze twice (ask, then answer)
    assert runner.count("collect") == 1 and runner.count("analyze") == 2
    assert out.steps["analyze"]["clarifications"][0]["source"] == "human"
    assert out.steps["analyze"]["clarifications"][0]["answer"] == "prod"


async def test_resume_needs_an_answer_when_waiting(plan, inventory):
    approve(plan, inventory)
    state = rt()
    runner = FakeRunner(scripts())
    await run_plan(
        plan, inventory, runner, params=PARAMS, ask_mode="interrupt", thread="r", **state
    )
    with pytest.raises(PlanRefused) as exc:
        await resume_plan(plan, inventory, runner, thread="r", ask_mode="interrupt", **state)
    assert codes(exc.value.issues) == {"missing_answer"}


async def test_resume_of_an_unknown_thread_is_refused(plan, inventory):
    approve(plan, inventory)
    with pytest.raises(PlanRefused) as exc:
        await resume_plan(plan, inventory, FakeRunner({}), thread="nope", **rt())
    assert codes(exc.value.issues) == {"unknown_thread"}


# ---- the approval is re-checked while the run is going ------------------------------------


async def test_the_run_is_held_when_the_inventory_changes_between_steps(plan, inventory):
    approve(plan, inventory)
    state = rt()
    changed = {
        **inventory,
        "agents": {**inventory["agents"], "extra": inventory["agents"]["analyst"]},
    }
    seen = {"n": 0}

    def source():
        seen["n"] += 1  # check 1: before collect; check 2: before analyze, after collect finished
        return changed if seen["n"] >= 2 else inventory

    runner = FakeRunner(scripts(analyze=[GOOD_ANALYZE]))
    held = await run_plan(
        plan, inventory, runner, params=PARAMS, thread="h", inventory_source=source, **state
    )
    assert held.status == "held"
    assert [i["code"] for i in held.issues] == ["inventory_changed"]
    assert runner.count("collect") == 1 and runner.count("analyze") == 0


async def test_a_held_run_continues_once_the_approval_is_valid_again(plan, inventory):
    approve(plan, inventory)
    state = rt()
    changed = {
        **inventory,
        "agents": {**inventory["agents"], "extra": inventory["agents"]["analyst"]},
    }
    live = {"inv": inventory, "n": 0}

    def source():
        live["n"] += 1
        return changed if live["n"] == 2 else live["inv"]

    runner = FakeRunner(scripts(analyze=[GOOD_ANALYZE]))
    held = await run_plan(
        plan, inventory, runner, params=PARAMS, thread="h", inventory_source=source, **state
    )
    assert held.status == "held"

    out = await resume_plan(
        plan, inventory, runner, thread="h", inventory_source=lambda: inventory, **state
    )
    assert out.status == "completed" and runner.count("collect") == 1  # finished work not repeated


async def test_resume_refuses_while_the_approval_is_still_invalid(plan, inventory):
    approve(plan, inventory)
    state = rt()
    runner = FakeRunner(scripts(analyze=[GOOD_ANALYZE]))
    await run_plan(plan, inventory, runner, params=PARAMS, thread="h", **state)
    changed = {
        **inventory,
        "agents": {**inventory["agents"], "extra": inventory["agents"]["analyst"]},
    }
    assert inventory_digest(changed) != inventory_digest(inventory)
    with pytest.raises(PlanRefused) as exc:
        await resume_plan(plan, changed, runner, thread="h", **state)
    assert "inventory_changed" in codes(exc.value.issues)


async def test_a_finished_run_resumes_to_its_own_outcome(plan, inventory):
    approve(plan, inventory)
    state = rt()
    runner = FakeRunner(scripts(analyze=[GOOD_ANALYZE]))
    first = await run_plan(plan, inventory, runner, params=PARAMS, thread="d", **state)
    again = await resume_plan(plan, inventory, FakeRunner({}), thread="d", **state)
    assert first.status == again.status == "completed"
    assert again.final_outputs == first.final_outputs
