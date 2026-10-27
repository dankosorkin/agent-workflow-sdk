# Copyright 2026 Daniel Sorkin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The design phase: request -> brief -> plan, reviewed until it can be approved.

    result = await design_plan("find the 3 largest files in ./src", inventory, designer)

Three roles, each one agent call: ``orchestrator`` writes the brief, ``creator`` writes the
plan, ``verifier`` reviews its logic. They run in a bounded loop (``max_rounds``):

  1. the creator proposes a plan;
  2. the DETERMINISTIC checks run first (:func:`validate_plan`, :func:`unsupported_features`,
     and the minimum-evidence rule), so an invalid plan never costs a verifier call, and
     their findings go straight back to the creator;
  3. only a mechanically sound plan reaches the verifier; its ``blocker`` remarks go back
     to the creator, its ``warning`` remarks travel with the plan to the human;
  4. no blockers means the plan has converged.

After the last round the plan goes to a human WITH its open remarks; it is never
silently accepted and the loop never runs on. A plan that is still not executable is
reported as such and cannot be approved.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from agentflow.workflow.checks import extract_json_object
from agentflow.workflow.design_prompts import (
    REVIEW_CODES,
    creator_prompt,
    creator_task,
    orchestrator_prompt,
    orchestrator_task,
    verifier_prompt,
    verifier_task,
)
from agentflow.workflow.inventory import inventory_digest
from agentflow.workflow.issues import Issue, has_errors
from agentflow.workflow.plan import (
    SCHEMA_VERSION,
    auto_approval_blockers,
    plan_hash,
    validate_plan,
)
from agentflow.workflow.roles import agent_name, claude_inline_agents
from agentflow.workflow.runner import PlanRefused, collect_text, unsupported_features

__all__ = [
    "AskUser",
    "DesignResult",
    "DesignerRunner",
    "approve_plan",
    "backend_designer",
    "claude_designer",
    "design_plan",
    "minimum_evidence_issues",
    "try_auto_approve",
]

#: ``(role, prompt) -> the agent's final text``; role is orchestrator, creator or verifier.
DesignerRunner = Callable[[str, str], Awaitable[str]]

#: ``(questions) -> the user's reply``, free text; ``None`` or blank means "no more answers".
AskUser = Callable[[list[str]], Awaitable[str | None]]

_PLAN_KEYS_FROM_CREATOR = {
    "parameters", "graph", "final_outputs", "run_limits", "budget",
}  # fmt: skip
_MAX_QUESTIONS_PER_ROUND = 5
_BRIEF_KEYS = ("goal", "context", "constraints", "expected_results", "ambiguities")


@dataclass
class DesignResult:
    """What the design phase produced and why it stopped.

    ``status``: ``converged`` (no blockers left), ``round_limit`` (blockers remain, the
    human decides), ``not_executable`` (the last plan fails the mechanical checks and
    cannot be approved), ``failed`` (no brief or no usable plan at all).
    """

    status: str
    brief: dict[str, Any] | None = None
    plan: dict[str, Any] | None = None
    issues: list[dict[str, Any]] = field(default_factory=list)  # open remarks
    mechanical: list[Issue] = field(default_factory=list)  # validator findings on the last plan
    rounds: list[dict[str, Any]] = field(default_factory=list)  # history, one entry per round
    dialogue: list[dict[str, Any]] = field(default_factory=list)  # the orchestrator's Q&A
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "brief": self.brief,
            "plan": self.plan,
            "issues": self.issues,
            "mechanical": [i.to_dict() for i in self.mechanical],
            "rounds": self.rounds,
            "dialogue": self.dialogue,
            "reason": self.reason,
        }


def claude_designer(
    cwd: Path | str, *, installed: bool = False, model: str | None = None
) -> DesignerRunner:
    """Designers on Claude Code with the named wf-* agents and no tools (use ``named_agents=True``).

    ``installed=True`` expects the agents to be installed where Claude Code finds them (see
    :mod:`agentflow.workflow.roles`); ``installed=False`` passes them inline with ``--agents``
    for this session only, so nothing is written anywhere.
    """
    from agentflow.backends import DenyAll
    from agentflow.backends.claude_code import ClaudeCodeBackend

    def make(role: str) -> Any:
        extra = [] if installed else ["--agents", claude_inline_agents()]
        return ClaudeCodeBackend(
            agent=agent_name(role), model=model, cwd=cwd, permission=DenyAll(), extra_args=extra
        )

    return backend_designer(make)


def backend_designer(make: Callable[[str], Any]) -> DesignerRunner:
    """A :data:`DesignerRunner` that gives every call a fresh backend from ``make(role)``.

    Designers should run without tools: build the backend with a deny-all policy, e.g.
    ``lambda role: ClaudeCodeBackend(cwd=tmp, permission=DenyAll())``. A fresh instance per
    call means no conversation leaks between the three roles or between rounds.
    """

    async def run(role: str, prompt: str) -> str:
        return await collect_text(make(role), prompt)

    return run


# ---------------------------------------------------------------------------
# Deterministic findings
# ---------------------------------------------------------------------------


def minimum_evidence_issues(plan: dict[str, Any]) -> list[dict[str, Any]]:
    """Review remarks the code can make itself: a step whose checks could never fail.

    A step with no checks at all, or only ``not_empty`` checks, gives no real evidence
    (design notes, OQ-5). Reported as blockers in the verifier's format.
    """
    out = []
    for step in (plan.get("graph") or {}).get("steps") or []:
        kinds = {c.get("kind") for c in step.get("checks") or [] if isinstance(c, dict)}
        if not kinds or kinds == {"not_empty"}:
            why = "has no checks" if not kinds else "has only 'not_empty' checks"
            out.append(
                {
                    "code": "check_cannot_detect_failure",
                    "step_id": step.get("id"),
                    "severity": "blocker",
                    "claim": f"step {step.get('id')!r} {why}, so a wrong result would pass",
                    "evidence_needed": "a check that fails on a wrong result, "
                    "e.g. 'has_fields' on the output list",
                    "source": "code",
                }
            )
    return out


def _mechanical(plan: dict[str, Any], inventory: dict[str, Any]) -> list[Issue]:
    return [
        i
        for i in validate_plan(plan, inventory=inventory) + unsupported_features(plan)
        if i.severity == "error"
    ]


def _feedback_from_issues(issues: list[Issue]) -> list[str]:
    return [
        f"- [{i.code}] {('step ' + i.step + ': ') if i.step else ''}{i.message}" for i in issues
    ]


def _feedback_from_remarks(remarks: list[dict[str, Any]]) -> list[str]:
    return [
        f"- [{r['code']}] {('step ' + str(r['step_id']) + ': ') if r.get('step_id') else ''}"
        f"{r['claim']}" + (f" (needed: {r['evidence_needed']})" if r.get("evidence_needed") else "")
        for r in remarks
    ]


# ---------------------------------------------------------------------------
# Parsing the three roles' answers
# ---------------------------------------------------------------------------


def _parse_brief(text: str) -> dict[str, Any] | None:
    obj = extract_json_object(text)
    if not obj or not isinstance(obj.get("goal"), str) or not obj["goal"].strip():
        return None
    brief: dict[str, Any] = {"goal": obj["goal"], "context": str(obj.get("context") or "")}
    for key in _BRIEF_KEYS[2:]:
        value = obj.get(key) or []
        brief[key] = [str(v) for v in value] if isinstance(value, list) else [str(value)]
    return brief


def _parse_questions(text: str) -> list[str] | None:
    """The orchestrator's questions, or ``None`` when the answer is not a question round."""
    obj = extract_json_object(text)
    if not obj or obj.get("goal") or not isinstance(obj.get("questions"), list):
        return None
    questions = [str(q).strip() for q in obj["questions"] if str(q).strip()]
    return questions[:_MAX_QUESTIONS_PER_ROUND] or None


def _parse_plan(text: str, brief: dict[str, Any]) -> dict[str, Any] | None:
    obj = extract_json_object(text)
    if not obj or "graph" not in obj:
        return None
    plan: dict[str, Any] = {"schema_version": SCHEMA_VERSION, "plan_version": 1, "brief": brief}
    plan.update({k: v for k, v in obj.items() if k in _PLAN_KEYS_FROM_CREATOR})
    # Anything else the creator wrote (approval, provenance, verification, typos) is dropped
    # on purpose: those are not the creator's to set.
    return plan


def _parse_remarks(text: str) -> list[dict[str, Any]] | None:
    """The verifier's remarks, or ``None`` when the answer is not usable at all."""
    obj = extract_json_object(text)
    if obj is None or not isinstance(obj.get("issues"), list):
        return None
    remarks = []
    for raw in obj["issues"]:
        ok = (
            isinstance(raw, dict)
            and raw.get("code") in REVIEW_CODES
            and raw.get("severity") in ("blocker", "warning")
            and isinstance(raw.get("claim"), str)
            and raw["claim"]
        )
        if ok:
            remarks.append(
                {
                    "code": raw["code"],
                    "step_id": raw.get("step_id") if isinstance(raw.get("step_id"), str) else None,
                    "severity": raw["severity"],
                    "claim": raw["claim"],
                    "evidence_needed": str(raw.get("evidence_needed") or ""),
                }
            )
        else:
            # Never lose a remark, never let a malformed one block: keep it visible as a warning.
            remarks.append(
                {
                    "code": "hidden_assumption",
                    "step_id": None,
                    "severity": "warning",
                    "claim": f"the verifier gave a remark that does not fit the format: {raw!r}",
                    "evidence_needed": "",
                }
            )
    return remarks


# ---------------------------------------------------------------------------
# The loop
# ---------------------------------------------------------------------------


async def design_plan(
    request: str,
    inventory: dict[str, Any],
    designer: DesignerRunner,
    *,
    max_rounds: int = 3,
    named_agents: bool = False,
    ask: AskUser | None = None,
    max_question_rounds: int = 3,
) -> DesignResult:
    """Design a plan for ``request`` from the agents in ``inventory``. Never raises on bad
    agent output: every failure ends in a :class:`DesignResult` that says what happened.

    With ``ask`` the orchestrator may talk to the user first: it sends questions, ``ask``
    returns the reply, and this repeats up to ``max_question_rounds`` times until the
    orchestrator writes the brief. Without ``ask`` (or after the budget, or when ``ask``
    returns nothing) the orchestrator must write the brief and list the gaps as ambiguities.
    """
    if max_rounds < 1:
        raise ValueError("max_rounds must be >= 1")
    # With named agents the role's instructions live in the agent definition (installed from
    # agentflow.workflow.roles, or passed inline), so each call carries only the data. Without
    # them, every call carries the full instructions and works on any backend.
    if named_agents:
        orchestrator_text = orchestrator_task
        creator_text = creator_task
        verifier_text = verifier_task
    else:
        orchestrator_text = orchestrator_prompt
        creator_text = creator_prompt
        verifier_text = verifier_prompt

    dialogue: list[dict[str, Any]] = []
    questions_left = max_question_rounds if ask is not None else 0
    brief = None
    unusable = 0
    while brief is None and unusable < 2:  # one retry for an unusable brief
        text = await designer(
            "orchestrator", orchestrator_text(request, dialogue, can_ask=questions_left > 0)
        )
        questions = _parse_questions(text) if questions_left > 0 else None
        if questions is not None and ask is not None:
            questions_left -= 1
            reply = await ask(questions)
            if not reply or not reply.strip():
                reply = "(the user gave no answer; decide yourself and note it as an ambiguity)"
                questions_left = 0
            dialogue.append({"questions": questions, "answer": reply})
            continue
        brief = _parse_brief(text)
        if brief is None:
            unusable += 1
    if brief is None:
        return DesignResult(
            "failed", dialogue=dialogue, reason="the orchestrator did not produce a usable brief"
        )

    result = DesignResult("failed", brief=brief, dialogue=dialogue)
    plan: dict[str, Any] | None = None
    feedback: list[str] = []
    remarks: list[dict[str, Any]] = []

    for number in range(1, max_rounds + 1):
        entry: dict[str, Any] = {"round": number, "feedback_in": feedback}
        result.rounds.append(entry)

        text = await designer(
            "creator", creator_text(brief, inventory, previous_plan=plan, feedback=feedback)
        )
        candidate = _parse_plan(text, brief)
        if candidate is None:
            entry["stage"] = "creator"
            feedback = [
                "- [unusable_answer] the answer must end with ONE JSON plan object "
                'that has a "graph" field'
            ]
            continue
        plan = candidate
        result.plan = plan

        mech = _mechanical(plan, inventory)
        result.mechanical = mech
        entry["mechanical"] = [i.to_dict() for i in mech]
        evidence = minimum_evidence_issues(plan) if not mech else []
        if mech or evidence:
            entry["stage"] = "mechanical"
            remarks = evidence
            feedback = _feedback_from_issues(mech) + _feedback_from_remarks(evidence)
            continue

        entry["stage"] = "verifier"
        found = _parse_remarks(await designer("verifier", verifier_text(brief, inventory, plan)))
        if found is None:
            found = [
                {
                    "code": "hidden_assumption",
                    "step_id": None,
                    "severity": "warning",
                    "claim": "the verifier gave no usable answer, so the plan was NOT reviewed",
                    "evidence_needed": "run the review again or review the plan by hand",
                }
            ]
        entry["remarks"] = found
        remarks = found
        blockers = [r for r in found if r["severity"] == "blocker"]
        if not blockers:
            return _finish(result, "converged", plan, inventory, remarks, number)
        feedback = _feedback_from_remarks(blockers)

    if plan is None:
        result.reason = "the creator never produced a plan with a graph"
        return result
    mech = _mechanical(plan, inventory)
    if mech:
        result.reason = "the last plan still fails the mechanical checks and cannot be approved"
        return _finish(result, "not_executable", plan, inventory, remarks, max_rounds)
    result.reason = "open blockers remain after the last round; a human must decide"
    return _finish(result, "round_limit", plan, inventory, remarks, max_rounds)


def _finish(
    result: DesignResult,
    status: str,
    plan: dict[str, Any],
    inventory: dict[str, Any],
    remarks: list[dict[str, Any]],
    rounds: int,
) -> DesignResult:
    plan["verification"] = {"iterations": rounds, "issues": remarks}
    plan["provenance"] = {
        "inventory_digest": inventory_digest(inventory),
        "engine_version": _engine_version(),
        "plan_hash": plan_hash(plan),
    }
    result.status = status
    result.plan = plan
    result.issues = remarks
    return result


def _engine_version() -> str:
    try:
        from importlib.metadata import version

        return version("agent-workflow-sdk")
    except Exception:  # pragma: no cover - not installed as a distribution
        return "unknown"


# ---------------------------------------------------------------------------
# Approval
# ---------------------------------------------------------------------------


def approve_plan(
    plan: dict[str, Any], inventory: dict[str, Any], *, by: str, mode: str = "manual"
) -> dict[str, Any]:
    """Record a HUMAN approval of exactly this plan against exactly this inventory.

    Refuses a plan that is not mechanically sound or not executable yet. The approval
    is bound to ``plan_hash`` and ``inventory_digest``: changing either revokes it.
    """
    problems = validate_plan(plan, inventory=inventory) + unsupported_features(plan)
    if has_errors(problems):
        raise PlanRefused([i for i in problems if i.severity == "error"])
    approved = dict(plan)
    approved["provenance"] = {
        **(plan.get("provenance") or {}),
        "inventory_digest": inventory_digest(inventory),
        "engine_version": _engine_version(),
        "plan_hash": plan_hash(plan),
    }
    approved["approval"] = {
        "mode": mode,
        "status": "approved",
        "plan_hash": plan_hash(plan),
        "by": by,
        "at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    return approved


def try_auto_approve(
    plan: dict[str, Any],
    inventory: dict[str, Any],
    *,
    command_allowlist: frozenset[str] = frozenset(),
) -> tuple[dict[str, Any] | None, list[str]]:
    """Approve without a human only when the safety threshold holds (OQ-4).

    Returns ``(approved plan, [])`` or ``(None, reasons)``; on ``None`` the plan must go to
    a human. Also refuses when a verifier ``blocker`` is still open.
    """
    reasons = auto_approval_blockers(plan, inventory, command_allowlist=command_allowlist)
    open_blockers = [
        r
        for r in (plan.get("verification") or {}).get("issues") or []
        if r.get("severity") == "blocker"
    ]
    if open_blockers:
        reasons.append(f"{len(open_blockers)} open verifier blocker(s)")
    if reasons:
        return None, reasons
    return approve_plan(plan, inventory, by="auto", mode="auto"), []
