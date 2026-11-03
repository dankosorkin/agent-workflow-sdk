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

"""Prompts for the three design roles. Static text plus data blocks, nothing generated.

The behaviour of the design phase lives HERE and in the inventory, never in engine code
(design notes, section 1). A prompt is built from fixed instructions and JSON data
blocks, so the same inputs always give the same prompt.
"""

from __future__ import annotations

import json
from typing import Any

from agentflow.workflow.checks import CHECKS

__all__ = [
    "REVIEW_CODES",
    "ROLES",
    "creator_prompt",
    "creator_task",
    "inventory_for_designers",
    "orchestrator_prompt",
    "orchestrator_task",
    "role_description",
    "role_system",
    "verifier_prompt",
    "verifier_task",
]

ROLES = ("orchestrator", "creator", "verifier")

#: The closed list of codes a verifier may use (design notes, OQ-5).
REVIEW_CODES = {
    "brief_not_covered": "part of the brief is not done by any step",
    "check_cannot_detect_failure": "the step's checks would pass on a wrong result",
    "hidden_assumption": "the plan relies on something the brief or inventory does not state",
    "agent_missing": "the plan needs an agent that the inventory does not offer",
}


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


def inventory_for_designers(inventory: dict[str, Any]) -> dict[str, Any]:
    """What a designer may see: names, descriptions and allowed permissions. No paths, no backends."""
    view: dict[str, Any] = {
        "agents": {
            name: {
                "description": agent.get("description", ""),
                "permissions": list(agent.get("permissions") or []),
            }
            for name, agent in sorted((inventory.get("agents") or {}).items())
        }
    }
    extensions = (inventory.get("context") or {}).get("extensions") or []
    if extensions:
        view["extensions"] = {
            "note": "The agent loads these itself. Name one in a step prompt when that step "
            "should use it (for example 'use the api-design skill'); nothing is pasted for you.",
            "items": [
                {"kind": e["kind"], "name": e["name"], "description": e.get("description", "")}
                for e in extensions
            ],
        }
    return view


_ROLE_DESCRIPTIONS = {
    "orchestrator": "Turns a user request into a brief (goal, context, constraints, expected "
    "results, ambiguities) for a workflow designer. Does not design and does not do the work.",
    "creator": "Designs a workflow plan (a JSON graph of steps) that fulfils a brief, using only "
    "the agents of an inventory. Answers with ONE JSON plan object.",
    "verifier": "Reviews the logic of a workflow plan against its brief and returns structured "
    "remarks (blocker or warning). Does not run the plan. Answers with ONE JSON object.",
}


def role_description(role: str) -> str:
    return _ROLE_DESCRIPTIONS[role]


_PLAN_SPEC = """\
A plan is ONE JSON object with these fields (and no others):
- "parameters": {<name>: <type>}  values supplied when the plan is run; read as $input.<name>.
- "graph": {"steps": [<step>, ...]}
- "final_outputs": [{"step": <id>, "field": <output field>}, ...]  at least one; these are the results.
Types are one of: string, number, integer, boolean, array, object, any.

A step has: "id" (unique), "agent" (a name from the inventory), "prompt" (the concrete task for
this step; static text, it must not contain placeholders), "permission" ("read_only" by default,
"allowlist" only with "tools": [...], "full" only if the agent's permissions allow it and the work
truly needs writing), "depends_on": [<step id>, ...], "outputs": {<field>: <type>} (what the step
returns as a JSON object), "inputs": {<name>: <binding>}, "checks": [<check>, ...],
"max_attempts": <int, default 3>, "side_effects": <bool>, "may_ask": {"max_questions": <int>,
"default_answer": <string or null>} (only if a human decision may be needed; not with side_effects).

A binding is exactly one of: {"from": "<step>.output.<field>"} (any ancestor step),
{"from": "$input.<name>"}, {"value": <literal>}, {"from": [..sources..], "combine": "concat"|"merge"}.
Optional keys: "type", "required": false with "default", "then": ["first", "len", "unique",
{"join": ","}, {"pick": "<key>"}]. A step sees data ONLY through its inputs.

Checks available: __CHECKS__.
A "target" is an output field of the step, optionally with a path: "items[*].level" (every item),
"items[0].name", "meta.window.from". A missing part of the path fails the check.
- {"kind": "not_empty", "target": <output field>}
- {"kind": "has_fields", "target": <output field holding a list of objects>, "fields": [<key>, ...]}
- {"kind": "count", "target": <list/string/object>, "min": n, "max": n, "equals": n,
  "same_count_as": <another target>} (give at least one of the four)
- {"kind": "compare", "target": <path>, "op": eq|ne|gt|ge|lt|le|in|not_in|contains|starts_with|
  ends_with|exists, "value": <literal> OR "other": <another target>} ("in"/"not_in" take a list in "value", or in "other" a list field or a [*] path
  (the allowed set); with another operator a [*] "other" means
  the test must hold against EVERY item (e.g. "ge" against all counts = a maximum);
  "exists" takes neither; with [*] every item must pass, and an empty match fails unless
  "allow_empty": true)
- {"kind": "unique", "target": <path>, e.g. "items[*].id"} (no duplicate values)
- {"kind": "sorted", "target": <list>, "key": <field of each item, optional>, "order": "asc"|"desc"}
A check must be able to FAIL on a wrong result; "not_empty" alone is a weak check. Prefer checks that
tie the answer to something known: a count that must equal the size of the input, a value from a
fixed set, an order, no duplicates, a number inside a range.

Do NOT use: skills, mcp, artifacts, semantic_check, budget, timeout (not supported yet)."""


def _orchestrator_system() -> str:
    return """You are wf-orchestrator. Turn the user's request into a brief for a workflow designer.
Do not design the workflow and do not do the work.

You talk to the user like a colleague who needs to understand the task before it is handed on.
If something that would change the workflow is unclear (the goal, the inputs, what a good result
looks like, limits, what must not be touched), ask about it instead of guessing. Ask only what you
cannot settle yourself: at most 5 short, concrete questions at a time, each with the answer you
would assume if the user does not care. Do not ask about how to build the workflow; that is the
designer's job. If the request is already clear, ask nothing. You may ask again after the answers
if they opened a new gap.

The user message contains the request under "### request" and, after the first round, the
conversation so far under "### conversation". When it says you cannot ask (more) questions, do not
ask: put what is still unclear under "ambiguities".

### answer format
Finish with ONE JSON object and nothing after it. Either questions for the user:
{"questions": [<string>, ...]}
or, when you understand the task well enough, the brief:
{"goal": <string>, "context": <string>, "constraints": [<string>, ...],
  "expected_results": [<string>, ...], "ambiguities": [<string>, ...]}
Fold everything the user told you into the brief."""


def _creator_system() -> str:
    spec = _PLAN_SPEC.replace("__CHECKS__", ", ".join(sorted(CHECKS)))
    return f"""You are wf-creator. Design a workflow plan that fulfils the brief, using ONLY the agents
in the inventory. Keep it as small as the brief allows: one step per distinct piece of work,
steps that do not depend on each other run in parallel.

Every step runs in a fresh session with no memory of the conversation: its prompt must carry the goal,
the paths and the decisions it needs, and name the result it must return. A step that judges another
step's work (a review) must pass or fail only through its structured result, never through free text,
and the review must come after the work it judges.

The user message contains the brief, the inventory (the only agents you may use), and, when this
is a revision, your previous plan and the problems found in it, which you must fix.

### plan format
{spec}

### answer format
Finish with the plan as ONE JSON object and nothing after it.
Do not include schema_version, approval, verification or provenance."""


def _verifier_system() -> str:
    codes = "\n".join(f"- {code}: {meaning}" for code, meaning in REVIEW_CODES.items())
    return f"""You are wf-verifier. Review a workflow plan against its brief. You judge the LOGIC of the
plan; you do not run it and you do not judge results. The plan already passed the mechanical
validator (ids, dependencies, bindings, permissions), so do not repeat those checks.

Look for: parts of the brief that no step does; checks that would still pass on a wrong result
(for each step, name the check that would catch a wrong answer and an example it would reject);
assumptions the plan makes without stating them; agents the plan needs but the inventory lacks.
Judge substance, not form: a check that exercises only the easy path is a defect; the check must model
the way the step can actually fail. A step whose success leaves no recorded evidence, so that nobody can
tell afterwards that it worked, is a defect too. Every issue must be something you can point to in the plan.

The user message contains the brief, the inventory and the plan.

### answer format
Finish with ONE JSON object and nothing after it:
{{"issues": [{{"code": <one of the codes below>, "step_id": <string or null>,
  "severity": "blocker" | "warning", "claim": <what is wrong>, "evidence_needed": <what would settle it>}}]}}
Use "blocker" only for a defect that makes the plan likely to give a wrong or incomplete result.
If you find nothing, answer {{"issues": []}}. Allowed codes:
{codes}"""


def role_system(role: str) -> str:
    """The static instructions of a role: what a named agent carries as its system prompt."""
    return {
        "orchestrator": _orchestrator_system,
        "creator": _creator_system,
        "verifier": _verifier_system,
    }[role]()


def orchestrator_task(
    request: str, dialogue: list[dict[str, Any]] | None = None, *, can_ask: bool = False
) -> str:
    """The orchestrator's data: the request, the talk so far, and whether more questions are allowed.

    ``can_ask`` is False when nobody is there to answer (or the question budget is spent); the
    message then says so, because a named agent's instructions cannot know it.
    """
    parts = [f"### request\n{request}"]
    if dialogue:
        lines = []
        for turn in dialogue:
            lines.append("you asked:\n" + "\n".join(f"- {q}" for q in turn["questions"]))
            lines.append(f"the user answered:\n{turn['answer']}")
        parts.append("### conversation\n" + "\n\n".join(lines))
    if not can_ask:
        parts.append(
            "### questions\nYou cannot ask the user (more) questions. Answer with the brief; "
            'list what is still unclear under "ambiguities".'
        )
    return "\n\n".join(parts)


def creator_task(
    brief: dict[str, Any],
    inventory: dict[str, Any],
    *,
    previous_plan: dict[str, Any] | None = None,
    feedback: list[str] | None = None,
) -> str:
    parts = [
        f"### brief\n{_json(brief)}",
        f"### inventory (the only agents you may use)\n{_json(inventory_for_designers(inventory))}",
    ]
    if previous_plan is not None:
        parts.append(f"### your previous plan\n{_json(previous_plan)}")
    if feedback:
        parts.append("### problems with your previous plan (fix every one)\n" + "\n".join(feedback))
    return "\n\n".join(parts)


def verifier_task(brief: dict[str, Any], inventory: dict[str, Any], plan: dict[str, Any]) -> str:
    return (
        f"### brief\n{_json(brief)}\n\n"
        f"### inventory\n{_json(inventory_for_designers(inventory))}\n\n"
        f"### plan\n{_json(plan)}"
    )


# The full prompts below carry the role AND the data in one message: they work on any backend,
# with no agent definition installed. A named agent carries `role_system` itself and gets only
# the `*_task` part.


def orchestrator_prompt(
    request: str, dialogue: list[dict[str, Any]] | None = None, *, can_ask: bool = False
) -> str:
    return (
        role_system("orchestrator") + "\n\n" + orchestrator_task(request, dialogue, can_ask=can_ask)
    )


def creator_prompt(
    brief: dict[str, Any],
    inventory: dict[str, Any],
    *,
    previous_plan: dict[str, Any] | None = None,
    feedback: list[str] | None = None,
) -> str:
    task = creator_task(brief, inventory, previous_plan=previous_plan, feedback=feedback)
    return role_system("creator") + "\n\n" + task


def verifier_prompt(brief: dict[str, Any], inventory: dict[str, Any], plan: dict[str, Any]) -> str:
    return role_system("verifier") + "\n\n" + verifier_task(brief, inventory, plan)
