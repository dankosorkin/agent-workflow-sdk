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
    return {
        "agents": {
            name: {
                "description": agent.get("description", ""),
                "permissions": list(agent.get("permissions") or []),
            }
            for name, agent in sorted((inventory.get("agents") or {}).items())
        }
    }


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
- {"kind": "not_empty", "target": <output field>}
- {"kind": "has_fields", "target": <output field holding a list of objects>, "fields": [<key>, ...]}
A check must be able to FAIL on a wrong result; "not_empty" alone is a weak check.

Do NOT use: skills, mcp, artifacts, semantic_check, budget, timeout (not supported yet)."""


def _orchestrator_system() -> str:
    return """You are wf-orchestrator. Turn the user's request into a brief for a workflow designer.
Do not design the workflow and do not do the work. If something is unclear, list it under
"ambiguities" instead of guessing.

The user message contains the request under "### request".

### answer format
Finish with ONE JSON object and nothing after it:
{"goal": <string>, "context": <string>, "constraints": [<string>, ...],
  "expected_results": [<string>, ...], "ambiguities": [<string>, ...]}"""


def _creator_system() -> str:
    spec = _PLAN_SPEC.replace("__CHECKS__", ", ".join(sorted(CHECKS)))
    return f"""You are wf-creator. Design a workflow plan that fulfils the brief, using ONLY the agents
in the inventory. Keep it as small as the brief allows: one step per distinct piece of work,
steps that do not depend on each other run in parallel.

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


def orchestrator_task(request: str) -> str:
    return f"### request\n{request}"


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


def orchestrator_prompt(request: str) -> str:
    return role_system("orchestrator") + "\n\n" + orchestrator_task(request)


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
