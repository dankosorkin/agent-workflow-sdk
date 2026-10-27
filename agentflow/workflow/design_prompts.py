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
    "creator_prompt",
    "inventory_for_designers",
    "orchestrator_prompt",
    "verifier_prompt",
]

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


def orchestrator_prompt(request: str) -> str:
    return f"""You are wf-orchestrator. Turn the user's request into a brief for a workflow designer.
Do not design the workflow and do not do the work. If something is unclear, list it under
"ambiguities" instead of guessing.

### request
{request}

### answer format
Finish with ONE JSON object and nothing after it:
{{"goal": <string>, "context": <string>, "constraints": [<string>, ...],
  "expected_results": [<string>, ...], "ambiguities": [<string>, ...]}}"""


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


def creator_prompt(
    brief: dict[str, Any],
    inventory: dict[str, Any],
    *,
    previous_plan: dict[str, Any] | None = None,
    feedback: list[str] | None = None,
) -> str:
    spec = _PLAN_SPEC.replace("__CHECKS__", ", ".join(sorted(CHECKS)))
    parts = [
        "You are wf-creator. Design a workflow plan that fulfils the brief, using ONLY the agents "
        "in the inventory. Keep it as small as the brief allows: one step per distinct piece of "
        "work, steps that do not depend on each other run in parallel.",
        f"### plan format\n{spec}",
        f"### brief\n{_json(brief)}",
        f"### inventory (the only agents you may use)\n{_json(inventory_for_designers(inventory))}",
    ]
    if previous_plan is not None:
        parts.append(f"### your previous plan\n{_json(previous_plan)}")
    if feedback:
        parts.append("### problems with your previous plan (fix every one)\n" + "\n".join(feedback))
    parts.append(
        "### answer format\nFinish with the plan as ONE JSON object and nothing after it. "
        "Do not include schema_version, approval, verification or provenance."
    )
    return "\n\n".join(parts)


def verifier_prompt(brief: dict[str, Any], inventory: dict[str, Any], plan: dict[str, Any]) -> str:
    codes = "\n".join(f"- {code}: {meaning}" for code, meaning in REVIEW_CODES.items())
    return f"""You are wf-verifier. Review a workflow plan against its brief. You judge the LOGIC of the
plan; you do not run it and you do not judge results. The plan already passed the mechanical
validator (ids, dependencies, bindings, permissions), so do not repeat those checks.

Look for: parts of the brief that no step does; checks that would still pass on a wrong result
(for each step, name the check that would catch a wrong answer and an example it would reject);
assumptions the plan makes without stating them; agents the plan needs but the inventory lacks.

### brief
{_json(brief)}

### inventory
{_json(inventory_for_designers(inventory))}

### plan
{_json(plan)}

### answer format
Finish with ONE JSON object and nothing after it:
{{"issues": [{{"code": <one of the codes below>, "step_id": <string or null>,
  "severity": "blocker" | "warning", "claim": <what is wrong>, "evidence_needed": <what would settle it>}}]}}
Use "blocker" only for a defect that makes the plan likely to give a wrong or incomplete result.
If you find nothing, answer {{"issues": []}}. Allowed codes:
{codes}"""
