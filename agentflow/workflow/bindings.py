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

"""Data bindings between plan steps: a closed set of declarative kinds.

A step's ``inputs`` are bindings from a CLOSED set, not an expression language:

    {"from": "step.output.field"}            a step result
    {"from": "$input.name"}                  a run parameter
    {"value": ...}                           a literal
    {"from": [..], "combine": "concat"|"merge"}
    {"artifact": "step.artifacts.name"}
    plus "then": [...] pure transforms (first, len, unique, join, pick)

Anything else (notably ``{"template": ...}``) is rejected. The prompt is static;
data reaches the agent only through ``inputs``. A binding may reference ANY
ANCESTOR of the step, which the validator proves statically. An unresolved
binding at run time fails the step with a reason; it does not raise.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from agentflow.prebuilt import artifact_key
from agentflow.workflow.issues import Issue

__all__ = [
    "MISSING",
    "ancestors_of",
    "dataflow",
    "find_cycle",
    "render_prompt",
    "resolve_inputs",
    "step_cache_key",
    "validate_bindings",
]

TYPES = {"string", "number", "integer", "boolean", "array", "object", "any"}
BINDING_KEYS = {"from", "value", "artifact", "combine", "then", "type", "required", "default"}
MISSING = object()


def type_ok(value: Any, expected: str) -> bool:
    if expected == "any":
        return True
    if expected == "string":
        return isinstance(value, str)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "array":
        return isinstance(value, list)
    if expected == "object":
        return isinstance(value, dict)
    return False


# name -> (accepted input types, result type)
TRANSFORMS: dict[str, tuple[set[str], str]] = {
    "first": ({"array"}, "any"),
    "len": ({"array", "string", "object"}, "integer"),
    "unique": ({"array"}, "array"),
    "join": ({"array"}, "string"),
    "pick": ({"array"}, "array"),
}


def transform_spec(op: Any) -> tuple[str, Any] | None:
    """Normalise ``"first"`` / ``{"join": ","}`` / ``{"pick": "id"}`` to (name, arg)."""
    if isinstance(op, str) and op in ("first", "len", "unique"):
        return op, None
    if isinstance(op, dict) and len(op) == 1:
        ((name, arg),) = op.items()
        if name in ("join", "pick") and isinstance(arg, str):
            return name, arg
    return None


def apply_transform(name: str, arg: Any, value: Any) -> Any:
    if name == "first":
        return value[0] if value else MISSING
    if name == "len":
        return len(value)
    if name == "unique":
        seen: list[Any] = []
        for item in value:
            if item not in seen:
                seen.append(item)
        return seen
    if name == "join":
        return arg.join(str(v) for v in value)
    if name == "pick":
        return [item[arg] for item in value if isinstance(item, dict) and arg in item]
    raise AssertionError(name)


# ---------------------------------------------------------------------------
# Graph helpers
# ---------------------------------------------------------------------------


def ancestors_of(step_id: str, deps: dict[str, list[str]]) -> set[str]:
    seen: set[str] = set()
    stack = list(deps.get(step_id, []))
    while stack:
        cur = stack.pop()
        if cur in seen or cur == step_id:
            continue
        seen.add(cur)
        stack.extend(deps.get(cur, []))
    return seen


def find_cycle(deps: dict[str, list[str]]) -> list[str]:
    """Ids on or behind a cycle (empty if the graph is a DAG)."""
    indegree = dict.fromkeys(deps, 0)
    for s, ds in deps.items():
        for d in ds:
            if d in indegree:
                indegree[s] += 1
    ready = [s for s, n in indegree.items() if n == 0]
    done = 0
    while ready:
        cur = ready.pop()
        done += 1
        for s, ds in deps.items():
            if cur in ds:
                indegree[s] -= 1
                if indegree[s] == 0:
                    ready.append(s)
    return sorted(s for s, n in indegree.items() if n > 0) if done < len(deps) else []


@dataclass
class Ref:
    step: str | None  # None for $input
    root: str  # "output" field for steps; the parameter name for $input
    rest: list[str]


def parse_ref(text: str) -> tuple[Ref | None, str | None]:
    parts = text.split(".")
    if parts[0] == "$input":
        if len(parts) < 2:
            return None, f"{text!r}: expected $input.<name>"
        return Ref(None, parts[1], parts[2:]), None
    if len(parts) < 3 or parts[1] != "output":
        return None, f"{text!r}: expected <step>.output.<field>[.<deeper>]"
    return Ref(parts[0], parts[2], parts[3:]), None


# ---------------------------------------------------------------------------
# Static validation
# ---------------------------------------------------------------------------


def validate_bindings(
    steps: list[dict[str, Any]],
    parameters: dict[str, str],
    deps: dict[str, list[str]],
) -> list[Issue]:
    """Check every step's ``inputs``. ``deps`` is the (already validated) dependency map."""
    issues: list[Issue] = []
    by_id = {s["id"]: s for s in steps if isinstance(s.get("id"), str)}
    for sid, s in by_id.items():
        anc = ancestors_of(sid, deps)
        inputs = s.get("inputs") or {}
        if not isinstance(inputs, dict):
            continue
        for name, b in inputs.items():
            _validate_binding(sid, name, b, by_id, anc, parameters, issues)
    return issues


def _validate_binding(sid, name, b, by_id, anc, params, issues: list[Issue]) -> None:
    def err(code: str, message: str) -> None:
        issues.append(Issue(code, message, step=sid, input=name))

    if not isinstance(b, dict):
        err("unknown_binding_kind", "a binding must be an object")
        return
    extra = set(b) - BINDING_KEYS
    if extra:
        # "template" lands here: templates are a closed-set violation, not a feature.
        code = "unknown_binding_kind" if "template" in extra else "unknown_key"
        err(code, f"unsupported binding key(s) {sorted(extra)}")
        return
    kinds = [k for k in ("from", "value", "artifact") if k in b]
    if len(kinds) != 1:
        err("unknown_binding_kind", "a binding needs exactly one of from/value/artifact")
        return
    if b.get("required") is False and "default" not in b:
        err("optional_needs_default", "required=false needs an explicit default")
    declared_type = b.get("type")
    if declared_type is not None and declared_type not in TYPES:
        err("type_mismatch", f"unknown type {declared_type!r}")

    result_type = "any"
    kind = kinds[0]
    if kind == "artifact":
        step_part, _, art = str(b["artifact"]).partition(".artifacts.")
        if step_part not in by_id:
            err("unknown_step", f"artifact refers to unknown step {step_part!r}")
        elif step_part not in anc:
            err("not_ancestor", f"{step_part!r} is not an ancestor of {sid!r}")
        elif art not in (by_id[step_part].get("artifacts") or []):
            err("unknown_artifact", f"{step_part!r} declares no artifact {art!r}")
        result_type = "string"
    elif kind == "from":
        sources = b["from"] if isinstance(b["from"], list) else [b["from"]]
        if isinstance(b["from"], list) and "combine" not in b:
            err("combine_needs_list", "a list of sources needs a combine")
        if "combine" in b and not isinstance(b["from"], list):
            err("bad_combine", "combine needs a list of sources")
        types = [_check_ref(str(src), by_id, anc, params, err) for src in sources]
        if "combine" in b:
            want = {"concat": "array", "merge": "object"}.get(b["combine"])
            if want is None:
                err("bad_combine", f"unknown combine {b['combine']!r}")
            elif any(t not in (want, "any") for t in types):
                err("bad_combine", f"{b['combine']} needs every source to be {want}")
            result_type = want or "any"
        else:
            result_type = types[0]

    for op in b.get("then") or []:
        spec = transform_spec(op)
        if spec is None:
            err("bad_transform", f"unknown transform {op!r}")
            result_type = "any"
            continue
        accepts, out = TRANSFORMS[spec[0]]
        if result_type != "any" and result_type not in accepts:
            err("bad_transform", f"{spec[0]} cannot take {result_type}")
        result_type = out
    if declared_type not in (None, "any") and result_type not in (declared_type, "any"):
        err("type_mismatch", f"declared {declared_type}, but the source is {result_type}")


def _check_ref(text: str, by_id, anc, params, err) -> str:
    ref, problem = parse_ref(text)
    if ref is None:
        err("bad_path", problem or "bad path")
        return "any"
    if ref.step is None:
        if ref.root not in params:
            err("unknown_param", f"no plan parameter {ref.root!r}")
            return "any"
        return params[ref.root] if not ref.rest else "any"
    if ref.step not in by_id:
        err("unknown_step", f"unknown step {ref.step!r}")
        return "any"
    if ref.step not in anc:
        err("not_ancestor", f"{ref.step!r} is not an ancestor of the consuming step")
        return "any"
    declared = by_id[ref.step].get("outputs") or {}
    if ref.root not in declared:
        err("unknown_output_field", f"{ref.step!r} declares no output {ref.root!r}")
        return "any"
    return declared[ref.root] if not ref.rest else "any"


def dataflow(steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Who feeds whom: the listing an approval screen shows next to the dependency graph."""
    by_id = {s["id"]: s for s in steps}
    deps = {sid: list(s.get("depends_on", [])) for sid, s in by_id.items()}
    edges: list[dict[str, Any]] = []
    for sid, s in by_id.items():
        for name, b in (s.get("inputs") or {}).items():
            srcs: list[str] = []
            if "artifact" in b:
                srcs = [str(b["artifact"]).partition(".artifacts.")[0]]
            elif "from" in b:
                for t in b["from"] if isinstance(b["from"], list) else [b["from"]]:
                    ref, _ = parse_ref(str(t))
                    if ref and ref.step:
                        srcs.append(ref.step)
            for src in srcs:
                edges.append({"from": src, "to": sid, "input": name, "direct": src in deps[sid]})
    return edges


# ---------------------------------------------------------------------------
# Run-time resolution, rendering, cache key
# ---------------------------------------------------------------------------


def lookup(envelope: dict[str, Any], parts: list[str]) -> Any:
    cur: Any = envelope
    for p in parts:
        if not isinstance(cur, dict) or p not in cur:
            return MISSING
        cur = cur[p]
    return cur


def resolve_one(b: dict[str, Any], results: dict[str, Any], params: dict[str, Any]) -> Any:
    """Resolve one binding to a value, or ``MISSING``. Never raises on missing data."""
    if "value" in b:
        value = b["value"]
    elif "artifact" in b:
        step, _, art = str(b["artifact"]).partition(".artifacts.")
        env = (results.get(step) or {}).get("output") or {}
        value = lookup(env, ["artifacts", art])
    else:
        sources = b["from"] if isinstance(b["from"], list) else [b["from"]]
        got = []
        for text in sources:
            ref, _ = parse_ref(str(text))
            assert ref is not None, "validated plans only"
            if ref.step is None:
                got.append(lookup(params, [ref.root, *ref.rest]))
            else:
                env = (results.get(ref.step) or {}).get("output") or {}
                got.append(lookup(env, ["output", ref.root, *ref.rest]))
        if "combine" in b:
            present = [g for g in got if g is not MISSING]
            if len(present) != len(got):
                value = MISSING
            elif b["combine"] == "concat":
                value = [x for g in present for x in g]
            else:
                value = {k: v for g in present for k, v in g.items()}
        else:
            value = got[0]
    for op in b.get("then") or []:
        if value is MISSING:
            break
        spec = transform_spec(op)
        assert spec is not None, "validated plans only"
        value = apply_transform(spec[0], spec[1], value)
    return value


def resolve_inputs(
    step: dict[str, Any], results: dict[str, Any], params: dict[str, Any]
) -> tuple[dict[str, Any], list[str]]:
    """Resolve a step's inputs; returns (values, problems). Problems fail the step."""
    inputs: dict[str, Any] = {}
    problems: list[str] = []
    for name, b in (step.get("inputs") or {}).items():
        value = resolve_one(b, results, params)
        if value is MISSING:
            if b.get("required", True):
                what = b.get("from") or b.get("artifact") or "value"
                problems.append(f"input {name!r}: binding {what!r} did not resolve")
                continue
            value = b["default"]
        if "type" in b and not type_ok(value, b["type"]):
            problems.append(f"input {name!r}: expected {b['type']}, got {type(value).__name__}")
            continue
        inputs[name] = value
    return inputs, problems


def render_prompt(step: dict[str, Any], inputs: dict[str, Any]) -> str:
    """Static prompt plus a fixed input block. Same inputs give byte-identical text."""
    blocks = [step["prompt"]]
    for name in sorted(inputs):
        body = json.dumps(inputs[name], ensure_ascii=False, sort_keys=True, indent=2)
        blocks.append(f"### input: {name}\n```json\n{body}\n```")
    return "\n\n".join(blocks)


def step_cache_key(step: dict[str, Any], inputs: dict[str, Any]) -> str:
    """A function of what the agent will actually see, nothing else."""
    return artifact_key({"agent": step.get("agent"), "prompt": step["prompt"], "inputs": inputs})
