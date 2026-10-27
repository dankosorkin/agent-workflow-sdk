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

"""Spike: data bindings between plan steps — a closed set of declarative kinds.

Decisions under test (design notes §6 / OQ-8):

  * A step's ``inputs`` are bindings from a CLOSED set of kinds, not an expression
    language: ``{"from": "step.output.field"}`` (a step result), ``{"from":
    "$input.name"}`` (a run parameter), ``{"value": ...}``, ``{"from": [..],
    "combine": "concat"|"merge"}``, ``{"artifact": "step.artifacts.name"}`` plus
    ``"then": [...]`` pure transforms (``first``, ``len``, ``unique``, ``join``,
    ``pick``). Anything else — notably ``{"template": ...}`` — is REJECTED.
  * The prompt is STATIC. Data reaches the agent only through ``inputs``, which the
    handler renders into a fixed, deterministic block after the prompt.
  * A binding may reference ANY ANCESTOR of the step, not just direct
    prerequisites: ``plan_executor`` only runs a step after its prerequisites
    succeeded, and those only after theirs, so an ancestor's result is guaranteed
    to exist. The validator proves "is an ancestor" statically.
  * An unresolved binding at run time FAILS THE STEP with a reason; it does not
    raise (an exception would discard the whole parallel round).
  * The cache key of a step is derived from the RESOLVED inputs, so a changed
    upstream result changes the key — and a change a step does not read does not.

What the spike builds, with no engine changes (hermetic, scripted agent double):
a validator (all errors, each with a code), a resolver, a prompt renderer, a data
flow listing for the approval screen, and an end-to-end plan run through the real
``add_plan_executor``.

HONEST SCOPE:
  * Paths are plain dotted names (``output.meta.count``), not JSONPath. A real
    layer would adopt JSONPath; the validation rules would not change.
  * Static type checking is coarse: it knows the declared top-level field types
    and treats everything deeper, and every transform it cannot type, as ``any``.
  * Skipped steps (``when``) and ``foreach`` are not modelled. When ``when``
    arrives, a binding to a possibly-skipped ancestor must be optional; the
    validator would have to enforce that.
  * Parameters (``$input``) are carried in a ``params`` state channel here. How
    they enter a real run (CLI, API) is outside the spike.
  * There are no retries; a failed contract or check fails the step. Retry and
    crash behaviour are covered by ``gate_resume_spike.py``.

Run it directly:

    python spikes/bindings_spike.py
"""

from __future__ import annotations

import asyncio
import copy
import json
from dataclasses import dataclass, field
from typing import Annotated, Any

from _checks import checkpointer_factories, run_checks
from agentflow import END, START, Graph, last
from agentflow.prebuilt import (
    PlanState,
    Task,
    TaskResult,
    add_plan_executor,
    artifact_key,
    plan_complete,
)

TYPES = {"string", "number", "integer", "boolean", "array", "object", "any"}
BINDING_KEYS = {"from", "value", "artifact", "combine", "then", "type", "required", "default"}
MAX_INPUT_CHARS = 4000


# ---------------------------------------------------------------------------
# Types and transforms
# ---------------------------------------------------------------------------


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
_MISSING = object()


def transform_spec(op: Any) -> tuple[str, Any] | None:
    """Normalise ``"first"`` / ``{"join": ","}`` / ``{"pick": "id"}`` -> (name, arg)."""
    if isinstance(op, str) and op in ("first", "len", "unique"):
        return op, None
    if isinstance(op, dict) and len(op) == 1:
        ((name, arg),) = op.items()
        if name in ("join", "pick") and isinstance(arg, str):
            return name, arg
    return None


def apply_transform(name: str, arg: Any, value: Any) -> Any:
    """Apply one pure transform; ``_MISSING`` means 'nothing to bind' (e.g. first([]))."""
    if name == "first":
        return value[0] if value else _MISSING
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
# Static validation: every error, each with a machine-readable code
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
    """Return the ids that are part of or behind a cycle (empty if the graph is a DAG)."""
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
    """A parsed ``from`` reference."""

    step: str | None  # None for $input
    root: str  # "output" for steps; the parameter name for $input
    rest: list[str]  # deeper path parts


def parse_ref(text: str) -> tuple[Ref | None, str | None]:
    parts = text.split(".")
    if parts[0] == "$input":
        if len(parts) < 2:
            return None, f"{text!r}: expected $input.<name>"
        return Ref(None, parts[1], parts[2:]), None
    if len(parts) < 3 or parts[1] != "output":
        return None, f"{text!r}: expected <step>.output.<field>[.<deeper>]"
    return Ref(parts[0], parts[2], parts[3:]), None


def validate_plan(plan: dict[str, Any]) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []

    def err(code: str, step: str, inp: str, message: str) -> None:
        errors.append({"code": code, "step": step, "input": inp, "message": message})

    steps = plan["steps"]
    params: dict[str, str] = plan.get("parameters", {})
    by_id: dict[str, dict[str, Any]] = {}
    for s in steps:
        if s["id"] in by_id:
            err("duplicate_id", s["id"], "", f"step id {s['id']!r} is used twice")
        by_id.setdefault(s["id"], s)

    deps = {sid: list(s.get("depends_on", [])) for sid, s in by_id.items()}
    for sid, ds in deps.items():
        for d in ds:
            if d not in by_id:
                err("unknown_dependency", sid, "", f"depends on unknown step {d!r}")
    cyc = find_cycle({sid: [d for d in ds if d in by_id] for sid, ds in deps.items()})
    if cyc:
        err("cycle", ",".join(cyc), "", f"dependency cycle among {cyc}")

    for sid, s in by_id.items():
        anc = ancestors_of(sid, deps)
        for name, b in (s.get("inputs") or {}).items():
            _validate_binding(sid, name, b, by_id, anc, params, err)
    return errors


def _validate_binding(sid, name, b, by_id, anc, params, err) -> None:
    """Check one binding; returns nothing, reports through ``err``."""
    if not isinstance(b, dict):
        err("unknown_binding_kind", sid, name, "a binding must be an object")
        return
    extra = set(b) - BINDING_KEYS
    if extra:
        # "template" lands here: templates are a closed-set violation, not a feature.
        code = "unknown_binding_kind" if "template" in extra else "unknown_key"
        err(code, sid, name, f"unsupported binding key(s) {sorted(extra)}")
        return
    kinds = [k for k in ("from", "value", "artifact") if k in b]
    if len(kinds) != 1:
        err("unknown_binding_kind", sid, name, "a binding needs exactly one of from/value/artifact")
        return
    if "required" in b and b["required"] is False and "default" not in b:
        err("optional_needs_default", sid, name, "required=false needs an explicit default")
    declared_type = b.get("type")
    if declared_type is not None and declared_type not in TYPES:
        err("type_mismatch", sid, name, f"unknown type {declared_type!r}")

    result_type = "any"
    kind = kinds[0]
    if kind == "value":
        result_type = "any"
    elif kind == "artifact":
        step_part, _, art = str(b["artifact"]).partition(".artifacts.")
        if step_part not in by_id:
            err("unknown_step", sid, name, f"artifact refers to unknown step {step_part!r}")
        elif step_part not in anc:
            err("not_ancestor", sid, name, f"{step_part!r} is not an ancestor of {sid!r}")
        elif art not in (by_id[step_part].get("artifacts") or []):
            err("unknown_artifact", sid, name, f"{step_part!r} declares no artifact {art!r}")
        result_type = "string"
    else:  # "from"
        sources = b["from"] if isinstance(b["from"], list) else [b["from"]]
        if isinstance(b["from"], list) and "combine" not in b:
            err("combine_needs_list", sid, name, "a list of sources needs a combine")
        if "combine" in b and not isinstance(b["from"], list):
            err("bad_combine", sid, name, "combine needs a list of sources")
        types = [_check_ref(sid, name, src, by_id, anc, params, err) for src in sources]
        if "combine" in b:
            want = {"concat": "array", "merge": "object"}.get(b["combine"])
            if want is None:
                err("bad_combine", sid, name, f"unknown combine {b['combine']!r}")
            elif any(t not in (want, "any") for t in types):
                err("bad_combine", sid, name, f"{b['combine']} needs every source to be {want}")
            result_type = want or "any"
        else:
            result_type = types[0]

    for op in b.get("then") or []:
        spec = transform_spec(op)
        if spec is None:
            err("bad_transform", sid, name, f"unknown transform {op!r}")
            result_type = "any"
            continue
        accepts, out = TRANSFORMS[spec[0]]
        if result_type != "any" and result_type not in accepts:
            err("bad_transform", sid, name, f"{spec[0]} cannot take {result_type}")
        result_type = out
    if declared_type not in (None, "any") and result_type not in (declared_type, "any"):
        err(
            "type_mismatch", sid, name, f"declared {declared_type}, but the source is {result_type}"
        )


def _check_ref(sid, name, text, by_id, anc, params, err) -> str:
    ref, problem = parse_ref(str(text))
    if ref is None:
        err("bad_path", sid, name, problem or "bad path")
        return "any"
    if ref.step is None:
        if ref.root not in params:
            err("unknown_param", sid, name, f"no plan parameter {ref.root!r}")
            return "any"
        return params[ref.root] if not ref.rest else "any"
    if ref.step not in by_id:
        err("unknown_step", sid, name, f"unknown step {ref.step!r}")
        return "any"
    if ref.step not in anc:
        err("not_ancestor", sid, name, f"{ref.step!r} is not an ancestor of {sid!r}")
        return "any"
    declared = by_id[ref.step].get("outputs") or {}
    if ref.root not in declared:
        err("unknown_output_field", sid, name, f"{ref.step!r} declares no output {ref.root!r}")
        return "any"
    return declared[ref.root] if not ref.rest else "any"


def dataflow(plan: dict[str, Any]) -> list[dict[str, Any]]:
    """Who feeds whom — the listing an approval screen shows next to the dependency graph."""
    by_id = {s["id"]: s for s in plan["steps"]}
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
            return _MISSING
        cur = cur[p]
    return cur


def resolve_one(b: dict[str, Any], results: dict[str, Any], params: dict[str, Any]) -> Any:
    """Resolve one binding to a value, or ``_MISSING``. Never raises on missing data."""
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
            present = [g for g in got if g is not _MISSING]
            if len(present) != len(got):
                value = _MISSING
            elif b["combine"] == "concat":
                value = [x for g in present for x in g]
            else:
                value = {k: v for g in present for k, v in g.items()}
        else:
            value = got[0]
    for op in b.get("then") or []:
        if value is _MISSING:
            break
        name, arg = transform_spec(op)  # type: ignore[misc]
        value = apply_transform(name, arg, value)
    return value


def resolve_inputs(
    step: dict[str, Any], results: dict[str, Any], params: dict[str, Any]
) -> tuple[dict[str, Any], list[str]]:
    inputs: dict[str, Any] = {}
    problems: list[str] = []
    for name, b in (step.get("inputs") or {}).items():
        value = resolve_one(b, results, params)
        if value is _MISSING:
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
    """Static prompt + a fixed input block. Same inputs -> byte-identical text."""
    blocks = [step["prompt"]]
    for name in sorted(inputs):
        body = json.dumps(inputs[name], ensure_ascii=False, sort_keys=True, indent=2)
        blocks.append(f"### input: {name}\n```json\n{body}\n```")
    text = "\n\n".join(blocks)
    return text


def step_cache_key(step: dict[str, Any], inputs: dict[str, Any]) -> str:
    """A function of what the agent will actually see — nothing else."""
    return artifact_key({"agent": step.get("agent"), "prompt": step["prompt"], "inputs": inputs})


# ---------------------------------------------------------------------------
# The plan used by the tests
# ---------------------------------------------------------------------------


def base_plan() -> dict[str, Any]:
    return {
        "parameters": {"window": "string"},
        "steps": [
            {
                "id": "collect",
                "agent": "collector",
                "prompt": "Collect the raw log records.",
                "outputs": {"logs": "array", "count": "integer"},
                "artifacts": ["dump"],
            },
            {
                "id": "enrich",
                "agent": "enricher",
                "prompt": "Label each record.",
                "depends_on": ["collect"],
                "inputs": {"logs": {"from": "collect.output.logs", "type": "array"}},
                "outputs": {"labels": "array"},
            },
            {
                "id": "analyze",
                "agent": "analyst",
                "prompt": "Find anomalies.",
                "depends_on": ["collect", "enrich"],
                "inputs": {
                    "ids": {
                        "from": "collect.output.logs",
                        "then": [{"pick": "id"}],
                        "type": "array",
                    },
                    "labels": {"from": "enrich.output.labels", "required": False, "default": []},
                    "window": {"from": "$input.window", "type": "string"},
                },
                "outputs": {"findings": "array"},
            },
            {
                "id": "report",
                "agent": "writer",
                "prompt": "Write the report.",
                "depends_on": ["analyze"],  # collect and enrich are reached only as ancestors
                "inputs": {
                    "top": {"from": "analyze.output.findings", "then": ["first"]},
                    "total": {"from": "collect.output.count", "type": "integer"},
                    "dump": {"artifact": "collect.artifacts.dump"},
                    "notes": {
                        "from": ["enrich.output.labels", "analyze.output.findings"],
                        "combine": "concat",
                    },
                },
                "outputs": {"summary": "string"},
            },
        ],
    }


def mutate(plan_edit) -> list[dict[str, str]]:
    plan = copy.deepcopy(base_plan())
    plan_edit(plan)
    return validate_plan(plan)


def step_of(plan: dict[str, Any], sid: str) -> dict[str, Any]:
    return next(s for s in plan["steps"] if s["id"] == sid)


INVALID_CASES: list[tuple[str, Any, str]] = [
    ("duplicate id", lambda p: p["steps"].append(copy.deepcopy(p["steps"][0])), "duplicate_id"),
    (
        "unknown dependency",
        lambda p: step_of(p, "report").update(depends_on=["analyze", "ghost"]),
        "unknown_dependency",
    ),
    ("dependency cycle", lambda p: step_of(p, "collect").update(depends_on=["report"]), "cycle"),
    (
        "reference to a non-ancestor",
        lambda p: step_of(p, "enrich")["inputs"].update(x={"from": "report.output.summary"}),
        "not_ancestor",
    ),
    (
        "reference to an unknown step",
        lambda p: step_of(p, "enrich")["inputs"].update(x={"from": "ghost.output.a"}),
        "unknown_step",
    ),
    (
        "field the producer does not declare",
        lambda p: step_of(p, "enrich")["inputs"].update(x={"from": "collect.output.nope"}),
        "unknown_output_field",
    ),
    (
        "unknown run parameter",
        lambda p: step_of(p, "enrich")["inputs"].update(x={"from": "$input.nope"}),
        "unknown_param",
    ),
    (
        "artifact the producer does not declare",
        lambda p: step_of(p, "enrich")["inputs"].update(x={"artifact": "collect.artifacts.nope"}),
        "unknown_artifact",
    ),
    (
        "declared type contradicts the source",
        lambda p: step_of(p, "enrich")["inputs"].update(
            x={"from": "collect.output.logs", "type": "integer"}
        ),
        "type_mismatch",
    ),
    (
        "template binding is not a binding kind",
        lambda p: step_of(p, "enrich")["inputs"].update(
            x={"template": "n={{collect.output.count}}"}
        ),
        "unknown_binding_kind",
    ),
    (
        "concat over a non-array",
        lambda p: step_of(p, "report")["inputs"].update(
            x={"from": ["collect.output.count", "collect.output.count"], "combine": "concat"}
        ),
        "bad_combine",
    ),
    (
        "list of sources without combine",
        lambda p: step_of(p, "enrich")["inputs"].update(
            x={"from": ["collect.output.logs", "collect.output.logs"]}
        ),
        "combine_needs_list",
    ),
    (
        "unknown transform",
        lambda p: step_of(p, "enrich")["inputs"].update(
            x={"from": "collect.output.logs", "then": ["shuffle"]}
        ),
        "bad_transform",
    ),
    (
        "transform on the wrong type",
        lambda p: step_of(p, "enrich")["inputs"].update(
            x={"from": "collect.output.count", "then": ["first"]}
        ),
        "bad_transform",
    ),
    (
        "optional without a default",
        lambda p: step_of(p, "enrich")["inputs"].update(
            x={"from": "collect.output.logs", "required": False}
        ),
        "optional_needs_default",
    ),
    (
        "`from` that does not address an output",
        lambda p: step_of(p, "enrich")["inputs"].update(x={"from": "collect.artifacts.dump"}),
        "bad_path",
    ),
    (
        "unknown key in a binding",
        lambda p: step_of(p, "enrich")["inputs"].update(
            x={"from": "collect.output.logs", "eval": "1+1"}
        ),
        "unknown_key",
    ),
]


def test_validator() -> None:
    assert validate_plan(base_plan()) == [], validate_plan(base_plan())
    print(f"  {'invalid plan':<42}{'expected code':<24}reported")
    for title, edit, code in INVALID_CASES:
        codes = sorted({e["code"] for e in mutate(edit)})
        print(f"  {title:<42}{code:<24}{codes}")
        assert code in codes, f"{title}: expected {code}, got {codes}"

    # Every error is reported, not just the first.
    plan = copy.deepcopy(base_plan())
    step_of(plan, "enrich")["inputs"].update(
        a={"from": "ghost.output.a"}, b={"from": "collect.output.nope"}, c={"template": "x"}
    )
    assert {e["code"] for e in validate_plan(plan)} >= {
        "unknown_step",
        "unknown_output_field",
        "unknown_binding_kind",
    }


def test_dataflow() -> None:
    edges = dataflow(base_plan())
    by = {(e["from"], e["to"], e["input"]): e["direct"] for e in edges}
    assert by[("analyze", "report", "top")] is True  # a direct dependency
    assert by[("collect", "report", "total")] is False  # reached only through an ancestor
    assert by[("collect", "report", "dump")] is False
    indirect = sorted(f"{e['from']}->{e['to']}({e['input']})" for e in edges if not e["direct"])
    print(f"  indirect data flow shown to the approver: {indirect}")


# ---------------------------------------------------------------------------
# Resolver and renderer
# ---------------------------------------------------------------------------


def test_resolver() -> None:
    results = {
        "collect": {
            "output": {
                "output": {"logs": [{"id": 1}, {"id": 2}, {"id": 2}], "count": 3},
                "artifacts": {"dump": "/tmp/d.json"},
            }
        },
        "enrich": {"output": {"output": {"labels": ["db", "net"]}, "artifacts": {}}},
    }
    params = {"window": "24h"}

    def one(b):
        return resolve_one(b, results, params)

    assert one({"from": "collect.output.count"}) == 3
    assert one({"from": "$input.window"}) == "24h"
    assert one({"value": [1]}) == [1]
    assert (
        one({"from": "collect.output.logs", "then": [{"pick": "id"}, "unique", {"join": ","}]})
        == "1,2"
    )
    assert one({"from": "collect.output.logs", "then": ["len"]}) == 3
    assert one({"from": ["enrich.output.labels", "enrich.output.labels"], "combine": "concat"}) == [
        "db",
        "net",
        "db",
        "net",
    ]
    assert one({"artifact": "collect.artifacts.dump"}) == "/tmp/d.json"
    assert one({"from": "collect.output.nope"}) is _MISSING
    assert (
        one(
            {
                "from": "enrich.output.labels",
                "then": [
                    "first",
                ],
            }
        )
        == "db"
    )

    # required binding that does not resolve -> a reason, never an exception
    step = {"inputs": {"a": {"from": "collect.output.nope"}}}
    assert resolve_inputs(step, results, params) == (
        {},
        ["input 'a': binding 'collect.output.nope' did not resolve"],
    )
    # optional binding falls back to its explicit default
    step = {"inputs": {"a": {"from": "collect.output.nope", "required": False, "default": []}}}
    assert resolve_inputs(step, results, params) == ({"a": []}, [])
    # first([]) is "nothing to bind", not an error inside the transform
    results["analyze"] = {"output": {"output": {"findings": []}, "artifacts": {}}}
    step = {"inputs": {"top": {"from": "analyze.output.findings", "then": ["first"]}}}
    assert resolve_inputs(step, results, params)[1], "first([]) must be unresolved"
    # run-time type check on the resolved value
    step = {"inputs": {"n": {"from": "collect.output.logs", "type": "integer"}}}
    assert "expected integer" in resolve_inputs(step, results, params)[1][0]
    print("  resolver: refs, params, values, combine, transforms, artifacts, optional, type: OK")


def test_render() -> None:
    step = {"prompt": "Do it."}
    a = render_prompt(step, {"b": {"y": 1, "x": 2}, "a": [3]})
    b = render_prompt(step, {"a": [3], "b": {"x": 2, "y": 1}})
    assert a == b, "rendering must not depend on key order"
    assert a.startswith("Do it.\n\n### input: a\n```json\n[\n  3\n]\n```")
    assert a.index("### input: a") < a.index("### input: b")
    print("  render: byte-identical regardless of key order, inputs sorted by name: OK")


# ---------------------------------------------------------------------------
# End to end through the real plan executor
# ---------------------------------------------------------------------------


@dataclass
class ScriptedAgent:
    """Returns {"output": ..., "artifacts": ...} per step and records the prompt it got."""

    scripts: dict[str, dict[str, Any]]
    prompts: dict[str, str] = field(default_factory=dict)

    async def run(self, step_id: str, prompt: str) -> dict[str, Any]:
        self.prompts[step_id] = prompt
        await asyncio.sleep(0)
        return copy.deepcopy(self.scripts[step_id])


class RunState(PlanState):
    params: Annotated[dict, last]


def agent_scripts(*, dump: str = "/tmp/dump-1.json", logs=None, findings=None) -> dict:
    logs = logs if logs is not None else [{"id": 1, "level": "error"}, {"id": 2, "level": "info"}]
    findings = findings if findings is not None else ["spike at 03:00"]
    return {
        "collect": {"output": {"logs": logs, "count": len(logs)}, "artifacts": {"dump": dump}},
        "enrich": {"output": {"labels": ["db", "net"]}, "artifacts": {}},
        "analyze": {"output": {"findings": findings}, "artifacts": {}},
        "report": {"output": {"summary": "ok"}, "artifacts": {}},
    }


def contract_problems(step: dict[str, Any], reply: dict[str, Any]) -> list[str]:
    out = reply.get("output", {})
    problems = []
    for field_name, expected in (step.get("outputs") or {}).items():
        if field_name not in out:
            problems.append(f"output {field_name!r} missing")
        elif not type_ok(out[field_name], expected):
            problems.append(f"output {field_name!r} is not {expected}")
    for art in step.get("artifacts") or []:
        if art not in reply.get("artifacts", {}):
            problems.append(f"artifact {art!r} missing")
    return problems


def build(plan: dict[str, Any], agent: ScriptedAgent, keys: dict[str, str], checkpointer):
    by_id = {s["id"]: s for s in plan["steps"]}

    async def handler(task: Task, state: dict, ctx: Any) -> TaskResult:
        step = by_id[task.id]
        results = {r["task_id"]: r for r in state.get("plan", {}).get("results") or ()}
        inputs, problems = resolve_inputs(step, results, state.get("params") or {})
        if problems:  # an unresolved binding fails THIS step; it does not raise
            return TaskResult(task.id, failed=True, error="; ".join(problems))
        prompt = render_prompt(step, inputs)
        if len(prompt) > MAX_INPUT_CHARS:
            return TaskResult(task.id, failed=True, error="inputs too large: pass an artifact")
        keys[task.id] = step_cache_key(step, inputs)
        reply = await agent.run(task.id, prompt)
        problems = contract_problems(step, reply)
        if problems:
            return TaskResult(task.id, failed=True, error="contract: " + "; ".join(problems))
        if step.get("checks"):
            ok, reasons = run_checks(reply["output"], step["checks"])
            if not ok:
                return TaskResult(task.id, failed=True, error="checks: " + "; ".join(reasons))
        return TaskResult(task.id, output=reply)  # the envelope: {"output": .., "artifacts": ..}

    async def planner(state, ctx):
        if state.get("plan", {}).get("tasks"):
            return {}
        tasks = [
            Task(s["id"], "agent_step", prerequisites=tuple(s.get("depends_on", [])))
            for s in plan["steps"]
        ]
        return {"plan": {"tasks": tasks}}

    async def idle(state, ctx):
        return {}

    g = Graph(RunState)
    g.add_node("planner", planner)
    add_plan_executor(g, handlers={"agent_step": handler}, on_idle="idle", max_fanout=4)
    g.add_node("idle", idle)
    g.add_edge(START, "planner")
    g.add_edge("planner", "dispatch")
    g.add_edge("idle", END)
    return g.compile(checkpointer=checkpointer, step_limit=4 * len(plan["steps"]) + 10)


async def run_plan(plan, scripts, make_cp) -> tuple[ScriptedAgent, dict[str, str], dict[str, Any]]:
    agent, keys = ScriptedAgent(scripts), {}
    final = await build(plan, agent, keys, make_cp()).invoke(
        {"params": {"window": "24h"}}, thread="bindings"
    )
    return agent, keys, final


async def run_once(label: str, make_cp) -> None:
    print(f"=== [{label}] ===")
    plan = base_plan()
    assert validate_plan(plan) == []

    agent, keys1, final = await run_plan(plan, agent_scripts(), make_cp)
    results = {r["task_id"]: r for r in final["plan"]["results"]}
    assert plan_complete(final) and not any(r["failed"] for r in results.values()), results

    # `report` depends only on `analyze`, yet its prompt carries data from the
    # ancestors `collect` (count, artifact path) and `enrich` (labels via combine).
    report_prompt = agent.prompts["report"]
    assert report_prompt.startswith("Write the report.")
    for needle in (
        '"spike at 03:00"',
        "/tmp/dump-1.json",
        '"db"',
        "### input: total\n```json\n2\n",
    ):
        assert needle in report_prompt, f"missing {needle!r} in the report prompt"
    # the window parameter and the optional labels reached `analyze`
    assert '"24h"' in agent.prompts["analyze"] and '"db"' in agent.prompts["analyze"]
    print("  e2e: complete; report received data from ancestors it does not depend on directly")

    # Cache keys follow the RESOLVED inputs. Same upstream -> same keys.
    _, keys_same, _ = await run_plan(plan, agent_scripts(), make_cp)
    assert keys_same == keys1
    # Upstream result changes -> every step that reads it gets a new key.
    _, keys_logs, _ = await run_plan(
        plan, agent_scripts(logs=[{"id": 9, "level": "warn"}]), make_cp
    )
    assert keys_logs["collect"] == keys1["collect"]  # collect reads nothing
    for sid in ("enrich", "analyze", "report"):
        assert keys_logs[sid] != keys1[sid], f"{sid} must miss the cache when its input changed"
    # A change a step does not read leaves its key alone (only `report` binds the dump path).
    _, keys_dump, _ = await run_plan(plan, agent_scripts(dump="/tmp/dump-2.json"), make_cp)
    assert keys_dump["enrich"] == keys1["enrich"] and keys_dump["analyze"] == keys1["analyze"]
    assert keys_dump["report"] != keys1["report"]
    print("  cache key: stable when unchanged; moves with exactly the inputs that moved")

    # Unresolved binding at run time -> the step FAILS with a reason, nothing raises.
    agent, _, final = await run_plan(plan, agent_scripts(findings=[]), make_cp)
    results = {r["task_id"]: r for r in final["plan"]["results"]}
    assert results["report"]["failed"] is True
    assert "analyze.output.findings" in results["report"]["error"], results["report"]["error"]
    assert "report" not in agent.prompts, "the agent must not be called with a missing input"
    assert not results["analyze"]["failed"]
    print(f"  unresolved binding: report FAILED, agent not called: {results['report']['error']}")

    # A broken contract (declared output missing) fails the producing step and blocks dependents.
    bad = agent_scripts()
    bad["collect"]["output"].pop("count")
    agent, _, final = await run_plan(plan, bad, make_cp)
    results = {r["task_id"]: r for r in final["plan"]["results"]}
    assert results["collect"]["failed"] and "count" in results["collect"]["error"]
    assert set(results) == {"collect"}, "dependents of a failed step must not run"
    print("  contract: collect FAILED (missing output), nothing downstream ran")
    print("  OK\n")


async def main() -> None:
    print("=== validator (static, no model) ===")
    test_validator()
    print("=== data flow, resolver, renderer ===")
    test_dataflow()
    test_resolver()
    test_render()
    print()
    for label, make_cp in checkpointer_factories():
        await run_once(label, make_cp)
    print("All spike assertions passed on both checkpointers.")


if __name__ == "__main__":
    asyncio.run(main())
