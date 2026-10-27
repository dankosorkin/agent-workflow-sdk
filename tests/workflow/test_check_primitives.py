from __future__ import annotations

import pytest
from agentflow.workflow import validate_plan
from agentflow.workflow.checks import CHECK_KEYS, CHECKS, parse_path, run_checks, validate_check

OUT = {
    "files": [
        {"name": "a.txt", "lines": 200, "level": "high"},
        {"name": "b.txt", "lines": 120, "level": "mid"},
        {"name": "c.txt", "lines": 80, "level": "low"},
    ],
    "total": 3,
    "summary": "three files",
    "empty": [],
    "flag": True,
}


def passes(check):
    ok, why = run_checks(OUT, [check])
    return ok, why


@pytest.mark.parametrize(
    ("check", "ok"),
    [
        ({"kind": "count", "target": "files", "equals": 3}, True),
        ({"kind": "count", "target": "files", "equals": 2}, False),
        ({"kind": "count", "target": "files", "min": 4}, False),
        ({"kind": "count", "target": "files", "max": 2}, False),
        ({"kind": "count", "target": "summary", "min": 1}, True),
        ({"kind": "count", "target": "total", "min": 1}, False),  # a number has no length
        ({"kind": "count", "target": "files[*]", "min": 1}, False),  # needs ONE value
        ({"kind": "count", "target": "files", "same_count_as": "files"}, True),
        ({"kind": "count", "target": "files", "same_count_as": "empty"}, False),
        ({"kind": "count", "target": "nope", "min": 1}, False),
        ({"kind": "compare", "target": "total", "op": "eq", "value": 3}, True),
        ({"kind": "compare", "target": "total", "op": "eq", "value": 4}, False),
        ({"kind": "compare", "target": "flag", "op": "eq", "value": 1}, False),  # True is not 1
        ({"kind": "compare", "target": "total", "op": "eq", "other": "total"}, True),
        ({"kind": "compare", "target": "total", "op": "ge", "value": 3}, True),
        ({"kind": "compare", "target": "total", "op": "gt", "value": 3}, False),
        ({"kind": "compare", "target": "summary", "op": "gt", "value": 3}, False),  # mixed types
        ({"kind": "compare", "target": "files[*].lines", "op": "gt", "value": 50}, True),
        ({"kind": "compare", "target": "files[*].lines", "op": "gt", "value": 100}, False),
        (
            {
                "kind": "compare",
                "target": "files[*].level",
                "op": "in",
                "value": ["high", "mid", "low"],
            },
            True,
        ),
        ({"kind": "compare", "target": "files[*].level", "op": "in", "value": ["high"]}, False),
        ({"kind": "compare", "target": "files[0].name", "op": "ends_with", "value": ".txt"}, True),
        ({"kind": "compare", "target": "summary", "op": "contains", "value": "three"}, True),
        ({"kind": "compare", "target": "files[*].name", "op": "contains", "value": ".txt"}, True),
        ({"kind": "compare", "target": "files[1].name", "op": "exists"}, True),
        ({"kind": "compare", "target": "files[5].name", "op": "exists"}, False),
        ({"kind": "compare", "target": "files[*].owner", "op": "exists"}, False),
        # an empty [*] match must not pass vacuously
        ({"kind": "compare", "target": "empty[*].x", "op": "eq", "value": 1}, False),
        (
            {
                "kind": "compare",
                "target": "empty[*].x",
                "op": "eq",
                "value": 1,
                "allow_empty": True,
            },
            True,
        ),
        ({"kind": "unique", "target": "files[*].name"}, True),
        ({"kind": "unique", "target": "files[*].level"}, True),
        ({"kind": "sorted", "target": "files", "key": "lines", "order": "desc"}, True),
        ({"kind": "sorted", "target": "files", "key": "lines"}, False),
        ({"kind": "sorted", "target": "files", "key": "name"}, True),
        ({"kind": "sorted", "target": "files", "key": "missing"}, False),
        ({"kind": "sorted", "target": "total"}, False),
    ],
)
def test_primitives(check, ok):
    got, why = passes(check)
    assert got is ok, why


def test_unique_catches_duplicates_and_bool_is_not_a_number():
    out = {"ids": [1, 2, 2], "mixed": [1, True]}
    assert not run_checks(out, [{"kind": "unique", "target": "ids"}])[0]
    assert run_checks(out, [{"kind": "unique", "target": "mixed"}])[0]


def test_failure_reasons_say_what_and_where():
    _, why = passes({"kind": "compare", "target": "files[*].lines", "op": "gt", "value": 100})
    assert "item 2" in why[0] and "80" in why[0]


@pytest.mark.parametrize(
    ("text", "tokens"),
    [
        ("files", ["files"]),
        ("files[*].name", ["files", "*", "name"]),
        ("a.b[2].c", ["a", "b", 2, "c"]),
        ("a-b", ["a-b"]),
        ("", None),
        ("[0]", None),
        ("a..b", None),
        ("a[x]", None),
        ("a b", None),
        (3, None),
    ],
)
def test_parse_path(text, tokens):
    assert parse_path(text) == tokens


OUTPUTS = {"files": "array", "total": "integer"}


@pytest.mark.parametrize(
    ("check", "fragment"),
    [
        ({"kind": "count", "target": "files"}, "needs at least one"),
        ({"kind": "count", "target": "files", "min": "1"}, "integer"),
        ({"kind": "count", "target": "ghost", "min": 1}, "not an output field"),
        ({"kind": "count", "target": "files", "min": 1, "bogus": 1}, "unknown key"),
        ({"kind": "compare", "target": "total", "op": "eq"}, "exactly one"),
        (
            {"kind": "compare", "target": "total", "op": "eq", "value": 1, "other": "total"},
            "exactly one",
        ),
        ({"kind": "compare", "target": "total", "op": "like", "value": 1}, "op must be"),
        ({"kind": "compare", "target": "total", "op": "in", "value": 1}, "list"),
        ({"kind": "compare", "target": "total", "op": "exists", "value": 1}, "no value"),
        ({"kind": "compare", "target": "files[", "op": "exists"}, "valid path"),
        ({"kind": "sorted", "target": "files", "order": "up"}, "order"),
        ({"kind": "unique"}, "missing 'target'"),
        ({"kind": "has_fields", "target": "files"}, "missing 'fields'"),
    ],
)
def test_validate_check_rejects(check, fragment):
    assert any(fragment in p for p in validate_check(check, OUTPUTS))


def test_validate_check_accepts_good_ones_and_ignores_unknown_kinds():
    assert validate_check({"kind": "count", "target": "files", "equals": 3}, OUTPUTS) == []
    assert validate_check({"kind": "unique", "target": "files[*].name"}, OUTPUTS) == []
    assert validate_check({"kind": "semantic", "target": "x"}, OUTPUTS) == []


def test_every_kind_has_a_parameter_spec():
    assert set(CHECKS) == set(CHECK_KEYS)


def test_the_plan_validator_reports_a_bad_check(plan):
    plan["graph"]["steps"][0]["checks"] = [{"kind": "count", "target": "nope", "min": 1}]
    found = [i for i in validate_plan(plan) if i.code == "bad_check"]
    assert found and found[0].step == plan["graph"]["steps"][0]["id"]
