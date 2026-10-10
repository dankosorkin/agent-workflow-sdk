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

"""Tests for the by_key per-key reducer.

Unit tests fold values through the reducer directly; the integration test
drives a real fan-out through the engine so two concurrent workers each add a
candidate to the same structured channel and the merge is deterministic.
"""

from __future__ import annotations

from typing import Annotated

import pytest
from agentflow import END, START, Graph, State, append, by_key, last, merge

# --- unit: folding rules ---------------------------------------------------


def test_folds_each_key_with_its_own_reducer():
    r = by_key({"intents": append, "events": last})
    current = {"intents": ["i1", "i2"], "events": ["OLD"]}
    out = r(current, {"intents": ["i3"], "events": ["NEW"]})
    assert out == {"intents": ["i1", "i2", "i3"], "events": ["NEW"]}


def test_absent_key_is_left_untouched():
    r = by_key({"intents": append, "events": last})
    current = {"intents": ["i1"], "events": ["OLD"]}
    # update touches only events; intents must survive unchanged
    out = r(current, {"events": ["NEW"]})
    assert out == {"intents": ["i1"], "events": ["NEW"]}


def test_empty_list_update_clears_via_last():
    # {"events": []} is a real last([...], []) -> [], not a no-op
    r = by_key({"events": last})
    out = r({"events": ["A", "B"]}, {"events": []})
    assert out == {"events": []}


def test_none_current_starts_empty():
    r = by_key({"intents": append})
    out = r(None, {"intents": ["i1"]})
    assert out == {"intents": ["i1"]}


def test_empty_update_is_noop_copy():
    r = by_key({"intents": append})
    current = {"intents": ["i1"]}
    for empty in ({}, None):
        out = r(current, empty)
        assert out == {"intents": ["i1"]}
        assert out is not current  # a copy, not the same object


def test_incumbent_last_overwrites_whole():
    # the promotion case: incumbent is replaced whole, not deep-merged
    r = by_key({"incumbent": last})
    current = {"incumbent": {"obj": 10.0, "plan": "IXSCAN"}}
    out = r(current, {"incumbent": {"obj": 7.5}})
    assert out == {"incumbent": {"obj": 7.5}}  # plan is gone, by design


def test_key_not_in_policy_raises():
    r = by_key({"intents": append})
    with pytest.raises(KeyError, match="incumbnet"):
        r({}, {"incumbnet": {}})  # typo -> loud failure


def test_current_key_outside_policy_passes_through():
    # a key already in current that the policy does not govern is preserved
    # as long as the update does not write it
    r = by_key({"intents": append})
    out = r({"legacy": 1, "intents": ["i1"]}, {"intents": ["i2"]})
    assert out == {"legacy": 1, "intents": ["i1", "i2"]}


def test_mixed_policy_with_merge_inside():
    # a nested reducer can itself be merge (shallow) for a sub-dict field
    r = by_key({"bounds": merge, "events": last})
    current = {"bounds": {"lo": 1.0}, "events": ["x"]}
    out = r(current, {"bounds": {"hi": 9.0}, "events": []})
    assert out == {"bounds": {"lo": 1.0, "hi": 9.0}, "events": []}


# --- integration: concurrent fan-out into one by_key channel ---------------


SEARCH = by_key(
    {
        "intents": append,
        "candidates": append,
        "verified": append,
        "events": last,
        "incumbent": last,
    }
)


class OptState(State):
    search: Annotated[dict, SEARCH]


async def test_concurrent_workers_append_into_one_channel():
    """Two workers in the same super-step each add a candidate; both survive.

    This is the dual-state shape: distinct keys of one structured channel
    accumulate differently (candidates grows via append, events replaced via
    last), and concurrent writers do not clobber each other.
    """
    g = Graph(OptState)

    async def planner(state, ctx):
        return {"search": {"intents": ["try_index", "reorder"], "events": ["PLANNED"]}}

    async def worker_a(state, ctx):
        return {"search": {"candidates": [{"id": "a"}]}}

    async def worker_b(state, ctx):
        return {"search": {"candidates": [{"id": "b"}]}}

    async def verifier(state, ctx):
        cands = state["search"]["candidates"]
        # promote a winner whole; clear the events the planner emitted
        return {
            "search": {
                "verified": [{"from": len(cands)}],
                "incumbent": {"obj": 1.0},
                "events": [],
            }
        }

    g.add_node("planner", planner)
    g.add_node("worker_a", worker_a)
    g.add_node("worker_b", worker_b)
    g.add_node("verifier", verifier)
    g.add_edge(START, "planner")
    # fan-out both workers in one super-step
    g.add_conditional_edges("planner", lambda s: ["a", "b"], {"a": "worker_a", "b": "worker_b"})
    g.add_edge("worker_a", "verifier")
    g.add_edge("worker_b", "verifier")
    g.add_edge("verifier", END)

    out = await g.compile().invoke({"search": {}})
    search = out["search"]

    # both workers' candidates merged (frontier order: a then b)
    assert search["candidates"] == [{"id": "a"}, {"id": "b"}]
    # planner's intents preserved through later super-steps
    assert search["intents"] == ["try_index", "reorder"]
    # verifier saw both candidates and promoted a whole incumbent
    assert search["verified"] == [{"from": 2}]
    assert search["incumbent"] == {"obj": 1.0}
    # events were replaced, not appended (last): planner's PLANNED is gone
    assert search["events"] == []
