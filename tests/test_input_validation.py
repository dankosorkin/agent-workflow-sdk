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

"""Input validation at run start: undeclared/reserved keys are rejected."""

from __future__ import annotations

from typing import Annotated

import pytest
from agentflow import END, START, Graph, GraphError, State, add


class S(State):
    n: Annotated[int, add]


def _app():
    g = Graph(S)
    g.add_node("x", lambda s, c: {"n": 1})
    g.add_edge(START, "x")
    g.add_edge("x", END)
    return g.compile()


async def test_valid_input_passes():
    out = await _app().invoke({"n": 5})
    assert out["n"] == 6


async def test_empty_input_passes():
    out = await _app().invoke({})
    assert out["n"] == 1


async def test_unknown_key_rejected():
    with pytest.raises(GraphError, match="undeclared channel"):
        await _app().invoke({"n": 1, "typo": 2})


async def test_reserved_key_rejected():
    with pytest.raises(GraphError):
        await _app().invoke({"__step": 3})


async def test_stream_also_validates():
    with pytest.raises(GraphError):
        async for _ in _app().stream({"nope": 1}):
            pass
