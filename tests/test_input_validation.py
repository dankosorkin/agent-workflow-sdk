# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

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
