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

"""Backend lifecycle: async context managers guarantee close; early
consumer termination and cancellation don't leak resources."""

from __future__ import annotations

import pytest
from agentflow.backends.base import AllowAll, BaseAgentBackend, BaseLLMBackend
from agentflow.events import Message, TextChunk, TurnEnd


class _FakeAgent(BaseAgentBackend):
    def __init__(self):
        super().__init__(permission=AllowAll())
        self.started = False
        self.closed = False

    async def start(self):
        self.started = True

    async def close(self):
        self.closed = True

    async def invoke(self, request, *, session=None):
        yield TextChunk(text="hi")
        yield TurnEnd(text="hi", stop_reason="end_turn")


class _FakeLLM(BaseLLMBackend):
    def __init__(self):
        self.started = False
        self.closed = False

    async def start(self):
        self.started = True

    async def close(self):
        self.closed = True

    async def invoke(self, request, *, session=None):
        yield TextChunk(text="hi")
        yield TurnEnd(text="hi", stop_reason="stop")


async def test_agent_context_manager_starts_and_closes():
    b = _FakeAgent()
    async with b as ctx:
        assert ctx is b
        assert b.started is True
        assert b.closed is False
    assert b.closed is True


async def test_llm_context_manager_starts_and_closes():
    b = _FakeLLM()
    async with b:
        assert b.started is True
    assert b.closed is True


async def test_close_runs_even_on_error():
    b = _FakeAgent()
    with pytest.raises(ValueError):
        async with b:
            raise ValueError("boom")
    assert b.closed is True  # __aexit__ closed despite the error


async def test_early_consumer_termination_does_not_prevent_close():
    b = _FakeAgent()
    async with b:
        # Consume only the first event, then stop iterating early.
        async for ev in b.prompt("go"):
            assert isinstance(ev, TextChunk)
            break
    assert b.closed is True


async def test_generator_aclose_is_clean():
    b = _FakeLLM()
    await b.start()
    gen = b.chat([Message(role="user", content="hi")])
    first = await gen.__anext__()
    assert isinstance(first, TextChunk)
    # Closing the generator early must not raise.
    await gen.aclose()
    await b.close()
    assert b.closed is True
