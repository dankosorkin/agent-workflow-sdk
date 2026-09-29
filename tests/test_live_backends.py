# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""Live end-to-end smoke tests against real backends.

Opt-in only: marked ``live`` and skipped by the default ``pytest`` run
(``addopts = -m 'not live'``). Run explicitly with:

    pytest -m live

Each test skips itself if its backend isn't installed / reachable, so the
suite runs whatever this machine actually has and never fails on a missing
CLI. These are smoke checks — they assert the backend produces a TurnEnd and
some text, not exact model output.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import urllib.request
from typing import Annotated

import pytest
from agentflow import State, append, last
from agentflow.backends.base import AllowAll
from agentflow.events import Message, TextChunk, TurnEnd

pytestmark = pytest.mark.live

PROMPT = "Reply with exactly the word: pong. Do not use any tools."


class _RedisLiveState(State):
    answer: Annotated[str, last]
    stage: Annotated[list, append]


async def _collect(stream):
    text_parts, final = [], None
    async for ev in stream:
        if isinstance(ev, TextChunk):
            text_parts.append(ev.text)
        elif isinstance(ev, TurnEnd):
            final = ev
    return "".join(text_parts), final


def _ollama_model() -> str | None:
    try:
        with urllib.request.urlopen("http://localhost:11434/api/tags", timeout=2) as resp:
            data = json.load(resp)
        names = [m["name"] for m in data.get("models", [])]
        return names[0] if names else None
    except Exception:  # noqa: BLE001
        return None


async def test_live_ollama():
    model = _ollama_model()
    if not model:
        pytest.skip("ollama not reachable or no models installed")
    from agentflow.backends.ollama import OllamaBackend

    backend = OllamaBackend(model)
    await backend.start()
    try:
        text, final = await _collect(backend.chat([Message(role="user", content=PROMPT)]))
    finally:
        await backend.close()
    assert final is not None
    assert text or (final.text or "")


async def test_live_openai_compatible_via_ollama():
    model = _ollama_model()
    if not model:
        pytest.skip("ollama not reachable (used as an OpenAI-compatible endpoint)")
    from agentflow.backends.openai import OpenAIBackend

    backend = OpenAIBackend(model, base_url="http://localhost:11434/v1", api_key="ollama")
    await backend.start()
    try:
        text, final = await _collect(backend.chat([Message(role="user", content=PROMPT)]))
    finally:
        await backend.close()
    assert final is not None
    assert text or (final.text or "")


async def test_live_openai_real():
    """Real OpenAI-compatible endpoint, when OPENAI_API_KEY is configured."""
    key = os.environ.get("OPENAI_API_KEY")
    if not key:
        pytest.skip("OPENAI_API_KEY not set")
    from agentflow.backends.openai import OpenAIBackend

    model = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
    base_url = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")
    backend = OpenAIBackend(model, base_url=base_url, api_key=key)
    await backend.start()
    try:
        text, final = await _collect(backend.chat([Message(role="user", content=PROMPT)]))
    finally:
        await backend.close()
    assert final is not None
    assert (text + (final.text or "")).strip()


async def test_live_codex():
    if not shutil.which("codex"):
        pytest.skip("codex not installed")
    from agentflow.backends.codex import CodexBackend

    backend = CodexBackend(sandbox="read-only", permission=AllowAll())
    await backend.start()
    try:
        text, final = await _collect(backend.prompt(PROMPT))
    finally:
        await backend.close()
    assert final is not None
    assert (text + (final.text or "")).strip()


async def test_live_claude():
    if not shutil.which("claude"):
        pytest.skip("claude not installed")
    from agentflow.backends.claude_code import ClaudeCodeBackend

    backend = ClaudeCodeBackend(permission=AllowAll())
    await backend.start()
    try:
        text, final = await _collect(backend.prompt(PROMPT))
    finally:
        await backend.close()
    assert final is not None
    assert (text + (final.text or "")).strip()


async def test_live_kiro():
    agent = os.environ.get("KIRO_AGENT")
    if not shutil.which("kiro-cli") or not agent:
        pytest.skip("kiro-cli not installed or KIRO_AGENT not set (e.g. KIRO_AGENT=vibe)")
    from agentflow.backends.base import AllowAll
    from agentflow.backends.kiro import KiroBackend

    backend = KiroBackend(agent, permission=AllowAll())
    await backend.start()
    try:
        text, final = await _collect(backend.prompt(PROMPT))
    finally:
        await backend.close()
    assert final is not None
    assert (text + (final.text or "")).strip()


async def test_live_anthropic():
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        pytest.skip("ANTHROPIC_API_KEY not set")
    from agentflow.backends.anthropic import AnthropicBackend

    model = os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5")
    workspace = os.environ.get("ANTHROPIC_WORKSPACE_ID")
    backend = AnthropicBackend(model, api_key=key, max_tokens=64, workspace_id=workspace)
    await backend.start()
    try:
        text, final = await _collect(backend.chat([Message(role="user", content=PROMPT)]))
    finally:
        await backend.close()
    assert final is not None
    assert (text + (final.text or "")).strip()


async def test_live_redis_checkpointer():
    """RedisCheckpointer against a real Redis (AGENTFLOW_TEST_REDIS_URL)."""
    url = os.environ.get("AGENTFLOW_TEST_REDIS_URL")
    if not url:
        pytest.skip("AGENTFLOW_TEST_REDIS_URL not set (e.g. redis://localhost:6379/0)")

    from agentflow import END, START, Graph
    from agentflow.checkpoint.redis import RedisCheckpointer

    g = Graph(_RedisLiveState)

    async def ask(state, ctx):
        human = await ctx.interrupt({"q": "ok?"})
        return {"answer": human, "stage": "asked"}

    async def finish(state, ctx):
        return {"stage": "finished"}

    g.add_node("ask", ask)
    g.add_node("finish", finish)
    g.add_edge(START, "ask")
    g.add_edge("ask", "finish")
    g.add_edge("finish", END)

    cp = RedisCheckpointer(url, prefix=f"aftest:{os.getpid()}")
    app = g.compile(checkpointer=cp)
    thread = f"live-{os.getpid()}"
    try:
        await app.invoke({"stage": []}, thread=thread)
        state = await app.get_state(thread)
        assert state is not None and state.interrupted
        out = await app.resume(thread, value="yes")
        assert out["answer"] == "yes"
        assert out["stage"] == ["asked", "finished"]
    finally:
        await cp.close()


async def test_live_tool_loop_ollama():
    """Full tool-calling loop over Ollama with a graph-executed tool."""
    model = _ollama_model()
    if not model:
        pytest.skip("ollama not reachable")
    from agentflow.backends.ollama import OllamaBackend
    from agentflow.prebuilt import Tool, tool_loop

    async def multiply(a: float, b: float) -> str:
        return str(a * b)

    tools = [
        Tool(
            "multiply",
            multiply,
            description="Multiply two numbers",
            schema={
                "type": "object",
                "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
                "required": ["a", "b"],
            },
        )
    ]

    llm = OllamaBackend(model)
    await llm.start()
    try:
        app = tool_loop(llm, tools, max_turns=6)
        out = await app.invoke(
            {
                "messages": [
                    Message(
                        role="user", content="Use multiply to compute 6 * 7, then state the result."
                    )
                ]
            }
        )
    finally:
        await llm.close()
    # The final assistant message should exist; if the model used the tool the
    # transcript will contain a tool result.
    assert out["messages"][-1].role == "assistant"
    assert out["turns"] >= 1


async def test_live_postgres_checkpointer():
    """PostgresCheckpointer against a real Postgres (AGENTFLOW_TEST_POSTGRES_DSN)."""
    dsn = os.environ.get("AGENTFLOW_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip(
            "AGENTFLOW_TEST_POSTGRES_DSN not set "
            "(e.g. postgresql://postgres:pw@localhost:5432/postgres)"
        )

    from agentflow import END, START, Graph
    from agentflow.checkpoint.base import Checkpoint
    from agentflow.checkpoint.postgres import PostgresCheckpointer
    from agentflow.errors import CheckpointConflict

    table = f"cp_test_{os.getpid()}"
    cp = PostgresCheckpointer(dsn, table=table)
    thread = f"live-{os.getpid()}"
    try:
        # Direct CAS: revision tracking + conflict on stale if_revision.
        await cp.put(Checkpoint(thread=thread, step=1, state={"n": 1}))
        got = await cp.get(thread, 1)
        assert got is not None and got.revision == 1 and got.state["n"] == 1
        await cp.put(Checkpoint(thread=thread, step=1, state={"n": 2}), if_revision=1)
        assert (await cp.get(thread, 1)).revision == 2
        with pytest.raises(CheckpointConflict):
            await cp.put(Checkpoint(thread=thread, step=1, state={"n": 9}), if_revision=1)
        assert (await cp.get(thread, 1)).state["n"] == 2

        # Retention.
        await cp.put(Checkpoint(thread=thread, step=2, state={"n": 2}))
        await cp.put(Checkpoint(thread=thread, step=3, state={"n": 3}))
        assert await cp.prune(thread, before_step=2) == 1
        assert [c.step async for c in cp.history(thread)] == [2, 3]

        # list_threads summarizes the latest step per thread.
        infos = await cp.list_threads()
        summary = {i.thread: i for i in infos}
        assert thread in summary and summary[thread].latest_step == 3

        # End-to-end HITL interrupt + resume on Postgres.
        g = Graph(_RedisLiveState)

        async def ask(state, ctx):
            human = await ctx.interrupt({"q": "ok?"})
            return {"answer": human, "stage": "asked"}

        async def finish(state, ctx):
            return {"stage": "finished"}

        g.add_node("ask", ask)
        g.add_node("finish", finish)
        g.add_edge(START, "ask")
        g.add_edge("ask", "finish")
        g.add_edge("finish", END)

        app = g.compile(checkpointer=cp)
        hthread = f"hitl-{os.getpid()}"
        await app.invoke({"stage": []}, thread=hthread)
        state = await app.get_state(hthread)
        assert state is not None and state.interrupted
        out = await app.resume(hthread, value="yes")
        assert out["answer"] == "yes"
        assert out["stage"] == ["asked", "finished"]
    finally:
        pool = await cp._get_pool()
        await pool.execute(f"DROP TABLE IF EXISTS {table}")
        await cp.close()


async def test_live_postgres_store():
    """PostgresStore against a real Postgres (AGENTFLOW_TEST_POSTGRES_DSN)."""
    dsn = os.environ.get("AGENTFLOW_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip(
            "AGENTFLOW_TEST_POSTGRES_DSN not set "
            "(e.g. postgresql://postgres:pw@localhost:5432/postgres)"
        )

    from agentflow.store.postgres import PostgresStore

    table = f"store_test_{os.getpid()}"
    s = PostgresStore(dsn, table=table)
    try:
        # Round-trip + upsert preserves created_at.
        first = await s.put(("users", "u1", "mem"), "name", {"v": "Ada"})
        got = await s.get(("users", "u1", "mem"), "name")
        assert got is not None and got.value == {"v": "Ada"}
        second = await s.put(("users", "u1", "mem"), "name", {"v": "Grace"})
        assert second.created_at == first.created_at
        assert (await s.get(("users", "u1", "mem"), "name")).value == {"v": "Grace"}

        # Prefix search + namespace listing, ordered.
        await s.put(("users", "u1", "mem"), "lang", "python")
        await s.put(("users", "u2", "mem"), "name", "Alan")
        await s.put(("orgs", "o1"), "name", "Acme")
        found = await s.search(("users",))
        assert [(i.namespace, i.key) for i in found] == [
            (("users", "u1", "mem"), "lang"),
            (("users", "u1", "mem"), "name"),
            (("users", "u2", "mem"), "name"),
        ]
        assert ("orgs", "o1") in await s.list_namespaces()
        assert await s.list_namespaces(prefix=("users",)) == [
            ("users", "u1", "mem"),
            ("users", "u2", "mem"),
        ]

        # Delete.
        assert await s.delete(("orgs", "o1"), "name") is True
        assert await s.delete(("orgs", "o1"), "name") is False

        # TTL: an expired item disappears from get and search.
        await s.put(("ttl",), "k", "v", ttl=1.0)
        assert (await s.get(("ttl",), "k")).value == "v"
        await asyncio.sleep(1.2)
        assert await s.get(("ttl",), "k") is None
        assert await s.search(("ttl",)) == []
    finally:
        pool = await s._get_pool()
        await pool.execute(f"DROP TABLE IF EXISTS {table}")
        await s.close()


async def test_live_postgres_run_queue():
    """PostgresRunQueue against real Postgres: claim/SKIP LOCKED, lease, cancel."""
    dsn = os.environ.get("AGENTFLOW_TEST_POSTGRES_DSN")
    if not dsn:
        pytest.skip(
            "AGENTFLOW_TEST_POSTGRES_DSN not set "
            "(e.g. postgresql://postgres:pw@localhost:5432/postgres)"
        )

    from agentflow import RunStatus
    from agentflow.controlplane.postgres import PostgresRunQueue

    table = f"runs_test_{os.getpid()}"
    q = PostgresRunQueue(dsn, table=table)
    try:
        # Enqueue N runs; two concurrent claimers must partition them with no
        # duplicates (FOR UPDATE SKIP LOCKED).
        n = 20
        for i in range(n):
            await q.enqueue("g", {"i": i})

        claimed_ids: list[str] = []

        async def claimer():
            got = []
            while True:
                rec = await q.claim(lease_seconds=60)
                if rec is None:
                    return got
                got.append(rec.run_id)
                await asyncio.sleep(0)  # yield to interleave

        a, b = await asyncio.gather(claimer(), claimer())
        claimed_ids = a + b
        assert len(claimed_ids) == n
        assert len(set(claimed_ids)) == n  # no run claimed twice
        assert not (set(a) & set(b))  # disjoint partitions

        # All are now running; a fresh claim finds nothing (leases not expired).
        assert await q.claim(lease_seconds=60) is None

        # Expired-lease reclaim: enqueue one, claim with a tiny lease, wait, reclaim.
        rec = await q.enqueue("g", {"i": 99})
        first = await q.claim(lease_seconds=1)
        assert first is not None
        await asyncio.sleep(1.2)
        again = await q.claim(lease_seconds=60)
        assert again is not None and again.attempt == 2

        # Cancel a queued run is immediate.
        c = await q.enqueue("g")
        assert await q.request_cancel(c.run_id) is True
        assert (await q.get(c.run_id)).status == RunStatus.CANCELLED

        # complete + list filter.
        await q.complete(rec.run_id, status=RunStatus.SUCCEEDED)
        done = await q.list(status=RunStatus.SUCCEEDED)
        assert rec.run_id in {r.run_id for r in done}

        # stats(): depth by status + expired-lease count (the reclaim above
        # left 'again' running with a 60s lease -> not expired).
        stats = await q.stats()
        assert stats.total >= 1
        assert stats.by_status.get(RunStatus.SUCCEEDED, 0) >= 1
        assert stats.expired_leases == 0
    finally:
        pool = await q._get_pool()
        await pool.execute(f"DROP TABLE IF EXISTS {table}")
        await q.close()
