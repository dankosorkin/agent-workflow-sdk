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

import json
import os
import shutil
import urllib.request

import pytest

from agentflow.backends.base import AllowAll
from agentflow.events import Message, TextChunk, TurnEnd

pytestmark = pytest.mark.live

PROMPT = "Reply with exactly the word: pong. Do not use any tools."


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
    from agentflow.backends.kiro import KiroBackend

    backend = KiroBackend(agent)
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


async def test_live_tool_loop_ollama():
    """Full tool-calling loop over Ollama with a graph-executed tool."""
    model = _ollama_model()
    if not model:
        pytest.skip("ollama not reachable")
    from agentflow.backends.ollama import OllamaBackend
    from agentflow.prebuilt import Tool, tool_loop

    async def multiply(a: float, b: float) -> str:
        return str(a * b)

    tools = [Tool("multiply", multiply, description="Multiply two numbers",
                  schema={"type": "object",
                          "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
                          "required": ["a", "b"]})]

    llm = OllamaBackend(model)
    await llm.start()
    try:
        app = tool_loop(llm, tools, max_turns=6)
        out = await app.invoke({"messages": [
            Message(role="user", content="Use multiply to compute 6 * 7, then state the result.")
        ]})
    finally:
        await llm.close()
    # The final assistant message should exist; if the model used the tool the
    # transcript will contain a tool result.
    assert out["messages"][-1].role == "assistant"
    assert out["turns"] >= 1
