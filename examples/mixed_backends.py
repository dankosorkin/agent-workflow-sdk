# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""One workflow, two backend kinds: an agent backend + an LLM tool loop.

The graph has two stages:

1. ``investigate`` — an AgentBackend (Kiro / Codex / Claude Code) runs a turn
   in the repo and returns free-form text (it can use its own tools).
2. ``summarize`` — an LLMBackend (Ollama / OpenAI-compatible) runs a tool loop
   over that text, with a ``word_count`` tool the graph executes, and produces
   a final structured summary.

Both stages are ordinary nodes over one shared state. The agent stage streams
its events to the run; the LLM stage owns tool execution. Run:

    python examples/mixed_backends.py

The script picks the first available agent backend and the first available LLM
backend, and skips gracefully if neither is present.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import urllib.request
from typing import Annotated

from agentflow import END, START, Graph, State, append, last
from agentflow.backends.base import AllowAll
from agentflow.events import Message, TextChunk, TurnEnd
from agentflow.prebuilt import Tool, tool_loop


class PipelineState(State):
    task: Annotated[str, last]
    findings: Annotated[str, last]
    summary: Annotated[str, last]
    events: Annotated[list, append]


# ---- backend selection (whatever is installed) ----


def pick_agent():
    if shutil.which("codex"):
        from agentflow.backends.codex import CodexBackend

        return "codex", CodexBackend(sandbox="read-only", permission=AllowAll())
    if shutil.which("claude"):
        from agentflow.backends.claude_code import ClaudeCodeBackend

        return "claude", ClaudeCodeBackend(permission=AllowAll())
    agent = os.environ.get("KIRO_AGENT")
    if shutil.which("kiro-cli") and agent:
        from agentflow.backends.kiro import KiroBackend

        return f"kiro:{agent}", KiroBackend(agent, permission=AllowAll())
    return None, None


def pick_llm():
    model = _first_ollama_model()
    if model:
        try:
            from agentflow.backends.ollama import OllamaBackend

            return f"ollama:{model}", OllamaBackend(model)
        except ModuleNotFoundError:
            pass
    if os.environ.get("OPENAI_API_KEY"):
        from agentflow.backends.openai import OpenAIBackend

        return "openai", OpenAIBackend(
            os.environ.get("OPENAI_MODEL", "gpt-4o-mini"),
            api_key=os.environ["OPENAI_API_KEY"],
        )
    return None, None


def _first_ollama_model():
    try:
        with urllib.request.urlopen("http://localhost:11434/api/tags", timeout=2) as resp:
            data = json.load(resp)
        names = [m["name"] for m in data.get("models", [])]
        return names[0] if names else None
    except Exception:  # noqa: BLE001
        return None


async def word_count(text: str) -> str:
    return str(len(text.split()))


async def main() -> None:
    agent_name, agent = pick_agent()
    llm_name, llm = pick_llm()
    print(f"agent backend: {agent_name or '(none)'}")
    print(f"llm backend:   {llm_name or '(none)'}")
    if agent is None or llm is None:
        print("Need one agent backend and one LLM backend; skipping.")
        return

    await agent.start()
    await llm.start()

    async def investigate(state, ctx):
        text_parts = []
        async for ev in agent.prompt(state["task"]):
            if isinstance(ev, TextChunk):
                text_parts.append(ev.text)
                ctx.emit(ev)
            elif isinstance(ev, TurnEnd):
                text_parts = [ev.text] if ev.text else text_parts
        return {"findings": "".join(text_parts), "events": f"investigate:{agent_name}"}

    # The summarize stage is itself a compiled tool_loop graph; we call it from
    # inside a node, composing graphs.
    summarizer = tool_loop(
        llm,
        [
            Tool(
                "word_count",
                word_count,
                description="Count words in a string",
                schema={
                    "type": "object",
                    "properties": {"text": {"type": "string"}},
                    "required": ["text"],
                },
            )
        ],
        max_turns=5,
    )

    async def summarize(state, ctx):
        prompt = (
            "Here are investigation findings:\n\n"
            f"{state['findings']}\n\n"
            "Call word_count on the findings, then reply with one sentence that "
            "includes the word count and a one-line summary."
        )
        out = await summarizer.invoke({"messages": [Message(role="user", content=prompt)]})
        return {"summary": out["messages"][-1].content, "events": f"summarize:{llm_name}"}

    g = Graph(PipelineState)
    g.add_node("investigate", investigate)
    g.add_node("summarize", summarize)
    g.add_edge(START, "investigate")
    g.add_edge("investigate", "summarize")
    g.add_edge("summarize", END)
    app = g.compile()

    try:
        out = await app.invoke(
            {
                "task": "In one short paragraph, say what kind of project this repo is. Do not use tools.",
            }
        )
    finally:
        await agent.close()
        await llm.close()

    print("\n--- findings (from agent) ---")
    print(out["findings"][:500])
    print("\n--- summary (from llm tool loop) ---")
    print(out["summary"])
    print("\nstages:", out["events"])


if __name__ == "__main__":
    asyncio.run(main())
