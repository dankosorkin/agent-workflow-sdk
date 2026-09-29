# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""Live tool-calling loop against Ollama.

The model is given a calculator tool and asked a question it must use the tool
to answer. The graph runs the tool and feeds the result back until the model
gives a final answer.

    python examples/tool_loop_ollama.py

Requires a running Ollama with a tool-capable model (e.g. gpt-oss, llama3.1).
"""

from __future__ import annotations

import asyncio
import json
import urllib.request

from agentflow.backends.ollama import OllamaBackend
from agentflow.events import Message
from agentflow.prebuilt import Tool, tool_loop


async def multiply(a: float, b: float) -> str:
    return str(a * b)


TOOLS = [
    Tool(
        "multiply",
        multiply,
        description="Multiply two numbers and return the product.",
        schema={
            "type": "object",
            "properties": {"a": {"type": "number"}, "b": {"type": "number"}},
            "required": ["a", "b"],
        },
    )
]


def _first_model() -> str | None:
    try:
        with urllib.request.urlopen("http://localhost:11434/api/tags", timeout=2) as resp:
            data = json.load(resp)
        names = [m["name"] for m in data.get("models", [])]
        return names[0] if names else None
    except Exception:  # noqa: BLE001
        return None


async def main() -> None:
    model = _first_model()
    if not model:
        print("Ollama not reachable or no models installed; skipping.")
        return

    llm = OllamaBackend(model)
    await llm.start()
    try:
        app = tool_loop(llm, TOOLS, max_turns=6)
        prompt = "What is 23 multiplied by 19? Use the multiply tool, then state the result."
        out = await app.invoke({"messages": [Message(role="user", content=prompt)]})
    finally:
        await llm.close()

    print(f"model: {model}")
    print(f"turns: {out['turns']}")
    print("--- transcript ---")
    for m in out["messages"]:
        if m.role == "assistant" and m.tool_calls:
            calls = ", ".join(f"{c.name}({c.args})" for c in m.tool_calls)
            print(f"  assistant -> tool_call: {calls}")
        elif m.role == "tool":
            print(f"  tool[{m.name}] -> {m.content}")
        else:
            print(f"  {m.role}: {m.content}")
    print("--- final answer ---")
    print(out["messages"][-1].content)


if __name__ == "__main__":
    asyncio.run(main())
