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

"""Live multi-backend smoke: prompt every installed backend for one word.

Runs whichever backends are available on this machine and prints the streamed
text plus the final TurnEnd. Skips any backend whose CLI / server is missing.

    python examples/agents_demo.py

This is a real end-to-end check, not a mock: it spawns kiro-cli / codex /
claude and calls the local Ollama server.
"""

from __future__ import annotations

import asyncio
import shutil

from agentflow.backends.base import AllowAll
from agentflow.events import TextChunk, ToolCall, TurnEnd

PROMPT = "Reply with exactly the word: pong. Do not use any tools."


async def drive(name: str, backend, request_text: str = PROMPT) -> None:
    print(f"\n=== {name} ===")
    try:
        await backend.start()
    except Exception as exc:  # noqa: BLE001
        print(f"  start failed: {exc}")
        return
    try:
        streamed = []
        final = None
        async for ev in backend.prompt(request_text):
            if isinstance(ev, TextChunk):
                streamed.append(ev.text)
            elif isinstance(ev, ToolCall):
                print(f"  [tool] {ev.name}")
            elif isinstance(ev, TurnEnd):
                final = ev
        print(f"  streamed: {''.join(streamed)!r}")
        if final:
            print(f"  TurnEnd : text={final.text!r} stop={final.stop_reason}")
    except Exception as exc:  # noqa: BLE001
        print(f"  turn failed: {type(exc).__name__}: {exc}")
    finally:
        await backend.close()


async def main() -> None:
    # Ollama (LLM backend) — needs the server up and a model.
    try:
        import httpx  # noqa: F401
        from agentflow.backends.ollama import OllamaBackend
        from agentflow.events import Message

        model = _first_ollama_model()
        if model:
            ollama = OllamaBackend(model)
            print(f"\n=== ollama ({model}) ===")
            await ollama.start()
            try:
                parts, final = [], None
                async for ev in ollama.chat([Message(role="user", content=PROMPT)]):
                    if isinstance(ev, TextChunk):
                        parts.append(ev.text)
                    elif isinstance(ev, TurnEnd):
                        final = ev
                print(f"  streamed: {''.join(parts)!r}")
                if final:
                    print(f"  TurnEnd : text={final.text!r} stop={final.stop_reason}")
            finally:
                await ollama.close()
        else:
            print("\n=== ollama === skipped (server down or no models)")
    except ModuleNotFoundError:
        print("\n=== ollama === skipped (httpx not installed)")

    # Codex (agent backend).
    if shutil.which("codex"):
        from agentflow.backends.codex import CodexBackend

        await drive("codex", CodexBackend(sandbox="read-only", permission=AllowAll()))
    else:
        print("\n=== codex === skipped (not installed)")

    # Claude Code (agent backend).
    if shutil.which("claude"):
        from agentflow.backends.claude_code import ClaudeCodeBackend

        await drive("claude", ClaudeCodeBackend(permission=AllowAll()))
    else:
        print("\n=== claude === skipped (not installed)")

    # Kiro (agent backend) — needs a configured agent; skip if none supplied.
    # Kiro v3 selects an agent as a session mode; without a known agent id we
    # can't guarantee a valid mode, so this is opt-in via KIRO_AGENT.
    import os

    kiro_agent = os.environ.get("KIRO_AGENT")
    if shutil.which("kiro-cli") and kiro_agent:
        from agentflow.backends.kiro import KiroBackend

        await drive("kiro", KiroBackend(kiro_agent, permission=AllowAll()))
    else:
        reason = "not installed" if not shutil.which("kiro-cli") else "set KIRO_AGENT to run"
        print(f"\n=== kiro === skipped ({reason})")


def _first_ollama_model() -> str | None:
    import json
    import urllib.request

    try:
        with urllib.request.urlopen("http://localhost:11434/api/tags", timeout=2) as resp:
            data = json.load(resp)
        models = [m["name"] for m in data.get("models", [])]
        return models[0] if models else None
    except Exception:  # noqa: BLE001
        return None


if __name__ == "__main__":
    asyncio.run(main())
