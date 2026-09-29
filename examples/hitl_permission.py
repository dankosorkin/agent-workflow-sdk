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

"""Human-in-the-loop permission with a real agent backend.

Runs Kiro under the Callback permission policy inside a graph. When the agent
asks to use a tool, the policy asks a human inline — within the same open turn —
and answers the request immediately. No graph interrupt/resume is involved.

    KIRO_AGENT=vibe python examples/hitl_permission.py

Kiro is used because it is the only backend that routes tool requests through
our PermissionPolicy (see the capability matrix in the README). Codex and
Claude Code manage permissions via their own CLI sandbox/flags and will not
surface a PermissionRequest here.

Why Callback and not Interactive here: Interactive suspends the whole run via
ctx.interrupt and needs a separate resume(). With a persistent-session backend
like Kiro, the agent holds one turn open awaiting the decision, so abandoning
that turn to resume a new one deadlocks. Callback answers in-place, so the turn
proceeds. Interactive remains the right choice for one-shot backends and for
durable, cross-process approval.
"""

from __future__ import annotations

import asyncio
import shutil
from typing import Annotated

from agentflow import END, START, Graph, State, last
from agentflow.backends.base import Callback
from agentflow.events import PermissionRequest, TextChunk, TurnEnd


class S(State):
    output: Annotated[str, last]


async def ask_human(req: PermissionRequest) -> str:
    """Prompt the operator to approve/deny a tool, inline (off the event loop)."""
    resource = (
        req.detail.get("_meta", {}).get("kiro", {}).get("consent", {}).get("resource")
        if isinstance(req.detail, dict)
        else None
    )
    prompt = (
        f"\n[permission requested] tool={req.tool!r}"
        + (f" resource={resource!r}" if resource else "")
        + f" options={[o.kind for o in req.options]}\napprove? [y/N] "
    )
    # input() blocks; run it off-thread so the backend's reader task keeps
    # draining the agent's output while we wait for the human.
    answer = (await asyncio.to_thread(input, prompt)).strip().lower()
    approved = answer in ("y", "yes", "да", "д")
    print(f"  -> {'ALLOW' if approved else 'DENY'}")
    return "y" if approved else "n"


def pick_agent():
    # Kiro is the only backend that routes tool requests through our
    # PermissionPolicy. Codex and Claude Code manage permissions via their own
    # CLI sandbox/flags and will NOT surface a PermissionRequest here.
    import os

    kiro_agent = os.environ.get("KIRO_AGENT", "vibe")
    if shutil.which("kiro-cli"):
        from agentflow.backends.kiro import KiroBackend

        return "kiro", KiroBackend(kiro_agent, permission=Callback(ask_human))
    if shutil.which("claude"):
        from agentflow.backends.claude_code import ClaudeCodeBackend

        return "claude", ClaudeCodeBackend(permission=Callback(ask_human))
    return None, None


async def main() -> None:
    name, agent = pick_agent()
    if agent is None:
        print("Need kiro-cli (or claude) installed; skipping.")
        return
    print(f"agent: {name}")
    await agent.start()

    async def act(state, ctx):
        text = []
        async for ev in agent.prompt(
            "Create a file called hitl_demo.txt containing the word 'approved'."
        ):
            if isinstance(ev, TextChunk):
                text.append(ev.text)
            elif isinstance(ev, PermissionRequest):
                ctx.emit(ev)  # surface to telemetry; the policy answers it inline
            elif isinstance(ev, TurnEnd):
                text = [ev.text] if ev.text else text
        return {"output": "".join(text)}

    g = Graph(S)
    g.add_node("act", act)
    g.add_edge(START, "act")
    g.add_edge("act", END)
    app = g.compile()

    try:
        out = await app.invoke({}, thread="demo")
        print("\n--- final output ---")
        print(out.get("output", ""))
    finally:
        await agent.close()


if __name__ == "__main__":
    asyncio.run(main())
