"""Human-in-the-loop permission with a real agent backend.

Runs Codex (or Claude Code) under the Interactive permission policy inside a
graph. When the agent asks to use a tool, the run suspends with an interrupt
carrying the permission request; a human approves or denies via resume().

    python examples/hitl_permission.py

This asks the agent to do something that requires a tool (writing a file), so
a permission prompt is likely. If the agent answers without requesting a tool,
the run simply completes with no interrupt.
"""

from __future__ import annotations

import asyncio
import shutil
from typing import Annotated

from agentflow import END, START, Graph, MemoryCheckpointer, State, last
from agentflow.backends.base import Interactive
from agentflow.events import Allow, Deny, PermissionRequest, TextChunk, TurnEnd


class S(State):
    output: Annotated[str, last]


def pick_agent():
    if shutil.which("codex"):
        from agentflow.backends.codex import CodexBackend

        # workspace-write so the agent actually wants permission to edit.
        return "codex", CodexBackend(sandbox="workspace-write", permission=Interactive())
    if shutil.which("claude"):
        from agentflow.backends.claude_code import ClaudeCodeBackend

        return "claude", ClaudeCodeBackend(permission=Interactive())
    return None, None


async def main() -> None:
    name, agent = pick_agent()
    if agent is None:
        print("Need codex or claude installed; skipping.")
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
                ctx.emit(ev)
            elif isinstance(ev, TurnEnd):
                text = [ev.text] if ev.text else text
        return {"output": "".join(text)}

    g = Graph(S)
    g.add_node("act", act)
    g.add_edge(START, "act")
    g.add_edge("act", END)
    app = g.compile(checkpointer=MemoryCheckpointer())

    try:
        await app.invoke({}, thread="demo")
        state = await app.get_state("demo")

        while state is not None and state.interrupted:
            req: PermissionRequest = state.interrupt_payload
            print(
                f"\n[permission requested] tool={req.tool!r} options="
                f"{[o.kind for o in req.options]}"
            )
            answer = input("approve? [y/N] ").strip().lower()
            decision = Allow() if answer in ("y", "yes") else Deny()
            print(f"  -> {'ALLOW' if isinstance(decision, Allow) else 'DENY'}")
            await app.resume("demo", value=decision)
            state = await app.get_state("demo")

        final = await app.get_state("demo")
        print("\n--- final output ---")
        print(final.state.get("output", ""))
    finally:
        await agent.close()


if __name__ == "__main__":
    asyncio.run(main())
