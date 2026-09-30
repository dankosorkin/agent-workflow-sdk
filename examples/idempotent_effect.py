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

"""Protecting a side effect with IdempotentOp so a re-run does not duplicate it.

Run: python examples/idempotent_effect.py

A node "sends" a notification — a stand-in for any real external effect (an
HTTP POST, an email, a file write). Because a super-step is atomic only over
state, a crash or a resume can run the node again, and without protection the
notification would go out twice. IdempotentOp records a claim -> done marker in
a Store keyed by the message, so the effect fires exactly once even though the
node body runs in two separate graph invocations.

The last part shows the honest edge: an attempt that died mid-effect leaves an
"in_flight" marker, and the on_incomplete policy decides what happens next.
Everything here is in-memory so it runs offline.
"""

from __future__ import annotations

import asyncio
from typing import Annotated

from agentflow import END, START, Graph, MemoryStore, State, last
from agentflow.prebuilt import IdempotentOp, IncompleteEffectError, effect_key

# A stand-in "outbox" for a real external system. Every entry here is one
# notification actually delivered to the outside world.
SENT: list[str] = []


class NotifyState(State):
    user: Annotated[str, last]
    message_id: Annotated[str, last]
    receipt: Annotated[str, last]


async def main() -> None:
    store = MemoryStore()
    op = IdempotentOp(store, ("effects", "notify"))

    async def notify(state, ctx):
        # The key identifies the effect. Pass it to the real API as its
        # idempotency token too, so duplicates also collapse downstream.
        key = effect_key("notify", state["message_id"])

        async def send():
            # The real effect. Runs at most once per key.
            line = f"-> {state['user']}: hello (msg {state['message_id']})"
            SENT.append(line)
            return {"delivered_to": state["user"], "token": key[:8]}

        receipt = await op.run(key, send, on_incomplete="error")
        return {"receipt": receipt["token"]}

    g = Graph(NotifyState)
    g.add_node("notify", notify)
    g.add_edge(START, "notify")
    g.add_edge("notify", END)
    app = g.compile()

    payload = {"user": "ada", "message_id": "m-42"}

    print("First run:")
    out1 = await app.invoke(payload, thread="run-1")
    print(f"  receipt={out1['receipt']}  outbox size={len(SENT)}")

    print("Second run (a resume / retry with the same message):")
    out2 = await app.invoke(payload, thread="run-2")
    print(f"  receipt={out2['receipt']}  outbox size={len(SENT)}")

    print(f"\nDelivered exactly once despite two runs: {SENT}")
    assert len(SENT) == 1, "effect should have fired exactly once"

    # --- the interrupted-in-flight case ---
    print("\nNow simulate an attempt that died mid-effect (an in_flight marker):")
    stuck_key = effect_key("notify", "m-99")
    await store.put(("effects", "notify"), stuck_key, {"status": "in_flight"})

    async def send_stuck():
        SENT.append("-> bob: hello (msg m-99)")
        return {"delivered_to": "bob"}

    try:
        await op.run(stuck_key, send_stuck, on_incomplete="error")
    except IncompleteEffectError as exc:
        print(f"  on_incomplete='error' refused to guess: {type(exc).__name__}")
        print("  -> a human or a higher-level gate now decides whether it landed.")
    print(f"  outbox unchanged: {len(SENT)} delivered")


if __name__ == "__main__":
    asyncio.run(main())
