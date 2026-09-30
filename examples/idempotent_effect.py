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

"""IdempotentOp under a realistic failure: a delivered effect whose response is lost.

Run: python examples/idempotent_effect.py

A super-step is atomic only over state, so a resume can run a node again and
repeat its side effect. This example models the hard case honestly:

1. The effect (a stand-in "delivery") succeeds, but reading its response fails
   with a TimeoutError — so the node raises *after* the effect already landed.
2. IdempotentOp with the default ``on_error="keep"`` leaves an ``in_flight``
   marker (an exception does not prove the effect was skipped).
3. The retry sees ``in_flight`` and, with ``on_incomplete="error"``, refuses to
   blindly repeat — it surfaces the ambiguity instead of double-delivering.
4. Finally we show what actually collapses a duplicate end-to-end: the
   downstream system deduplicates on the effect key, so even a re-run is safe.

Everything is in-memory, so it runs offline. The "network" is a function that
delivers to a keyed outbox and then loses its response the first time.
"""

from __future__ import annotations

import asyncio

from agentflow import MemoryStore
from agentflow.prebuilt import IdempotentOp, IncompleteEffectError, effect_key

# The downstream system. It deduplicates on the idempotency key: a delivery
# with a key it has already seen is recorded once. This is the real
# exactly-once mechanism — the orchestrator cannot provide it.
DELIVERED: dict[str, str] = {}


async def deliver(idempotency_key: str, payload: str) -> str:
    """Deliver to the keyed outbox. Idempotent: a repeat key is a no-op."""
    if idempotency_key not in DELIVERED:
        DELIVERED[idempotency_key] = payload
    return f"receipt:{idempotency_key[:8]}"


async def main() -> None:
    store = MemoryStore()
    op = IdempotentOp(store, ("effects", "deliver"))
    key = effect_key("deliver", "msg-42")
    payload = "hello ada"

    # --- Attempt 1: the effect lands, but the response is lost ---
    print("Attempt 1: effect succeeds, then the response is lost")

    async def send_but_lose_response():
        await deliver(key, payload)  # <- actually delivered
        raise TimeoutError("connection dropped before the receipt came back")

    try:
        await op.run(key, send_but_lose_response, on_incomplete="error", on_error="keep")
    except TimeoutError as exc:
        print(f"  node raised: {exc}")
    marker = await store.get(("effects", "deliver"), key)
    print(f"  marker left as: {marker.value['status']!r}  (delivered so far: {len(DELIVERED)})")

    # --- Attempt 2 (the resume): IdempotentOp refuses to guess ---
    print("\nAttempt 2 (resume): on_incomplete='error' will not blindly repeat")

    async def send_again():
        await deliver(key, payload)
        return "receipt"

    try:
        await op.run(key, send_again, on_incomplete="error")
        print("  ERROR: should not have run")
    except IncompleteEffectError:
        print(f"  refused — a human/gate decides. deliveries still: {len(DELIVERED)}")

    # --- What makes a retry safe: downstream dedup on the same key ---
    print("\nSuppose the operator judges it safe to retry (effect is keyed):")
    receipt = await op.run(key, send_again, on_incomplete="rerun")
    print(f"  reran with on_incomplete='rerun' -> {receipt}")
    print(f"  downstream deduplicated on the key: deliveries = {len(DELIVERED)}")
    assert len(DELIVERED) == 1, "the keyed outbox must hold exactly one delivery"

    print("\nExactly-once held because the effect itself is keyed;")
    print("IdempotentOp narrowed the window and forced the decision to be explicit.")


if __name__ == "__main__":
    asyncio.run(main())
