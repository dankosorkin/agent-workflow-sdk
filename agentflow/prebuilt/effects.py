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

"""Exactly-once *effects* in an at-least-once world.

A super-step is atomic over state: if any node in the step raises, the step's
state updates are discarded and no checkpoint is written. That transaction
covers the checkpointer and nothing else. A side effect a node already
performed — a file written, an HTTP POST sent, a row inserted in someone
else's database — is not rolled back, because the engine has no transaction
over it. So on a resume the node runs again and the effect can happen twice.
This is not a bug to fix; it is the delivery contract of every durable
executor without distributed transactions: **at-least-once execution, and
exactly-once only if the effect itself is idempotent.**

``IdempotentOp`` is the primitive that makes that idempotency explicit. It
wraps *one specific effect inside a node* (not the whole node — a node usually
does an effect plus other work) with a claim -> run -> commit marker in a
:class:`~agentflow.store.Store`:

    op = IdempotentOp(store, ("effects", "send_email"))

    async def notify(state, ctx):
        async def send():
            return await email_api.send(state["to"], state["body"],
                                        idempotency_key=key)  # <- pass it downstream!
        key = state["message_id"]
        receipt = await op.run(key, send, on_incomplete="error")
        return {"receipt": receipt}

State machine per key, all in a single store entry so a plain ``get`` reads it
in one shot:

- absent   -> write ``in_flight``, run ``fn``, write ``done(result)``, return it.
- ``done`` -> return the stored result without running ``fn`` again.
- ``in_flight`` -> a previous attempt did not reach ``done``. What to do is a
  policy you choose per effect (``on_incomplete``), because the SDK cannot know
  whether the effect actually completed.

## Two windows this does NOT close

First, the crash gap. There is an irreducible gap between "the effect happened"
and "the marker says done". If the process dies *inside* ``fn`` — the POST left,
the file is half-written — no ``done`` marker exists and the next run sees
``in_flight``; the SDK cannot know if the effect landed.

Second, the lost-response case, which happens even without a crash: ``fn``
performs the effect and then raises (a `TimeoutError` reading the response of a
`POST` that already succeeded). An exception does not prove the effect did not
happen. ``on_error`` controls the marker here — ``"keep"`` (default) preserves
``in_flight`` so the next attempt decides via ``on_incomplete``; ``"release"``
drops it and the retry re-runs the effect, which duplicates it unless the effect
is idempotent. So ``on_incomplete="error"`` protects the crash gap, but with
``on_error="release"`` it does **not** protect the lost-response case — the
marker is already gone.

The real fix for both lives in the effect itself, keyed idempotency downstream:
pass the same key as an idempotency token to the API, write to a temp file at a
*stable* destination path with deterministic content and atomically ``rename``,
use ``INSERT ... ON CONFLICT``. Use the key this primitive gives you as that
token — that is what actually collapses duplicates.

## Concurrency: an atomic claim

The claim is an atomic conditional write — :meth:`Store.put(..., if_absent=True)
<agentflow.store.Store.put>` — not a plain get-then-put. Two workers racing the
same key give exactly one winner (who runs the effect) and one
:class:`~agentflow.errors.StoreConflict` (who re-reads and defers to the
winner's marker). So the "both saw absent and both ran" race is closed, as far
as the store can see it: `MemoryStore` is atomic within a process, and
`PostgresStore` is atomic across processes via a single conditional `INSERT`.

That still does not make the *effect* exactly-once — the winner can crash or
lose its response inside ``fn`` (the windows above). The atomic claim removes
the duplicate-*starter* race; keyed idempotency downstream removes the rest.

The claim's single-runner guarantee also applies only to that first attempt.
``on_incomplete="rerun"`` (see :meth:`IdempotentOp.run`) overwrites the marker
unconditionally, so a rerun can execute ``fn`` concurrently with a winner that
is still running. Reserve ``"rerun"`` for idempotent effects that deduplicate
downstream by the key.

## IdempotentOp vs skip_if_done

:func:`~agentflow.prebuilt.skip_if_done` wraps a whole *node* and caches its
update to avoid recomputing deterministic work; its contract is "recomputing
would be wasteful", and re-running on a lost cache is harmless. ``IdempotentOp``
guards a *side effect*, where re-running is not harmless, and forces you to
handle the interrupted-in-flight case. Reach for ``skip_if_done`` to save work;
reach for ``IdempotentOp`` to protect an effect.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from typing import Any, Literal, TypeVar

from agentflow.errors import AgentFlowError, StoreConflict
from agentflow.store.base import Namespace, Store

__all__ = ["IdempotentOp", "IncompleteEffectError", "effect_key"]

T = TypeVar("T")

OnIncomplete = Literal["error", "rerun", "skip"]
OnError = Literal["keep", "release"]

_IN_FLIGHT = "in_flight"
_DONE = "done"

#: Sentinel returned by _resolve to mean "no stored result — run the effect".
_RUN = object()


class IncompleteEffectError(AgentFlowError):
    """A prior attempt at an effect died between claim and commit.

    Raised by :meth:`IdempotentOp.run` when it finds an ``in_flight`` marker and
    ``on_incomplete="error"`` — the safe default for a non-idempotent effect,
    where blindly re-running could duplicate it. The run suspends the decision
    to a human or a higher-level policy rather than guessing. ``key`` and
    ``namespace`` identify the stuck effect.
    """

    def __init__(self, namespace: Namespace, key: str):
        self.namespace = namespace
        self.key = key
        super().__init__(
            f"effect {key!r} under {namespace} was left in-flight by a prior attempt; "
            f"cannot tell if it completed. Choose on_incomplete='rerun' if the effect "
            f"is idempotent, or resolve it by hand."
        )


def effect_key(*parts: Any) -> str:
    """Build a stable key for an effect from any JSON-serializable parts.

    Serializes with sorted keys and hashes with SHA-256, so the same logical
    effect always maps to the same key regardless of dict ordering. Feed it the
    things that identify the effect (a message id, a target path, the payload) —
    and pass the returned key to the downstream system as its idempotency token
    so duplicates collapse there too.
    """
    encoded = json.dumps(parts, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class IdempotentOp:
    """Run a side effect at most once per key, with an explicit interrupted case.

    ``store`` holds the claim/commit markers; ``namespace`` scopes them (one
    namespace per logical effect reads well, e.g. ``("effects", "send_email")``).
    Markers can expire with ``default_ttl`` — set it when a stale ``in_flight``
    from a long-dead process should eventually be reclaimable.
    """

    def __init__(
        self,
        store: Store,
        namespace: Namespace,
        *,
        default_ttl: float | None = None,
    ) -> None:
        self._store = store
        self._namespace = namespace
        self._default_ttl = default_ttl

    async def run(
        self,
        key: str,
        fn: Callable[[], Awaitable[T]],
        *,
        on_incomplete: OnIncomplete = "error",
        on_error: OnError = "keep",
        ttl: float | None = None,
    ) -> T:
        """Run ``fn`` unless this ``key`` already completed; return its result.

        - Completed before: returns the stored result, ``fn`` is not called.
        - Never started: writes an ``in_flight`` marker, awaits ``fn``, stores
          the result as ``done``, returns it.
        - Left ``in_flight`` by a prior attempt: dispatch on ``on_incomplete`` —
          ``"error"`` raises :class:`IncompleteEffectError` (safe default);
          ``"rerun"`` runs ``fn`` again (only when the effect is idempotent);
          ``"skip"`` assumes it completed and returns ``None``.

        Note on ``"rerun"`` and concurrency: the atomic claim (see the class
        docstring) only guarantees a single runner for the *first* attempt.
        ``"rerun"`` overwrites the existing ``in_flight`` marker
        unconditionally, so if the marker belongs to a winner that is *still
        running* (not a dead attempt), the rerun runs ``fn`` concurrently with
        it. Two simultaneous executions are the result. That is why ``"rerun"``
        is only for a genuinely idempotent effect whose safety rests on
        downstream deduplication by the key, not on this primitive's claim.

        If ``fn`` raises, ``on_error`` decides what the marker is left as, and
        this is a genuine choice you must make — an exception does **not** prove
        the effect did not happen. A `TimeoutError` reading the response of a
        `POST` that already succeeded is an exception *after* the effect landed.

        - ``"keep"`` (default): leave the ``in_flight`` marker, so the next
          attempt hits ``on_incomplete`` and decides deliberately. This is the
          safe default for a non-idempotent effect, because it does not assume
          the effect was skipped.
        - ``"release"``: remove the marker so the next attempt starts from
          ``absent`` and re-runs ``fn`` on the clean path. Correct **only** when
          the exception reliably means the effect did not happen (e.g. a
          connect error before any request was sent). With ``"release"``,
          ``on_incomplete="error"`` cannot protect you from an
          effect-with-a-lost-response — the marker is gone, so the retry just
          runs the effect again. Use ``"release"`` only with downstream
          idempotency (see the class docstring).

        In both cases the exception propagates after the marker is handled.

        The stored result must be JSON-serializable (it round-trips through the
        store). Return an id or receipt, not a live object.
        """
        effective_ttl = ttl if ttl is not None else self._default_ttl
        existing = await self._store.get(self._namespace, key)

        if existing is not None:
            resolved = self._resolve(existing, key, on_incomplete)
            if resolved is not _RUN:
                return resolved  # type: ignore[return-value]
            return await self._handle_incomplete(key, fn, on_incomplete, on_error, effective_ttl)

        # Absent: attempt an atomic claim. If a racing writer claimed first, the
        # conditional write raises StoreConflict; re-read and dispatch on what
        # they left (in_flight or done) instead of running the effect too.
        try:
            return await self._claim_run_commit(
                key, fn, on_error, effective_ttl, conditional=True
            )
        except StoreConflict:
            current = await self._store.get(self._namespace, key)
            if current is None:
                # The winner released/expired between our conflict and re-read;
                # treat as a fresh interrupted claim for the policy to decide.
                raise IncompleteEffectError(self._namespace, key) from None
            resolved = self._resolve(current, key, on_incomplete)
            if resolved is not _RUN:
                return resolved  # type: ignore[return-value]
            return await self._handle_incomplete(key, fn, on_incomplete, on_error, effective_ttl)

    def _resolve(self, item: Any, key: str, on_incomplete: OnIncomplete) -> Any:
        """Map a stored marker to a return value, or _RUN to signal 'act'.

        ``done`` -> the stored result; ``in_flight`` -> _RUN so the caller runs
        the ``on_incomplete`` path; anything else -> _RUN (treat as absent).
        """
        record = item.value
        status = record.get("status") if isinstance(record, dict) else None
        if status == _DONE:
            return record.get("result")
        return _RUN

    async def _handle_incomplete(
        self,
        key: str,
        fn: Callable[[], Awaitable[T]],
        on_incomplete: OnIncomplete,
        on_error: OnError,
        ttl: float | None,
    ) -> T:
        if on_incomplete == "error":
            raise IncompleteEffectError(self._namespace, key)
        if on_incomplete == "skip":
            return None  # type: ignore[return-value]
        # "rerun": the effect is declared idempotent. We already hold an
        # in_flight marker for this key, so overwrite it unconditionally rather
        # than trying to claim again (which would conflict with ourselves).
        return await self._claim_run_commit(key, fn, on_error, ttl, conditional=False)

    async def _claim_run_commit(
        self,
        key: str,
        fn: Callable[[], Awaitable[T]],
        on_error: OnError,
        ttl: float | None,
        *,
        conditional: bool,
    ) -> T:
        # Claim: mark in-flight before touching the outside world. When
        # conditional, this is an ATOMIC create (put if_absent) — two writers
        # racing the same key give one winner and one StoreConflict, so only one
        # runs the effect. The marker also makes an interrupted attempt visible
        # as in_flight on the next try.
        await self._store.put(
            self._namespace, key, {"status": _IN_FLIGHT}, ttl=ttl, if_absent=conditional
        )
        try:
            result = await fn()
        except BaseException:
            # fn raised, but that does NOT prove the effect did not happen (the
            # response may have been lost after the effect landed). on_error is
            # the deliberate choice: "release" drops the marker (retry re-runs
            # on the clean path — only safe if the exception means "not done");
            # "keep" leaves in_flight so the next attempt's on_incomplete
            # decides. Either way the exception propagates.
            if on_error == "release":
                await self._store.delete(self._namespace, key)
            raise
        # Commit: record the result so any later attempt short-circuits.
        await self._store.put(self._namespace, key, {"status": _DONE, "result": result}, ttl=ttl)
        return result
