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

"""A structured convention for human-in-the-loop interrupt payloads.

``ctx.interrupt(payload)`` accepts any value, and the runtime persists it in
the interrupted checkpoint for a human to inspect. Free-form payloads are fine
for a one-off, but a control plane or UI that has to render *every* pause needs
to know what kind of question it is looking at. This module is that shared
shape: a small set of payloads with a ``type`` discriminator, plus a helper to
read it back.

It is a convention, not runtime magic — these are plain dataclasses you pass to
``ctx.interrupt``. The prebuilt ``Interactive`` permission policy already
surfaces a machine-readable ``PermissionRequest``; these types generalize that
to the other pauses a long run takes: gating on quality and asking a human to
clarify.

    ask = ClarificationRequest(question="Which region?", options=["eu", "us"])
    answer = await ctx.interrupt(ask.to_payload())
    # resume(thread, value=...) delivers the answer

On the other side, a UI switches on the ``type``::

    kind = interrupt_type(checkpoint.interrupt_payload)
    if kind == "quality_gate": ...
    elif kind == "clarification": ...
    elif kind == "permission": ...
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

__all__ = [
    "InterruptType",
    "QualityGateReview",
    "ClarificationRequest",
    "interrupt_type",
]

#: The discriminator values this convention defines. ``"permission"`` is the
#: shape the ``Interactive`` policy already emits (a ``PermissionRequest``);
#: the others are defined here.
InterruptType = Literal["quality_gate", "permission", "clarification"]


@dataclass(frozen=True)
class QualityGateReview:
    """Ask a human to accept or reject an artifact a gate could not clear.

    Carry the machine's own assessment (``auto_score``, ``reasons``) alongside
    the artifact and the criteria, so the reviewer sees why it was escalated.
    Resume with, for example, ``{"decision": "accept" | "reject",
    "feedback": "..."}``.
    """

    artifact: Any
    criteria: list[str] = field(default_factory=list)
    auto_score: float | None = None
    reasons: list[str] = field(default_factory=list)
    gate: str | None = None

    def to_payload(self) -> dict[str, Any]:
        """Render to a plain dict with a ``type`` discriminator for interrupt()."""
        return {"type": "quality_gate", **asdict(self)}


@dataclass(frozen=True)
class ClarificationRequest:
    """Ask a human a question mid-run when the agent is genuinely uncertain.

    ``options`` is an optional closed set of acceptable answers; when empty the
    answer is free text. Resume with the chosen value.
    """

    question: str
    options: list[str] = field(default_factory=list)
    context: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        """Render to a plain dict with a ``type`` discriminator for interrupt()."""
        return {"type": "clarification", **asdict(self)}


def interrupt_type(payload: Any) -> str | None:
    """Return the ``type`` discriminator of an interrupt ``payload``, if any.

    Recognizes a dict with a ``type`` key (what :meth:`to_payload` produces) and
    a ``PermissionRequest`` (the shape the ``Interactive`` policy raises),
    reported as ``"permission"``. Returns ``None`` for an unstructured payload,
    so a caller can fall back to treating it as free-form.
    """
    if isinstance(payload, dict):
        t = payload.get("type")
        return t if isinstance(t, str) else None
    # A PermissionRequest (duck-typed to avoid importing the events module's
    # concrete class into this dependency-light convention module).
    if hasattr(payload, "tool") and hasattr(payload, "options"):
        return "permission"
    return None
