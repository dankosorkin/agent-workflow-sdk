"""Redaction for data written to disk (checkpoints, telemetry).

The library persists workflow state and events — prompts, model output, tool
arguments, and possibly tokens or user data. Nothing is redacted unless you
ask for it. This module provides a small, composable redaction contract so a
caller can strip or mask sensitive values before they are serialized.

A :class:`Redactor` is any callable ``(obj) -> obj`` that returns a
redaction-safe copy. :class:`RedactKeys` masks values whose dict key matches a
set of names (case-insensitive substring), recursively through dicts/lists.
Compose your own for structural rules.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

__all__ = ["Redactor", "RedactKeys", "redact_none"]

Redactor = Callable[[Any], Any]

#: Default sensitive key fragments (case-insensitive substring match).
DEFAULT_SENSITIVE = frozenset(
    {
        "api_key",
        "apikey",
        "authorization",
        "auth",
        "token",
        "secret",
        "password",
        "passwd",
        "x-api-key",
        "access_key",
        "private_key",
        "cookie",
        "bearer",
        "credential",
    }
)

_MASK = "***REDACTED***"


def redact_none(obj: Any) -> Any:
    """No-op redactor — the default. Persists data as-is."""
    return obj


class RedactKeys:
    """Recursively mask dict values whose key matches a sensitive fragment.

    Matching is case-insensitive substring: a key ``"OPENAI_API_KEY"`` matches
    the fragment ``"api_key"``. Lists/tuples are walked; scalars pass through.
    The input is never mutated — a redacted copy is returned.
    """

    def __init__(
        self,
        fragments: Iterable[str] = DEFAULT_SENSITIVE,
        *,
        mask: str = _MASK,
    ) -> None:
        self.fragments = tuple(f.lower() for f in fragments)
        self.mask = mask

    def _is_sensitive(self, key: str) -> bool:
        k = key.lower()
        return any(frag in k for frag in self.fragments)

    def __call__(self, obj: Any) -> Any:
        if isinstance(obj, dict):
            out: dict[Any, Any] = {}
            for k, v in obj.items():
                if isinstance(k, str) and self._is_sensitive(k):
                    out[k] = self.mask
                else:
                    out[k] = self(v)
            return out
        if isinstance(obj, list):
            return [self(v) for v in obj]
        if isinstance(obj, tuple):
            return tuple(self(v) for v in obj)
        return obj
