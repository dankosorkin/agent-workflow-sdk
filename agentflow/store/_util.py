"""Shared time/TTL helpers for Store backends, so Memory and Postgres agree
on timestamp format and expiry semantics."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

__all__ = ["now_iso", "expiry_iso", "is_expired", "validate_namespace"]


def now_iso() -> str:
    """Current UTC time as an ISO-8601 string (second-ish precision, sortable)."""
    return datetime.now(UTC).isoformat()


def expiry_iso(ttl: float | None, *, base: str | None = None) -> str | None:
    """ISO time ``ttl`` seconds after ``base`` (default now), or None."""
    if ttl is None:
        return None
    start = datetime.fromisoformat(base) if base else datetime.now(UTC)
    return (start + timedelta(seconds=ttl)).isoformat()


def is_expired(expires_at: str | None, *, at: str | None = None) -> bool:
    """True if ``expires_at`` is set and is at/before ``at`` (default now)."""
    if not expires_at:
        return False
    ref = datetime.fromisoformat(at) if at else datetime.now(UTC)
    return datetime.fromisoformat(expires_at) <= ref


def validate_namespace(namespace: tuple[str, ...]) -> None:
    """Reject an empty or malformed namespace early (used on write)."""
    if not isinstance(namespace, tuple):
        raise ValueError(f"namespace must be a tuple, got {type(namespace).__name__}")
    if not namespace:
        raise ValueError("namespace must have at least one segment")
    for seg in namespace:
        if not isinstance(seg, str) or not seg:
            raise ValueError(f"namespace segments must be non-empty strings, got {seg!r}")
