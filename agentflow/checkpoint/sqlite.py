"""Transactional SQLite checkpointer for durable, multi-writer-safe runs.

Unlike :class:`~agentflow.checkpoint.file.FileCheckpointer` (single-writer,
one JSON per step), this stores every checkpoint as a row in a SQLite database
and writes it inside a transaction. Each ``(thread, step)`` is a primary key
with a monotonic ``revision``; a write to an existing step bumps the revision
atomically, so a resume that re-runs a super-step overwrites cleanly and two
writers cannot silently corrupt history — the last committed transaction wins
and the revision records the overwrite.

``sqlite3`` is stdlib but blocking, so every call runs in a worker thread via
``asyncio.to_thread`` to preserve the async contract. WAL mode is enabled so a
reader and a writer don't block each other.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

from agentflow.checkpoint.base import Checkpoint
from agentflow.errors import CheckpointError
from agentflow.redaction import Redactor, redact_none

__all__ = ["SqliteCheckpointer"]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS checkpoints (
    thread   TEXT    NOT NULL,
    step     INTEGER NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1,
    payload  TEXT    NOT NULL,
    ts       TEXT    NOT NULL,
    PRIMARY KEY (thread, step)
);
CREATE INDEX IF NOT EXISTS idx_checkpoints_thread ON checkpoints(thread, step);
"""


class SqliteCheckpointer:
    """Durable checkpointer backed by a SQLite database file."""

    def __init__(
        self,
        path: Path | str = ".runs/checkpoints.db",
        *,
        redact: Redactor | None = None,
    ) -> None:
        self.path = Path(path)
        self._redact = redact or redact_none
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_done = False
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, isolation_level=None, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    async def _ensure_schema(self) -> None:
        if self._init_done:
            return
        async with self._lock:
            if self._init_done:
                return
            await asyncio.to_thread(self._init_schema)
            self._init_done = True

    def _init_schema(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(_SCHEMA)
        finally:
            conn.close()

    # ------------------------------------------------------------------

    async def put(self, cp: Checkpoint) -> str:
        await self._ensure_schema()
        payload = _to_payload(cp)
        payload["state"] = self._redact(payload["state"])
        payload["interrupt_payload"] = self._redact(payload["interrupt_payload"])
        try:
            blob = json.dumps(payload, ensure_ascii=False)
        except TypeError as exc:
            raise CheckpointError(
                f"checkpoint state for thread {cp.thread!r} is not JSON-serializable: {exc}"
            ) from exc
        await asyncio.to_thread(self._put_row, cp.thread, cp.step, blob, cp.ts)
        return f"{cp.thread}:{cp.step}"

    def _put_row(self, thread: str, step: int, blob: str, ts: str) -> None:
        conn = self._connect()
        try:
            # One transaction: upsert with an atomic revision bump. A re-run of
            # the same step (resume) overwrites and increments revision.
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO checkpoints (thread, step, revision, payload, ts)
                VALUES (?, ?, 1, ?, ?)
                ON CONFLICT(thread, step) DO UPDATE SET
                    revision = revision + 1,
                    payload = excluded.payload,
                    ts = excluded.ts
                """,
                (thread, step, blob, ts),
            )
            conn.execute("COMMIT")
        except sqlite3.Error as exc:
            conn.execute("ROLLBACK")
            raise CheckpointError(f"sqlite put failed: {exc}") from exc
        finally:
            conn.close()

    async def get(self, thread: str, step: int | None = None) -> Checkpoint | None:
        await self._ensure_schema()
        row = await asyncio.to_thread(self._get_row, thread, step)
        return _from_payload(json.loads(row)) if row is not None else None

    def _get_row(self, thread: str, step: int | None) -> str | None:
        conn = self._connect()
        try:
            if step is None:
                cur = conn.execute(
                    "SELECT payload FROM checkpoints WHERE thread=? ORDER BY step DESC LIMIT 1",
                    (thread,),
                )
            else:
                cur = conn.execute(
                    "SELECT payload FROM checkpoints WHERE thread=? AND step=?",
                    (thread, step),
                )
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    async def history(self, thread: str) -> AsyncIterator[Checkpoint]:
        await self._ensure_schema()
        rows = await asyncio.to_thread(self._history_rows, thread)
        for blob in rows:
            yield _from_payload(json.loads(blob))

    def _history_rows(self, thread: str) -> list[str]:
        conn = self._connect()
        try:
            cur = conn.execute(
                "SELECT payload FROM checkpoints WHERE thread=? ORDER BY step ASC",
                (thread,),
            )
            return [r[0] for r in cur.fetchall()]
        finally:
            conn.close()


def _to_payload(cp: Checkpoint) -> dict[str, Any]:
    return {
        "thread": cp.thread,
        "step": cp.step,
        "state": dict(cp.state),
        "next": list(cp.next),
        "parent": cp.parent,
        "ts": cp.ts,
        "interrupted": cp.interrupted,
        "interrupt_node": cp.interrupt_node,
        "interrupt_payload": cp.interrupt_payload,
        "extra": dict(cp.extra),
    }


def _from_payload(d: dict[str, Any]) -> Checkpoint:
    return Checkpoint(
        thread=d["thread"],
        step=d["step"],
        state=d["state"],
        next=tuple(d.get("next", ())),
        parent=d.get("parent"),
        ts=d.get("ts", ""),
        interrupted=d.get("interrupted", False),
        interrupt_node=d.get("interrupt_node"),
        interrupt_payload=d.get("interrupt_payload"),
        extra=d.get("extra", {}),
    )
