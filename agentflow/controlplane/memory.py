# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of AgentFlow.
#
# AgentFlow is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""In-memory RunQueue: fast, ephemeral, for tests and single-process use."""

from __future__ import annotations

import asyncio
import dataclasses
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

from agentflow.controlplane.records import QueueStats, RunRecord, RunStatus
from agentflow.errors import RunNotFound

__all__ = ["MemoryRunQueue"]


def _now() -> datetime:
    return datetime.now(UTC)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


class MemoryRunQueue:
    """Keeps run records in a dict, guarded by a lock so ``claim`` is atomic."""

    def __init__(self) -> None:
        self._runs: dict[str, RunRecord] = {}
        self._lock = asyncio.Lock()

    async def enqueue(
        self, graph: str, input: Mapping[str, Any] | None = None, *, thread: str | None = None
    ) -> RunRecord:
        run_id = uuid.uuid4().hex
        ts = _iso(_now())
        rec = RunRecord(
            run_id=run_id,
            graph=graph,
            thread=thread or run_id,
            status=RunStatus.QUEUED,
            input=dict(input or {}),
            created_at=ts,
            updated_at=ts,
        )
        async with self._lock:
            self._runs[run_id] = rec
        return rec

    async def get(self, run_id: str) -> RunRecord | None:
        return self._runs.get(run_id)

    async def list(
        self,
        *,
        status: str | None = None,
        graph: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[RunRecord]:
        recs = [
            r
            for r in self._runs.values()
            if (status is None or r.status == status) and (graph is None or r.graph == graph)
        ]
        recs.sort(key=lambda r: r.created_at, reverse=True)
        return recs[offset : offset + limit]

    async def claim(self, *, lease_seconds: float = 60.0) -> RunRecord | None:
        now = _now()
        async with self._lock:
            candidates = [
                r
                for r in self._runs.values()
                if r.status == RunStatus.QUEUED
                or (
                    r.status == RunStatus.RUNNING
                    and r.lease_until is not None
                    and datetime.fromisoformat(r.lease_until) <= now
                )
            ]
            if not candidates:
                return None
            candidates.sort(key=lambda r: r.created_at)
            chosen = candidates[0]
            claimed = dataclasses.replace(
                chosen,
                status=RunStatus.RUNNING,
                lease_until=_iso(now + timedelta(seconds=lease_seconds)),
                attempt=chosen.attempt + 1,
                updated_at=_iso(now),
            )
            self._runs[chosen.run_id] = claimed
            return claimed

    async def heartbeat(self, run_id: str, *, lease_seconds: float = 60.0) -> None:
        async with self._lock:
            rec = self._require(run_id)
            self._runs[run_id] = dataclasses.replace(
                rec,
                lease_until=_iso(_now() + timedelta(seconds=lease_seconds)),
                updated_at=_iso(_now()),
            )

    async def complete(self, run_id: str, *, status: str, error: str | None = None) -> None:
        async with self._lock:
            rec = self._require(run_id)
            self._runs[run_id] = dataclasses.replace(
                rec,
                status=status,
                error=error,
                lease_until=None,
                updated_at=_iso(_now()),
            )

    async def request_cancel(self, run_id: str) -> bool:
        async with self._lock:
            rec = self._runs.get(run_id)
            if rec is None or rec.status in RunStatus.TERMINAL:
                return False
            if rec.status == RunStatus.QUEUED:
                # Not yet claimed — cancel outright.
                self._runs[run_id] = dataclasses.replace(
                    rec, status=RunStatus.CANCELLED, lease_until=None, updated_at=_iso(_now())
                )
            else:
                # Running / interrupted — flag for the worker to stop.
                self._runs[run_id] = dataclasses.replace(
                    rec, cancel_requested=True, updated_at=_iso(_now())
                )
            return True

    async def enqueue_resume(self, run_id: str, value: Any = None) -> RunRecord:
        async with self._lock:
            rec = self._require(run_id)
            updated = dataclasses.replace(
                rec,
                status=RunStatus.QUEUED,
                resume_value=value,
                error=None,
                lease_until=None,
                cancel_requested=False,
                updated_at=_iso(_now()),
            )
            self._runs[run_id] = updated
            return updated

    async def stats(self) -> QueueStats:
        now = _now()
        by_status: dict[str, int] = {}
        expired = 0
        for rec in self._runs.values():
            by_status[rec.status] = by_status.get(rec.status, 0) + 1
            if (
                rec.status == RunStatus.RUNNING
                and rec.lease_until is not None
                and datetime.fromisoformat(rec.lease_until) <= now
            ):
                expired += 1
        return QueueStats(total=len(self._runs), by_status=by_status, expired_leases=expired)

    def _require(self, run_id: str) -> RunRecord:
        rec = self._runs.get(run_id)
        if rec is None:
            raise RunNotFound(run_id)
        return rec
