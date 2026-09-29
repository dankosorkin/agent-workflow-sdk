# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""Redis-backed durable checkpointer for distributed / multi-process runs.

Each checkpoint is stored as a JSON string at ``{prefix}:{thread}:cp:{step}``,
and a per-thread sorted set ``{prefix}:{thread}:steps`` (score = step number)
keeps the step index for latest-lookup and ordered history. A write uses a
pipeline transaction so the checkpoint value and its index entry commit
together; re-writing an existing step (a resumed super-step) overwrites the
value and leaves the index idempotent.

Uses redis-py's asyncio client. Requires the ``redis`` extra:
``pip install 'agent-workflow-sdk[redis]'``.
"""

from __future__ import annotations

import dataclasses
import json
from collections.abc import AsyncIterator

from agentflow.checkpoint._serde import from_payload, to_payload
from agentflow.checkpoint.base import Checkpoint, ThreadInfo
from agentflow.errors import CheckpointConflict, CheckpointError
from agentflow.redaction import Redactor, redact_none

try:
    import redis.asyncio as aioredis
except ModuleNotFoundError as exc:  # pragma: no cover - import-time guard
    raise ModuleNotFoundError(
        "RedisCheckpointer requires redis. Install the extra: "
        "pip install 'agent-workflow-sdk[redis]'"
    ) from exc

__all__ = ["RedisCheckpointer"]


class RedisCheckpointer:
    """Durable checkpointer backed by a Redis server.

    Pass either a ``url`` (``redis://host:port/db``) or an existing async
    client via ``client``. ``prefix`` namespaces all keys.
    """

    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        *,
        client: aioredis.Redis | None = None,
        prefix: str = "agentflow",
        redact: Redactor | None = None,
    ) -> None:
        self._client = client or aioredis.from_url(url, decode_responses=True)
        self.prefix = prefix
        self._redact = redact or redact_none

    # ------------------------------------------------------------------

    def _cp_key(self, thread: str, step: int) -> str:
        return f"{self.prefix}:{thread}:cp:{step}"

    def _index_key(self, thread: str) -> str:
        return f"{self.prefix}:{thread}:steps"

    def _threads_key(self) -> str:
        return f"{self.prefix}:threads"

    async def close(self) -> None:
        """Close the underlying client (if we own it)."""
        await self._client.aclose()

    # ------------------------------------------------------------------

    async def put(self, cp: Checkpoint, *, if_revision: int | None = None) -> str:
        payload = to_payload(cp)
        payload["state"] = self._redact(payload["state"])
        payload["interrupt_payload"] = self._redact(payload["interrupt_payload"])
        key = self._cp_key(cp.thread, cp.step)

        if if_revision is None:
            revision = await self._current_revision(cp.thread, cp.step) + 1
            blob = self._dump_envelope(cp.thread, revision, payload)
            pipe = self._client.pipeline(transaction=True)
            pipe.set(key, blob)
            pipe.zadd(self._index_key(cp.thread), {str(cp.step): cp.step})
            pipe.sadd(self._threads_key(), cp.thread)
            await pipe.execute()
            return f"{cp.thread}:{cp.step}"

        # Conditional (compare-and-set) write via WATCH/MULTI optimistic txn.
        async with self._client.pipeline(transaction=True) as pipe:
            await pipe.watch(key)
            raw = await pipe.get(key)
            current = self._envelope_revision(raw)
            if current != if_revision:
                await pipe.reset()
                raise CheckpointConflict(cp.thread, cp.step, if_revision, current)
            blob = self._dump_envelope(cp.thread, current + 1, payload)
            pipe.multi()
            pipe.set(key, blob)
            pipe.zadd(self._index_key(cp.thread), {str(cp.step): cp.step})
            pipe.sadd(self._threads_key(), cp.thread)
            await pipe.execute()
        return f"{cp.thread}:{cp.step}"

    def _dump_envelope(self, thread: str, revision: int, payload: dict) -> str:
        try:
            return json.dumps({"revision": revision, "payload": payload}, ensure_ascii=False)
        except TypeError as exc:
            raise CheckpointError(
                f"checkpoint state for thread {thread!r} is not JSON-serializable: {exc}"
            ) from exc

    @staticmethod
    def _envelope_revision(raw: bytes | str | None) -> int:
        if raw is None:
            return 0
        return int(json.loads(raw).get("revision", 0))

    async def _current_revision(self, thread: str, step: int) -> int:
        raw = await self._client.get(self._cp_key(thread, step))
        return self._envelope_revision(raw)

    @staticmethod
    def _load(raw: bytes | str) -> Checkpoint:
        env = json.loads(raw)
        return dataclasses.replace(
            from_payload(env["payload"]), revision=int(env.get("revision", 0))
        )

    async def get(self, thread: str, step: int | None = None) -> Checkpoint | None:
        if step is None:
            latest = await self._client.zrange(self._index_key(thread), -1, -1)
            if not latest:
                return None
            step = int(str(latest[0]))
        blob = await self._client.get(self._cp_key(thread, step))
        return self._load(blob) if blob is not None else None

    async def history(self, thread: str) -> AsyncIterator[Checkpoint]:
        steps = await self._client.zrange(self._index_key(thread), 0, -1)
        for s in steps:
            blob = await self._client.get(self._cp_key(thread, int(str(s))))
            if blob is not None:
                yield self._load(blob)

    async def delete_thread(self, thread: str) -> None:
        steps = await self._client.zrange(self._index_key(thread), 0, -1)
        pipe = self._client.pipeline(transaction=True)
        for s in steps:
            pipe.delete(self._cp_key(thread, int(str(s))))
        pipe.delete(self._index_key(thread))
        pipe.srem(self._threads_key(), thread)
        await pipe.execute()

    async def prune(
        self, thread: str, *, before_step: int | None = None, older_than: str | None = None
    ) -> int:
        steps = [int(str(s)) for s in await self._client.zrange(self._index_key(thread), 0, -1)]
        removed = 0
        for step in steps:
            if before_step is not None and step >= before_step:
                continue
            if older_than is not None:
                blob = await self._client.get(self._cp_key(thread, step))
                if blob is None:
                    continue
                ts = json.loads(blob).get("payload", {}).get("ts", "")
                if ts >= older_than:
                    continue
            pipe = self._client.pipeline(transaction=True)
            pipe.delete(self._cp_key(thread, step))
            pipe.zrem(self._index_key(thread), str(step))
            await pipe.execute()
            removed += 1
        return removed

    async def list_threads(self, *, limit: int = 100, offset: int = 0) -> list[ThreadInfo]:
        threads = sorted(str(t) for t in await self._client.smembers(self._threads_key()))
        infos: list[ThreadInfo] = []
        for thread in threads[offset : offset + limit]:
            cp = await self.get(thread)
            if cp is None:
                continue
            infos.append(
                ThreadInfo(
                    thread=thread,
                    latest_step=cp.step,
                    interrupted=cp.interrupted,
                    done=cp.done,
                    ts=cp.ts,
                )
            )
        return infos
