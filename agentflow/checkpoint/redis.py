"""Redis-backed durable checkpointer for distributed / multi-process runs.

Each checkpoint is stored as a JSON string at ``{prefix}:{thread}:cp:{step}``,
and a per-thread sorted set ``{prefix}:{thread}:steps`` (score = step number)
keeps the step index for latest-lookup and ordered history. A write uses a
pipeline transaction so the checkpoint value and its index entry commit
together; re-writing an existing step (a resumed super-step) overwrites the
value and leaves the index idempotent.

Uses redis-py's asyncio client. Requires the ``redis`` extra:
``pip install 'agentic-workflow-sdk[redis]'``.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator

from agentflow.checkpoint._serde import from_payload, to_payload
from agentflow.checkpoint.base import Checkpoint
from agentflow.errors import CheckpointError
from agentflow.redaction import Redactor, redact_none

try:
    import redis.asyncio as aioredis
except ModuleNotFoundError as exc:  # pragma: no cover - import-time guard
    raise ModuleNotFoundError(
        "RedisCheckpointer requires redis. Install the extra: "
        "pip install 'agentic-workflow-sdk[redis]'"
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

    async def close(self) -> None:
        """Close the underlying client (if we own it)."""
        await self._client.aclose()

    # ------------------------------------------------------------------

    async def put(self, cp: Checkpoint) -> str:
        payload = to_payload(cp)
        payload["state"] = self._redact(payload["state"])
        payload["interrupt_payload"] = self._redact(payload["interrupt_payload"])
        try:
            blob = json.dumps(payload, ensure_ascii=False)
        except TypeError as exc:
            raise CheckpointError(
                f"checkpoint state for thread {cp.thread!r} is not JSON-serializable: {exc}"
            ) from exc

        # Atomic: write the value and index the step in one transaction.
        pipe = self._client.pipeline(transaction=True)
        pipe.set(self._cp_key(cp.thread, cp.step), blob)
        pipe.zadd(self._index_key(cp.thread), {str(cp.step): cp.step})
        await pipe.execute()
        return f"{cp.thread}:{cp.step}"

    async def get(self, thread: str, step: int | None = None) -> Checkpoint | None:
        if step is None:
            latest = await self._client.zrange(self._index_key(thread), -1, -1)
            if not latest:
                return None
            step = int(str(latest[0]))
        blob = await self._client.get(self._cp_key(thread, step))
        return from_payload(json.loads(blob)) if blob is not None else None

    async def history(self, thread: str) -> AsyncIterator[Checkpoint]:
        steps = await self._client.zrange(self._index_key(thread), 0, -1)
        for s in steps:
            blob = await self._client.get(self._cp_key(thread, int(str(s))))
            if blob is not None:
                yield from_payload(json.loads(blob))
