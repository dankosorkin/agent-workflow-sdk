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

"""FastAPI app factory mapping HTTP routes onto a ``RunQueue``.

Each route is a direct adapter over one queue method; see the package docstring
for the exposed surface and the explicit non-goals (no auth, no worker
protocol). Response/request shapes are Pydantic models so the generated OpenAPI
docs (``/docs``) are accurate.

Note: this module intentionally does NOT use ``from __future__ import
annotations``. FastAPI resolves endpoint annotations with ``get_type_hints``,
which cannot re-evaluate a stringized ``Depends(get_queue)`` that references a
function-local dependency — real annotation objects are required.
"""

from collections.abc import Callable
from typing import Annotated, Any

try:
    from fastapi import Depends, FastAPI, HTTPException, Query
    from pydantic import BaseModel, Field
except ModuleNotFoundError as exc:  # pragma: no cover - import-time guard
    raise ModuleNotFoundError(
        "agentflow.server requires FastAPI. Install the extra: "
        "pip install 'agent-workflow-sdk[server]'"
    ) from exc

from agentflow.controlplane.queue import RunQueue
from agentflow.controlplane.records import QueueStats, RunRecord
from agentflow.errors import RunNotFound

__all__ = ["create_app"]


# ---------------------------------------------------------------------------
# Wire models (what crosses the HTTP boundary)
# ---------------------------------------------------------------------------


class EnqueueRequest(BaseModel):
    """Body for ``POST /runs`` — ask to run a registered graph."""

    graph: str = Field(..., description="Name of a graph registered on the worker side.")
    input: dict[str, Any] = Field(default_factory=dict, description="Initial state for the run.")
    thread: str | None = Field(None, description="Checkpoint thread; defaults to the run id.")


class ResumeRequest(BaseModel):
    """Body for ``POST /runs/{run_id}/resume`` — answer an interrupted run."""

    value: Any = Field(None, description="Value fed to the interrupted node on resume.")


class RunResponse(BaseModel):
    """One run's queue record."""

    run_id: str
    graph: str
    thread: str
    status: str
    input: dict[str, Any] = Field(default_factory=dict)
    resume_value: Any = None
    error: str | None = None
    attempt: int = 0
    created_at: str = ""
    updated_at: str = ""
    lease_until: str | None = None
    wake_at: str | None = None
    cancel_requested: bool = False

    @classmethod
    def of(cls, rec: RunRecord) -> "RunResponse":
        return cls(
            run_id=rec.run_id,
            graph=rec.graph,
            thread=rec.thread,
            status=rec.status,
            input=dict(rec.input),
            resume_value=rec.resume_value,
            error=rec.error,
            attempt=rec.attempt,
            created_at=rec.created_at,
            updated_at=rec.updated_at,
            lease_until=rec.lease_until,
            wake_at=rec.wake_at,
            cancel_requested=rec.cancel_requested,
        )


class CancelResponse(BaseModel):
    cancelled: bool = Field(
        ..., description="True if the run existed and was not already terminal."
    )


class StatsResponse(BaseModel):
    total: int = 0
    by_status: dict[str, int] = Field(default_factory=dict)
    expired_leases: int = 0

    @classmethod
    def of(cls, s: QueueStats) -> "StatsResponse":
        return cls(total=s.total, by_status=dict(s.by_status), expired_leases=s.expired_leases)


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def create_app(
    queue: RunQueue,
    *,
    pool_health: Callable[[], Any] | None = None,
    title: str = "AgentFlow control plane",
) -> FastAPI:
    """Build a FastAPI app over ``queue``.

    ``queue`` is any :class:`~agentflow.controlplane.queue.RunQueue` (Memory for
    a single process, Postgres for a shared/durable one). ``pool_health``, if
    given, is a zero-arg callable returning a
    :class:`~agentflow.controlplane.records.PoolHealth` (typically
    ``pool.health``); ``GET /healthz`` reports it when present. The app adds no
    behavior of its own — every route forwards to a queue method.
    """
    app = FastAPI(title=title, version="1")

    def get_queue() -> RunQueue:
        return queue

    # FastAPI dependency injected via the Annotated form (the modern idiom:
    # keeps Depends() out of the parameter default, so no ruff B008). Inlined in
    # each signature rather than aliased to a local var, because FastAPI
    # resolves annotations by name and would not see a function-local alias.
    @app.post("/runs", response_model=RunResponse, status_code=201, tags=["runs"])
    async def enqueue_run(
        body: EnqueueRequest, q: Annotated[RunQueue, Depends(get_queue)]
    ) -> RunResponse:
        rec = await q.enqueue(body.graph, body.input, thread=body.thread)
        return RunResponse.of(rec)

    @app.get("/runs/{run_id}", response_model=RunResponse, tags=["runs"])
    async def get_run(run_id: str, q: Annotated[RunQueue, Depends(get_queue)]) -> RunResponse:
        rec = await q.get(run_id)
        if rec is None:
            raise HTTPException(status_code=404, detail=f"no run with id {run_id!r}")
        return RunResponse.of(rec)

    @app.get("/runs", response_model=list[RunResponse], tags=["runs"])
    async def list_runs(
        q: Annotated[RunQueue, Depends(get_queue)],
        status: Annotated[str | None, Query()] = None,
        graph: Annotated[str | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> list[RunResponse]:
        recs = await q.list(status=status, graph=graph, limit=limit, offset=offset)
        return [RunResponse.of(r) for r in recs]

    @app.post("/runs/{run_id}/resume", response_model=RunResponse, tags=["runs"])
    async def resume_run(
        run_id: str, body: ResumeRequest, q: Annotated[RunQueue, Depends(get_queue)]
    ) -> RunResponse:
        try:
            rec = await q.enqueue_resume(run_id, body.value)
        except RunNotFound:
            raise HTTPException(status_code=404, detail=f"no run with id {run_id!r}") from None
        return RunResponse.of(rec)

    @app.post("/runs/{run_id}/cancel", response_model=CancelResponse, tags=["runs"])
    async def cancel_run(
        run_id: str, q: Annotated[RunQueue, Depends(get_queue)]
    ) -> CancelResponse:
        cancelled = await q.request_cancel(run_id)
        return CancelResponse(cancelled=cancelled)

    @app.get("/stats", response_model=StatsResponse, tags=["monitoring"])
    async def get_stats(q: Annotated[RunQueue, Depends(get_queue)]) -> StatsResponse:
        return StatsResponse.of(await q.stats())

    @app.get("/healthz", tags=["monitoring"])
    async def healthz() -> dict[str, Any]:
        body: dict[str, Any] = {"status": "ok"}
        if pool_health is not None:
            h = pool_health()
            body["pool"] = {
                "workers": h.workers,
                "alive": h.alive,
                "busy": h.busy,
                "idle": h.idle,
                "healthy": h.healthy,
            }
        return body

    return app
