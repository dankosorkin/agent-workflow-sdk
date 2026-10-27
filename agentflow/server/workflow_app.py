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

"""HTTP adapter for :class:`agentflow.workflow.service.WorkflowService` (``server`` extra).

Routes are listed in the service module docstring. No authentication: ``serve`` listens
on ``127.0.0.1`` only. This module does not use ``from __future__ import annotations``
because FastAPI resolves endpoint annotations at runtime.
"""

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

from agentflow.workflow.runner import PlanRefused
from agentflow.workflow.service import Conflict, NotFound, WorkflowService

__all__ = ["create_workflow_app", "serve"]


def create_workflow_app(service: WorkflowService, *, cors_origins: list[str] | None = None):
    """The FastAPI app over ``service``. Needs the ``server`` extra."""
    try:
        from fastapi import FastAPI, HTTPException
        from fastapi.middleware.cors import CORSMiddleware
        from pydantic import BaseModel, Field
    except ModuleNotFoundError as exc:  # pragma: no cover - import-time guard
        raise ModuleNotFoundError(
            "the workflow server requires FastAPI: pip install 'agent-workflow-sdk[server]'"
        ) from exc

    class DesignRequest(BaseModel):
        request: str

    class AnswerRequest(BaseModel):
        answer: Any

    class ApproveRequest(BaseModel):
        by: str

    class RunRequest(BaseModel):
        params: dict[str, Any] = Field(default_factory=dict)

    @asynccontextmanager
    async def lifespan(_app: Any) -> AsyncIterator[None]:
        yield
        await service.close()

    app = FastAPI(title="AgentFlow workflow service", version="1", lifespan=lifespan)
    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_methods=["GET", "POST"],
            allow_headers=["content-type"],
        )

    def call(fn: Callable[..., Any], *args: Any) -> Any:
        try:
            return fn(*args)
        except NotFound as exc:
            raise HTTPException(404, str(exc)) from exc
        except Conflict as exc:
            raise HTTPException(409, str(exc)) from exc
        except PlanRefused as exc:
            raise HTTPException(422, [i.to_dict() for i in exc.issues]) from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @app.get("/inventory")
    async def inventory() -> dict[str, Any]:
        return service.inventory()

    @app.post("/designs", status_code=201)
    async def start_design(body: DesignRequest) -> dict[str, Any]:
        return call(service.start_design, body.request)

    @app.get("/designs")
    async def list_designs() -> list[dict[str, Any]]:
        return service.list_designs()

    @app.get("/designs/{design_id}")
    async def get_design(design_id: str) -> dict[str, Any]:
        return call(service.get_design, design_id)

    @app.post("/designs/{design_id}/answer")
    async def answer_design(design_id: str, body: AnswerRequest) -> dict[str, Any]:
        return call(service.answer_design, design_id, str(body.answer))

    @app.get("/plans")
    async def list_plans() -> list[dict[str, Any]]:
        return service.list_plans()

    @app.get("/plans/{plan_id}")
    async def get_plan(plan_id: str) -> dict[str, Any]:
        return call(service.get_plan, plan_id)

    @app.get("/plans/{plan_id}/issues")
    async def plan_issues(plan_id: str) -> list[dict[str, Any]]:
        return [i.to_dict() for i in call(service.check_plan, plan_id)]

    @app.post("/plans/{plan_id}/approve")
    async def approve(plan_id: str, body: ApproveRequest) -> dict[str, Any]:
        return call(service.approve, plan_id, body.by)

    @app.post("/plans/{plan_id}/runs", status_code=201)
    async def start_run(plan_id: str, body: RunRequest) -> dict[str, Any]:
        return call(service.start_run, plan_id, body.params)

    @app.get("/runs")
    async def list_runs() -> list[dict[str, Any]]:
        return service.list_runs()

    @app.get("/runs/{run_id}")
    async def get_run(run_id: str) -> dict[str, Any]:
        return call(service.get_run, run_id)

    @app.post("/runs/{run_id}/answer")
    async def answer_run(run_id: str, body: AnswerRequest) -> dict[str, Any]:
        return call(service.answer_run, run_id, body.answer)

    @app.post("/runs/{run_id}/resume")
    async def resume_run(run_id: str) -> dict[str, Any]:
        return call(service.resume_run, run_id)

    return app


def serve(
    service: WorkflowService,
    *,
    port: int = 8765,
    cors_origins: list[str] | None = None,
) -> None:
    """Run the service on ``127.0.0.1`` (never another address: there is no auth)."""
    import uvicorn

    uvicorn.run(
        create_workflow_app(service, cors_origins=cors_origins), host="127.0.0.1", port=port
    )
