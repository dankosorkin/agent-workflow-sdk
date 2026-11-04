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

import asyncio
import json
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any

from agentflow.workflow.extensions import (
    BACKENDS,
    ProbeRunner,
    available_backends,
    discover,
    fingerprint,
    probe_backend,
)
from agentflow.workflow.runner import PlanRefused
from agentflow.workflow.service import Conflict, NotFound, WorkflowService

__all__ = ["create_workflow_app", "serve"]


def create_workflow_app(
    service: WorkflowService,
    *,
    cors_origins: list[str] | None = None,
    ui_dir: Path | None = None,
    workspace: Path | None = None,
    probe_runner: ProbeRunner | None = None,
):
    """The FastAPI app over ``service``. Needs the ``server`` extra.

    ``workspace`` is the project directory whose steering, skills and instructions are listed
    next to the user's own (default: the current directory). ``probe_runner`` replaces the real
    agent in the extension probe (tests).
    """
    ui_dir = ui_dir or (Path(__file__).resolve().parents[2] / "ui-mockup-demo")
    workspace = Path(workspace or Path.cwd()).expanduser().resolve()
    try:
        from fastapi import FastAPI, HTTPException, Query
        from fastapi.middleware.cors import CORSMiddleware
        from fastapi.responses import RedirectResponse, StreamingResponse
        from fastapi.staticfiles import StaticFiles
        from pydantic import BaseModel, Field
    except ModuleNotFoundError as exc:  # pragma: no cover - import-time guard
        raise ModuleNotFoundError(
            "the workflow server requires FastAPI: pip install 'agent-workflow-sdk[server]'"
        ) from exc

    class ExtensionRef(BaseModel):
        kind: str
        name: str

    class DesignRequest(BaseModel):
        request: str
        system_backend: str | None = None

    class AnswerRequest(BaseModel):
        answer: Any

    class ApproveRequest(BaseModel):
        by: str

    class SweepRequest(BaseModel):
        mode: str = "clean"

    class RunRequest(BaseModel):
        params: dict[str, Any] = Field(default_factory=dict)
        backend: str | None = None
        extensions: list[ExtensionRef] = Field(default_factory=list)
        mode: str = "auto"

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

    def call(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
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
        return call(
            service.start_design,
            body.request,
            system_backend=body.system_backend,
        )

    @app.get("/designs")
    async def list_designs() -> list[dict[str, Any]]:
        return service.list_designs()

    def with_events(record: dict[str, Any]) -> dict[str, Any]:
        # The activity log is observation only; it travels with the record but is stored apart.
        return {**record, "events": service.events(record["id"])}

    @app.get("/designs/{design_id}")
    async def get_design(design_id: str) -> dict[str, Any]:
        return with_events(call(service.get_design, design_id))

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
        return call(
            service.start_run,
            plan_id,
            body.params,
            backend=body.backend,
            extensions=[e.model_dump() for e in body.extensions],
            mode=body.mode,
        )

    @app.get("/plans/{plan_id}/binding")
    async def plan_binding(
        plan_id: str,
        backend: str,
        ext: Annotated[list[str] | None, Query()] = None,
        mode: str = "auto",
    ) -> dict[str, Any]:
        # ``ext`` repeats as ``kind:name`` (for example ``skills:api-design``).
        picked = []
        for item in ext or []:
            kind, _, name = item.partition(":")
            picked.append({"kind": kind, "name": name})
        return call(service.binding_preview, plan_id, backend, picked, mode)

    @app.get("/runs")
    async def list_runs() -> list[dict[str, Any]]:
        return service.list_runs()

    @app.get("/workspaces")
    async def list_workspaces() -> list[dict[str, Any]]:
        return await asyncio.to_thread(service.list_workspaces)

    @app.delete("/runs/{run_id}/workspace")
    async def remove_workspace(run_id: str, branch: str = "keep") -> dict[str, Any]:
        return await asyncio.to_thread(call, service.remove_workspace, run_id, branch)

    @app.post("/workspaces/sweep")
    async def sweep_workspaces(body: SweepRequest) -> dict[str, Any]:
        return await asyncio.to_thread(call, service.sweep_workspaces, body.mode)

    @app.get("/runs/{run_id}")
    async def get_run(run_id: str) -> dict[str, Any]:
        return with_events(call(service.get_run, run_id))

    @app.post("/runs/{run_id}/answer")
    async def answer_run(run_id: str, body: AnswerRequest) -> dict[str, Any]:
        return call(service.answer_run, run_id, body.answer)

    @app.post("/runs/{run_id}/resume")
    async def resume_run(run_id: str) -> dict[str, Any]:
        return call(service.resume_run, run_id)

    TICK_EVERY = 10  # loops of 0.1 s: a heartbeat once a second while nothing else changes

    def tick() -> str:
        # Proves the stream and the server are alive when no record changes (a model is thinking).
        return f"event: tick\ndata: {json.dumps({'now': int(time.time() * 1000)})}\n\n"

    async def _design_events(design_id: str) -> AsyncIterator[str]:
        terminal = {"done", "failed", "interrupted"}
        last: str | None = None
        quiet = 0
        while True:
            record = with_events(call(service.get_design, design_id))
            payload = json.dumps(record)
            if payload != last:
                last = payload
                quiet = 0
                yield f"data: {payload}\n\n"
            elif quiet >= TICK_EVERY:
                quiet = 0
                yield tick()
            if record["status"] in terminal:
                return
            quiet += 1
            await asyncio.sleep(0.1)

    @app.get("/designs/{design_id}/events")
    async def design_events(design_id: str) -> Any:
        call(service.get_design, design_id)  # 404 before streaming for unknown id
        return StreamingResponse(_design_events(design_id), media_type="text/event-stream")

    async def _run_events(run_id: str) -> AsyncIterator[str]:
        # Stream only while running; emit the terminal/paused record then stop so
        # the client can switch to its answer/resume controls.
        terminal = {
            "completed",
            "partial",
            "failed",
            "refused",
            "error",
            "interrupted",
            "waiting",
            "held",
        }
        last: str | None = None
        quiet = 0
        while True:
            record = with_events(call(service.get_run, run_id))
            payload = json.dumps(record)
            if payload != last:
                last = payload
                quiet = 0
                yield f"data: {payload}\n\n"
            elif quiet >= TICK_EVERY:
                quiet = 0
                yield tick()
            if record["status"] in terminal:
                return
            quiet += 1
            await asyncio.sleep(0.1)

    @app.get("/runs/{run_id}/events")
    async def run_events(run_id: str) -> Any:
        call(service.get_run, run_id)  # 404 before streaming for unknown id
        return StreamingResponse(_run_events(run_id), media_type="text/event-stream")

    # -- backends: what exists, what it can load, and whether it really does ---------------

    probes: dict[str, dict[str, Any]] = {}
    probe_tasks: set[asyncio.Task[Any]] = set()

    def known_backend(name: str) -> None:
        if name not in BACKENDS:
            raise HTTPException(status_code=404, detail=f"unknown backend {name!r}")

    @app.get("/environment")
    async def environment() -> dict[str, Any]:
        return service.environment_info()

    @app.get("/backends")
    async def list_backends() -> list[dict[str, Any]]:
        return await asyncio.to_thread(available_backends)

    @app.get("/backends/{name}/extensions")
    async def backend_extensions(name: str) -> dict[str, Any]:
        known_backend(name)
        found = await asyncio.to_thread(discover, name, workspace)
        return {
            "backend": name,
            "workspace": str(workspace),
            "fingerprint": fingerprint([e for items in found.values() for e in items]),
            "extensions": {kind: [e.to_dict() for e in items] for kind, items in found.items()},
        }

    @app.post("/backends/{name}/probe", status_code=202)
    async def start_probe(name: str) -> dict[str, Any]:
        known_backend(name)
        if probes.get(name, {}).get("status") == "running":
            raise HTTPException(status_code=409, detail="a probe is already running")
        state: dict[str, Any] = {
            "backend": name,
            "status": "running",
            "started": int(time.time() * 1000),
            "results": [],
        }
        probes[name] = state

        async def job() -> None:
            try:
                results = await probe_backend(name, runner=probe_runner)
                state.update(status="done", results=[r.to_dict() for r in results])
            except Exception as exc:
                state.update(status="error", error=f"{type(exc).__name__}: {exc}")
            state["finished"] = int(time.time() * 1000)

        task = asyncio.create_task(job())
        probe_tasks.add(task)
        task.add_done_callback(probe_tasks.discard)
        return state

    @app.get("/backends/{name}/probe")
    async def get_probe(name: str) -> dict[str, Any]:
        known_backend(name)
        return probes.get(name) or {"backend": name, "status": "never", "results": []}

    async def _agent_events(
        owner_id: str, status_of: Callable[[], str], stop: set[str], after: int
    ) -> AsyncIterator[str]:
        # What the agents did inside their calls. The status is read BEFORE the log, so every
        # entry written before a stopping status is delivered before the stream ends.
        last = after
        while True:
            status = status_of()
            items = service.agent_events(owner_id, last)
            for item in items:
                last = item["seq"]
                yield f"id: {last}\ndata: {json.dumps(item)}\n\n"
            if items:
                continue
            if status in stop:
                return
            await asyncio.sleep(0.2)

    @app.get("/designs/{design_id}/agent-stream")
    async def design_agent_stream(design_id: str, after: int = 0) -> Any:
        call(service.get_design, design_id)  # 404 before streaming for unknown id
        return StreamingResponse(
            _agent_events(
                design_id,
                lambda: service.get_design(design_id)["status"],
                {"done", "failed", "interrupted"},
                after,
            ),
            media_type="text/event-stream",
        )

    @app.get("/runs/{run_id}/agent-stream")
    async def run_agent_stream(run_id: str, after: int = 0) -> Any:
        call(service.get_run, run_id)  # 404 before streaming for unknown id
        return StreamingResponse(
            _agent_events(
                run_id,
                lambda: service.get_run(run_id)["status"],
                {
                    "completed",
                    "partial",
                    "failed",
                    "refused",
                    "error",
                    "interrupted",
                    "waiting",
                    "held",
                },
                after,
            ),
            media_type="text/event-stream",
        )

    # Serve the UI from the same app. Mount LAST so it never shadows API routes.
    if ui_dir.exists():

        @app.get("/")
        async def index() -> Any:
            return RedirectResponse("/ui/index.html")

        app.mount("/ui", StaticFiles(directory=str(ui_dir), html=True), name="ui")

    return app


def serve(
    service: WorkflowService,
    *,
    port: int = 8765,
    cors_origins: list[str] | None = None,
    ui_dir: Path | None = None,
    workspace: Path | None = None,
) -> None:
    """Run the service on ``127.0.0.1`` (never another address: there is no auth)."""
    import uvicorn

    uvicorn.run(
        create_workflow_app(service, cors_origins=cors_origins, ui_dir=ui_dir, workspace=workspace),
        host="127.0.0.1",
        port=port,
    )
