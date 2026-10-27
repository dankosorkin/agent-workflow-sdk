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

"""A local service for the workflow layer: design a plan, approve it, run it.

One process on the developer's machine. It has NO authentication, so it must listen on
``127.0.0.1`` only (``serve`` does; do not put it on a network). State lives in a data
directory the caller names: ``records.db`` (designs, plans, runs), ``store.db`` and
``checkpoints.db`` (the engine's own state, so a run survives a restart).

Flow::

    POST /designs {request}                  -> design starts in the background
    GET  /designs/{id}                       -> asking (question) | designing | done | failed
    POST /designs/{id}/answer {answer}       -> answer the orchestrator's questions
    POST /plans/{id}/approve {by}            -> record a human approval (plan_hash bound)
    POST /plans/{id}/runs {params}           -> start a run
    GET  /runs/{id}                          -> running | waiting | held | completed | ...
    POST /runs/{id}/answer {answer}          -> answer a step's question and continue
    POST /runs/{id}/resume                   -> continue a held or interrupted run

Limits, stated plainly: a design in progress is not saved (a restart marks it
``interrupted``; ask again); a run interrupted by a restart is continued with ``resume``;
clients poll the ``GET`` routes (there is no event stream yet).

This module is the logic and needs no web framework; the HTTP adapter is
``agentflow.server.workflow_app`` (the ``server`` extra).
"""

import asyncio
import json
import sqlite3
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from agentflow.checkpoint.sqlite import SqliteCheckpointer
from agentflow.store.sqlite import SqliteStore
from agentflow.workflow.design import DesignerRunner, approve_plan, design_plan
from agentflow.workflow.issues import Issue
from agentflow.workflow.plan import validate_plan
from agentflow.workflow.runner import (
    AgentRunner,
    PlanRefused,
    resume_plan,
    run_plan,
    unsupported_features,
)

__all__ = ["Conflict", "NotFound", "WorkflowService"]


class NotFound(LookupError):
    pass


class Conflict(RuntimeError):
    """The request is valid but not possible in the current state."""


class _Records:
    """Small JSON document table. One connection, used from the event loop only."""

    def __init__(self, path: Path) -> None:
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS docs (kind TEXT, id TEXT, data TEXT, updated REAL,"
            " PRIMARY KEY (kind, id))"
        )
        self._db.commit()

    def put(self, kind: str, doc: dict[str, Any]) -> None:
        self._db.execute(
            "INSERT OR REPLACE INTO docs VALUES (?, ?, ?, ?)",
            (kind, doc["id"], json.dumps(doc), time.time()),
        )
        self._db.commit()

    def get(self, kind: str, doc_id: str) -> dict[str, Any] | None:
        row = self._db.execute(
            "SELECT data FROM docs WHERE kind = ? AND id = ?", (kind, doc_id)
        ).fetchone()
        return json.loads(row[0]) if row else None

    def all(self, kind: str) -> list[dict[str, Any]]:
        rows = self._db.execute(
            "SELECT data FROM docs WHERE kind = ? ORDER BY updated DESC", (kind,)
        ).fetchall()
        return [json.loads(r[0]) for r in rows]

    def close(self) -> None:
        self._db.close()


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


class WorkflowService:
    """Design, approval and runs behind one object; the HTTP layer only forwards to it."""

    def __init__(
        self,
        data_dir: Path | str,
        *,
        inventory_source: Callable[[], dict[str, Any]],
        designer: DesignerRunner,
        runner: AgentRunner,
        named_agents: bool = True,
        max_rounds: int = 3,
    ) -> None:
        self.dir = Path(data_dir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self._records = _Records(self.dir / "records.db")
        self._store = SqliteStore(self.dir / "store.db")
        self._checkpointer = SqliteCheckpointer(self.dir / "checkpoints.db")
        self._inventory_source = inventory_source
        self._designer = designer
        self._runner = runner
        self._named_agents = named_agents
        self._max_rounds = max_rounds
        self._tasks: set[asyncio.Task[Any]] = set()
        self._pending: dict[str, asyncio.Future[str]] = {}  # design id -> awaited user reply
        self._mark_interrupted()

    # -- lifecycle ---------------------------------------------------------------------

    def _mark_interrupted(self) -> None:
        """Whatever was in flight when the previous process stopped is not running now."""
        for d in self._records.all("design"):
            if d["status"] in ("asking", "designing"):
                d.update(status="interrupted", question=None, error="the server restarted")
                self._records.put("design", d)
        for r in self._records.all("run"):
            if r["status"] == "running":
                r.update(status="interrupted", error="the server restarted; use resume")
                self._records.put("run", r)

    async def close(self) -> None:
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._records.close()

    def _spawn(self, coro: Any) -> None:
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def inventory(self) -> dict[str, Any]:
        return self._inventory_source()

    # -- design ------------------------------------------------------------------------

    def start_design(self, request: str) -> dict[str, Any]:
        if not request.strip():
            raise ValueError("the request is empty")
        design: dict[str, Any] = {
            "id": _new_id("design"),
            "request": request,
            "status": "designing",
            "question": None,
            "dialogue": [],
            "result": None,
            "plan_id": None,
            "error": None,
        }
        self._records.put("design", design)
        self._spawn(self._design(design["id"]))
        return design

    async def _design(self, design_id: str) -> None:
        design = self._records.get("design", design_id)
        assert design is not None

        async def ask(questions: list[str]) -> str | None:
            loop = asyncio.get_running_loop()
            future: asyncio.Future[str] = loop.create_future()
            self._pending[design_id] = future
            design.update(status="asking", question={"questions": questions})
            self._records.put("design", design)
            try:
                return await future
            finally:
                self._pending.pop(design_id, None)

        try:
            result = await design_plan(
                design["request"],
                self.inventory(),
                self._designer,
                max_rounds=self._max_rounds,
                named_agents=self._named_agents,
                ask=ask,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            design.update(status="failed", question=None, error=f"{type(exc).__name__}: {exc}")
            self._records.put("design", design)
            return
        design.update(
            status="done" if result.plan is not None else "failed",
            question=None,
            dialogue=result.dialogue,
            result=result.to_dict(),
            error=None if result.plan is not None else result.reason,
        )
        if result.plan is not None:
            plan = {"id": _new_id("plan"), "design_id": design_id, "plan": result.plan}
            self._records.put("plan", plan)
            design["plan_id"] = plan["id"]
        self._records.put("design", design)

    def get_design(self, design_id: str) -> dict[str, Any]:
        design = self._records.get("design", design_id)
        if design is None:
            raise NotFound(f"no design {design_id!r}")
        return design

    def list_designs(self) -> list[dict[str, Any]]:
        return self._records.all("design")

    def answer_design(self, design_id: str, answer: str) -> dict[str, Any]:
        design = self.get_design(design_id)
        future = self._pending.get(design_id)
        if design["status"] != "asking" or future is None or future.done():
            raise Conflict("the design is not waiting for an answer")
        future.set_result(answer)
        design.update(status="designing", question=None)
        self._records.put("design", design)
        return design

    # -- plans -------------------------------------------------------------------------

    def get_plan(self, plan_id: str) -> dict[str, Any]:
        plan = self._records.get("plan", plan_id)
        if plan is None:
            raise NotFound(f"no plan {plan_id!r}")
        return plan

    def list_plans(self) -> list[dict[str, Any]]:
        return self._records.all("plan")

    def check_plan(self, plan_id: str) -> list[Issue]:
        """Everything that would stop this plan from being approved or run now."""
        plan = self.get_plan(plan_id)["plan"]
        return validate_plan(plan, inventory=self.inventory()) + unsupported_features(plan)

    def approve(self, plan_id: str, by: str) -> dict[str, Any]:
        if not by.strip():
            raise ValueError("'by' must name who approves")
        record = self.get_plan(plan_id)
        record["plan"] = approve_plan(record["plan"], self.inventory(), by=by)
        self._records.put("plan", record)
        return record

    # -- runs --------------------------------------------------------------------------

    def start_run(self, plan_id: str, params: dict[str, Any]) -> dict[str, Any]:
        record = self.get_plan(plan_id)
        run: dict[str, Any] = {
            "id": _new_id("run"),
            "plan_id": plan_id,
            "params": params,
            "status": "running",
            "outcome": None,
            "issues": [],
            "error": None,
        }
        self._records.put("run", run)
        self._spawn(self._run(run["id"], record["plan"], first=True))
        return run

    async def _run(
        self, run_id: str, plan: dict[str, Any], *, first: bool, answer: Any = None
    ) -> None:
        run = self._records.get("run", run_id)
        assert run is not None
        common: dict[str, Any] = {
            "store": self._store,
            "checkpointer": self._checkpointer,
            "ask_mode": "interrupt",
            "inventory_source": self._inventory_source,
        }
        try:
            if first:
                outcome = await run_plan(
                    plan,
                    self.inventory(),
                    self._runner,
                    params=run["params"],
                    thread=run_id,
                    **common,
                )
            elif answer is None:
                outcome = await resume_plan(
                    plan, self.inventory(), self._runner, thread=run_id, **common
                )
            else:
                outcome = await resume_plan(
                    plan, self.inventory(), self._runner, thread=run_id, answer=answer, **common
                )
        except asyncio.CancelledError:
            raise
        except PlanRefused as exc:
            run.update(status="refused", issues=[i.to_dict() for i in exc.issues])
        except Exception as exc:
            run.update(status="error", error=f"{type(exc).__name__}: {exc}")
        else:
            run.update(status=outcome.status, outcome=outcome.to_dict(), issues=[], error=None)
        self._records.put("run", run)

    def get_run(self, run_id: str) -> dict[str, Any]:
        run = self._records.get("run", run_id)
        if run is None:
            raise NotFound(f"no run {run_id!r}")
        return run

    def list_runs(self) -> list[dict[str, Any]]:
        return self._records.all("run")

    def _continue_run(self, run_id: str, answer: Any, allowed: tuple[str, ...]) -> dict[str, Any]:
        run = self.get_run(run_id)
        if run["status"] not in allowed:
            raise Conflict(f"a run in status {run['status']!r} cannot do this")
        plan = self.get_plan(run["plan_id"])["plan"]
        run.update(status="running", issues=[], error=None)
        self._records.put("run", run)
        self._spawn(self._run(run_id, plan, first=False, answer=answer))
        return run

    def answer_run(self, run_id: str, answer: Any) -> dict[str, Any]:
        if answer is None:
            raise ValueError("an answer is required")
        return self._continue_run(run_id, answer, ("waiting",))

    def resume_run(self, run_id: str) -> dict[str, Any]:
        return self._continue_run(run_id, None, ("held", "interrupted", "refused", "error"))
