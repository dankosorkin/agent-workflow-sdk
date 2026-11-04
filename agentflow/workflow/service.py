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
clients poll the ``GET`` routes or follow the SSE streams. While a design or a run is working the
service keeps a small activity log (``events(id)``): which role or step started, how long it took,
whether it failed. It is observation only; nothing in it feeds back into the design or the run.

This module is the logic and needs no web framework; the HTTP adapter is
``agentflow.server.workflow_app`` (the ``server`` extra).
"""

import asyncio
import json
import sqlite3
import time
import uuid
from collections.abc import Callable
from contextlib import nullcontext
from pathlib import Path
from typing import Any

from agentflow.checkpoint.sqlite import SqliteCheckpointer
from agentflow.events import (
    ErrorEvent,
    GateEvent,
    PermissionRequest,
    TextChunk,
    ToolCall,
    ToolResult,
    TurnEnd,
    Usage,
)
from agentflow.store.sqlite import SqliteStore
from agentflow.workflow.design import DesignerRunner, approve_plan, design_plan
from agentflow.workflow.environment import MODES, Environment, drift
from agentflow.workflow.issues import Issue
from agentflow.workflow.plan import check_approval, validate_plan
from agentflow.workflow.runner import (
    AgentRunner,
    ApprovalRevoked,
    PlanRefused,
    in_workspace,
    observing,
    resume_plan,
    run_plan,
    unsupported_features,
)

__all__ = ["Conflict", "NotFound", "WorkflowService"]


_FINISHED = {"completed", "failed", "partial", "error", "refused"}


class NotFound(LookupError):
    pass


class Conflict(RuntimeError):
    """The request is valid but not possible in the current state."""


#: Most agent events kept per design or run (oldest dropped). A safety net, not a policy.
_AGENT_EVENT_CAP = 5000


class _Records:
    """Small JSON document table. One connection, used from the event loop only."""

    def __init__(self, path: Path) -> None:
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS docs (kind TEXT, id TEXT, data TEXT, updated REAL,"
            " PRIMARY KEY (kind, id))"
        )
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS agent_events (owner TEXT, seq INTEGER, t INTEGER,"
            " data TEXT, PRIMARY KEY (owner, seq))"
        )
        self._db.commit()

    def append_event(self, owner: str, data: dict[str, Any]) -> None:
        row = self._db.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM agent_events WHERE owner = ?", (owner,)
        ).fetchone()
        seq = row[0]
        self._db.execute(
            "INSERT INTO agent_events VALUES (?, ?, ?, ?)",
            (owner, seq, int(time.time() * 1000), json.dumps(data, default=str)),
        )
        if seq > _AGENT_EVENT_CAP:  # a safety net only; truncation policy comes later
            self._db.execute(
                "DELETE FROM agent_events WHERE owner = ? AND seq <= ?",
                (owner, seq - _AGENT_EVENT_CAP),
            )
        self._db.commit()

    def list_events(self, owner: str, after: int = 0, limit: int = 1000) -> list[dict[str, Any]]:
        rows = self._db.execute(
            "SELECT seq, t, data FROM agent_events"
            " WHERE owner = ? AND seq > ? ORDER BY seq LIMIT ?",
            (owner, after, limit),
        ).fetchall()
        return [{"seq": r[0], "t": r[1], **json.loads(r[2])} for r in rows]

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


def _usage_dict(u: Usage | None) -> dict[str, Any] | None:
    if u is None:
        return None
    return {
        "input_tokens": u.input_tokens,
        "output_tokens": u.output_tokens,
        "total_tokens": u.total_tokens,
        "cost_usd": u.cost_usd,
    }


def _describe(ev: Any, *, streamed_text: bool) -> dict[str, Any]:
    """One backend event as a plain dict for the agent log. Everything is kept, nothing is cut."""
    if isinstance(ev, ToolCall):
        return {
            "type": "tool_call",
            "id": ev.id,
            "name": ev.name,
            "title": ev.title,
            "args": ev.args,
        }
    if isinstance(ev, ToolResult):
        return {"type": "tool_result", "id": ev.id, "status": ev.status, "content": ev.content}
    if isinstance(ev, PermissionRequest):
        return {
            "type": "permission",
            "id": ev.id,
            "tool": ev.tool,
            "options": [o.name for o in ev.options],
            "detail": ev.detail,
        }
    if isinstance(ev, Usage):
        return {"type": "usage", **(_usage_dict(ev) or {})}
    if isinstance(ev, TurnEnd):
        out: dict[str, Any] = {
            "type": "turn_end",
            "stop_reason": ev.stop_reason,
            "usage": _usage_dict(ev.usage),
        }
        if not streamed_text and ev.text:  # the backend sent the answer only as a whole
            out["text"] = ev.text
        return out
    if isinstance(ev, ErrorEvent):
        return {"type": "error", "message": ev.message, "detail": ev.detail}
    if isinstance(ev, GateEvent):
        return {"type": "gate", "gate": ev.gate, "phase": ev.phase, "attempt": ev.attempt}
    return {"type": "event", "name": type(ev).__name__, "repr": repr(ev)}


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
        environment: Environment | None = None,
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
        self._env = environment
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
        """What plans are designed from: the neutral role catalog when an environment is
        configured, otherwise the inventory the caller supplied."""
        return self._env.catalog() if self._env is not None else self._inventory_source()

    def _inventory_for(self, record: dict[str, Any]) -> dict[str, Any]:
        """The inventory a plan is judged against. A plan designed with an environment is
        provider-neutral: it is judged against the current role catalog, whatever backend will
        run it. An older plan keeps the snapshot it was designed with."""
        if record.get("neutral") and self._env is not None:
            return self._env.catalog()
        if record.get("inventory") is not None:
            return record["inventory"]
        return self._inventory_source()

    def environment_info(self) -> dict[str, Any]:
        if self._env is None:
            return {"enabled": False, "project": None, "system_backend": None}
        try:
            system = self._env.system_backend_name()
        except ValueError:
            system = None
        return {"enabled": True, "project": str(self._env.project), "system_backend": system}

    def binding_preview(
        self,
        plan_id: str,
        backend: str,
        extensions: list[dict[str, str]] | None = None,
        mode: str = "auto",
    ) -> dict[str, Any]:
        """What starting ``plan_id`` on ``backend`` would bind, without starting it."""
        record = self.get_plan(plan_id)
        if not record.get("neutral") or self._env is None:
            raise ValueError("this plan was designed for a fixed inventory; it has no binding")
        if mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}")
        return self._env.preview(record["plan"], backend, extensions, mode)

    # -- activity ----------------------------------------------------------------------

    def _emit(self, owner_id: str, kind: str, **data: Any) -> None:
        """Append one entry to the activity log of a design or run (newest 500 are kept)."""
        doc = self._records.get("events", owner_id) or {"id": owner_id, "items": []}
        doc["items"].append({"t": int(time.time() * 1000), "kind": kind, **data})
        doc["items"] = doc["items"][-500:]
        self._records.put("events", doc)

    def events(self, owner_id: str) -> list[dict[str, Any]]:
        """The activity log of a design or run, oldest first (empty when there is none)."""
        return (self._records.get("events", owner_id) or {}).get("items", [])

    def agent_events(
        self, owner_id: str, after: int = 0, limit: int = 1000
    ) -> list[dict[str, Any]]:
        """What the agents did inside their calls (text, tools, permissions, usage), in order."""
        return self._records.list_events(owner_id, after, limit)

    def _agent_observer(self, owner_id: str, **labels: Any) -> Any:
        """A callback for :func:`observing` that logs every backend event under ``labels``.

        Consecutive text chunks are joined (flushed at least every half second) so a long answer
        is a few rows, not thousands. Never raises into the agent call.
        """
        buf: list[str] = []
        state = {"last": time.monotonic(), "streamed": False}

        def flush() -> None:
            if buf:
                self._records.append_event(
                    owner_id, {**labels, "type": "text", "text": "".join(buf)}
                )
                buf.clear()

        def observe(ev: Any) -> None:
            if isinstance(ev, TextChunk):
                buf.append(ev.text)
                state["streamed"] = True
                if time.monotonic() - state["last"] >= 0.5:
                    flush()
                    state["last"] = time.monotonic()
                return
            flush()
            state["last"] = time.monotonic()
            self._records.append_event(
                owner_id, {**labels, **_describe(ev, streamed_text=state["streamed"])}
            )

        observe.flush = flush  # type: ignore[attr-defined]
        return observe

    def _watched_designer(self, design_id: str, designer: DesignerRunner) -> DesignerRunner:
        counts: dict[str, int] = {}

        async def run(role: str, prompt: str) -> str:
            counts[role] = n = counts.get(role, 0) + 1
            self._emit(design_id, "stage", role=role, n=n, state="started")
            t0 = time.monotonic()
            observer = self._agent_observer(design_id, role=role, n=n)
            try:
                with observing(observer):
                    out = await designer(role, prompt)
                observer.flush()
            except BaseException as exc:
                observer.flush()
                self._emit(
                    design_id,
                    "stage",
                    role=role,
                    n=n,
                    state="failed",
                    seconds=round(time.monotonic() - t0, 1),
                    error=type(exc).__name__,
                )
                raise
            self._emit(
                design_id,
                "stage",
                role=role,
                n=n,
                state="finished",
                seconds=round(time.monotonic() - t0, 1),
                chars=len(out),
            )
            return out

        return run

    def _watched_runner(self, run_id: str, inventory: dict[str, Any] | None = None) -> AgentRunner:
        counts: dict[str, int] = {}

        async def run(step: dict[str, Any], entry: dict[str, Any], prompt: str) -> str:
            sid = step["id"]
            counts[sid] = n = counts.get(sid, 0) + 1
            where = {"step": sid, "attempt": n, "backend": entry.get("backend")}
            self._emit(run_id, "step", state="started", **where)
            t0 = time.monotonic()
            observer = self._agent_observer(
                run_id, step=sid, attempt=n, backend=entry.get("backend")
            )
            try:
                preamble = (
                    ((inventory or {}).get("agents") or {})
                    .get(step.get("agent"), {})
                    .get("preamble")
                )
                task = f"{preamble}\n\n{prompt}" if preamble else prompt
                with observing(observer):
                    out = await self._runner(step, entry, task)
                observer.flush()
            except BaseException as exc:
                observer.flush()
                self._emit(
                    run_id,
                    "step",
                    state="failed",
                    seconds=round(time.monotonic() - t0, 1),
                    error=type(exc).__name__,
                    **where,
                )
                raise
            self._emit(
                run_id,
                "step",
                state="finished",
                seconds=round(time.monotonic() - t0, 1),
                chars=len(out) if isinstance(out, str) else None,
                **where,
            )
            return out

        return run

    # -- design ------------------------------------------------------------------------

    def start_design(self, request: str, *, system_backend: str | None = None) -> dict[str, Any]:
        """Start designing a plan. The plan is provider-neutral: it names roles, not backends.
        The system agents (orchestrator, designer, critic) run on ``system_backend`` (default: the
        configured one, else the first installed); the executing backend is chosen at launch."""
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
            "system_backend": None,
        }
        if self._env is not None:
            design["system_backend"] = self._env.system_backend_name(system_backend)  # may raise
        elif system_backend is not None:
            raise ValueError("choosing a system backend needs an environment; none is configured")
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
            self._emit(design_id, "design", state="asking", questions=len(questions))
            try:
                return await future
            finally:
                self._pending.pop(design_id, None)

        self._emit(design_id, "design", state="started")
        if self._env is not None:
            designer, named = self._env.designer_for(design["system_backend"]), False
        else:
            designer, named = self._designer, self._named_agents
        inventory = self.inventory()
        try:
            result = await design_plan(
                design["request"],
                inventory,
                self._watched_designer(design_id, designer),
                max_rounds=self._max_rounds,
                named_agents=named,
                ask=ask,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            design.update(status="failed", question=None, error=f"{type(exc).__name__}: {exc}")
            self._records.put("design", design)
            self._emit(design_id, "design", state="failed", error=type(exc).__name__)
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
            if self._env is not None:
                plan["neutral"] = True
            self._records.put("plan", plan)
            design["plan_id"] = plan["id"]
        self._records.put("design", design)
        self._emit(design_id, "design", state="done" if result.plan is not None else "failed")

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
        record = self.get_plan(plan_id)
        plan = record["plan"]
        return validate_plan(plan, inventory=self._inventory_for(record)) + unsupported_features(
            plan
        )

    def approve(self, plan_id: str, by: str) -> dict[str, Any]:
        if not by.strip():
            raise ValueError("'by' must name who approves")
        record = self.get_plan(plan_id)
        record["plan"] = approve_plan(record["plan"], self._inventory_for(record), by=by)
        self._records.put("plan", record)
        return record

    # -- runs --------------------------------------------------------------------------

    def start_run(
        self,
        plan_id: str,
        params: dict[str, Any],
        *,
        backend: str | None = None,
        extensions: list[dict[str, str]] | None = None,
        mode: str = "auto",
    ) -> dict[str, Any]:
        """Start a run. A provider-neutral plan is bound HERE to one backend: every step runs on
        its generic agent shaped by the role, and the backend applies the user's own steering,
        skills and instructions. ``extensions`` are the files the run relies on; ``mode`` says what
        a change to them means (``strict`` holds the run, ``track`` warns). A plan that cannot run
        on that backend is refused now, with the reasons."""
        record = self.get_plan(plan_id)
        binding: dict[str, Any] | None = None
        if record.get("neutral") and self._env is not None:
            if backend is None:
                raise ValueError("choose the backend this run executes on")
            if mode not in MODES:
                raise ValueError(f"mode must be one of {MODES}")
            selection = [{"kind": e["kind"], "name": e["name"]} for e in extensions or []]
            preview = self._env.preview(record["plan"], backend, selection, mode)
            bound = self._env.bind(backend, selection) if not preview["issues"] else None
            issues = [Issue(**i) for i in preview["issues"]]
            if bound is not None:
                issues += check_approval(record["plan"], bound)
            if any(i.severity == "error" for i in issues):
                raise PlanRefused([i for i in issues if i.severity == "error"])
            binding = {
                "backend": backend,
                "selection": selection,
                "mode": mode,
                "resolved_mode": preview["resolved_mode"],
                "extensions": bound["context"]["extensions"] if bound else [],
            }
        elif backend is not None:
            raise ValueError("this plan was designed for a fixed inventory; it has no binding")
        run: dict[str, Any] = {
            "id": _new_id("run"),
            "plan_id": plan_id,
            "params": params,
            "binding": binding,
            "status": "running",
            "outcome": None,
            "issues": [],
            "warnings": [],
            "workspace": None,
            "error": None,
        }
        self._records.put("run", run)
        self._spawn(self._run(run["id"], record, first=True))
        return run

    async def _run(
        self, run_id: str, record: dict[str, Any], *, first: bool, answer: Any = None
    ) -> None:
        run = self._records.get("run", run_id)
        assert run is not None
        plan = record["plan"]
        binding = run.get("binding")

        def inventory_now() -> dict[str, Any]:
            if binding is None or self._env is None:
                return self._inventory_for(record)
            live = self._env.bind(binding["backend"], binding["selection"])
            changed = drift(binding["extensions"], live["context"]["extensions"])
            if changed and binding["resolved_mode"] == "strict":
                raise ApprovalRevoked(
                    [
                        Issue(
                            "context_changed",
                            f"files changed since the run was bound: {', '.join(changed)}",
                        )
                    ]
                )
            if changed:  # track: say so once, keep going
                note = f"files changed since the run was bound: {', '.join(changed)}"
                if note not in run["warnings"]:
                    run["warnings"].append(note)
                    self._records.put("run", run)
                    self._emit(run_id, "context", state="changed", files=changed)
            return live

        common: dict[str, Any] = {
            "store": self._store,
            "checkpointer": self._checkpointer,
            "ask_mode": "interrupt",
            "inventory_source": inventory_now,
        }
        self._emit(run_id, "run", state="started" if first else "resumed")
        try:
            if binding is not None and self._env is not None and not first:
                # Resuming is the user's explicit go-ahead: bind to the files as they are now.
                live = self._env.bind(binding["backend"], binding["selection"])
                binding["extensions"] = live["context"]["extensions"]
                run["binding"] = binding
            inventory = inventory_now()
            runner = self._watched_runner(run_id, inventory)
            workspace = run.get("workspace")
            if workspace is None and binding is not None and self._env is not None:
                workspace = await asyncio.to_thread(
                    self._env.prepare_workspace, run_id, binding["backend"]
                )
                run["workspace"] = workspace
                self._records.put("run", run)
                self._emit(
                    run_id,
                    "workspace",
                    state=workspace["kind"],
                    path=workspace["path"],
                    branch=workspace.get("branch"),
                    note=workspace["note"],
                )
            where = in_workspace(workspace["path"]) if workspace else nullcontext()
            with where:
                if first:
                    outcome = await run_plan(
                        plan, inventory, runner, params=run["params"], thread=run_id, **common
                    )
                elif answer is None:
                    outcome = await resume_plan(plan, inventory, runner, thread=run_id, **common)
                else:
                    outcome = await resume_plan(
                        plan, inventory, runner, thread=run_id, answer=answer, **common
                    )
        except asyncio.CancelledError:
            raise
        except ApprovalRevoked as exc:
            run.update(status="held", issues=[i.to_dict() for i in exc.issues])
        except PlanRefused as exc:
            run.update(status="refused", issues=[i.to_dict() for i in exc.issues])
        except Exception as exc:
            run.update(status="error", error=f"{type(exc).__name__}: {exc}")
        else:
            body = outcome.to_dict()
            held_reasons = body["issues"] if outcome.status == "held" else []
            run.update(status=outcome.status, outcome=body, issues=held_reasons, error=None)
        if run["status"] in _FINISHED and run.get("workspace"):
            await self._close_workspace(run)
        self._records.put("run", run)
        self._emit(run_id, "run", state=run["status"])

    async def _close_workspace(self, run: dict[str, Any]) -> None:
        """A finished run leaves nothing behind that is not wanted: what the agents wrote is
        committed to the run's branch, and a worktree with no changes is removed with its branch.
        A failure here never changes the run's result; it is reported as a warning."""
        ws = run["workspace"]
        if ws.get("kind") != "worktree" or self._env is None:
            return
        try:
            info = await asyncio.to_thread(
                self._env.finalize_workspace, ws, run["id"], f"plan {run['plan_id']}"
            )
            ws.update(info)
            if info["ahead"] == 0:
                await asyncio.to_thread(self._env.remove_workspace, ws, "delete")
                ws.update(removed=True, note="no changes: the worktree and its branch were removed")
            else:
                ws["note"] = (
                    f"{info['changed']} file(s) changed, saved on branch {ws['branch']}"
                    f" (commit {info['commit'] or 'earlier'}); the worktree is kept"
                )
            self._emit(
                run["id"], "workspace", state="removed" if ws.get("removed") else "kept", **info
            )
        except Exception as exc:
            run["warnings"].append(f"worktree cleanup: {type(exc).__name__}: {exc}")

    # -- worktrees of finished runs ------------------------------------------------------------

    def _workspace_run(self, run_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
        run = self.get_run(run_id)
        ws = run.get("workspace")
        if not ws or ws.get("kind") != "worktree" or self._env is None:
            raise NotFound(f"run {run_id!r} has no worktree")
        return run, ws

    def list_workspaces(self) -> list[dict[str, Any]]:
        """Every worktree a run made: where it is, whether it is still on disk, what is in it."""
        out = []
        for run in self.list_runs():
            ws = run.get("workspace")
            if not ws or ws.get("kind") != "worktree" or self._env is None:
                continue
            try:
                state = self._env.workspace_state(ws)
            except ValueError:
                continue
            out.append(
                {
                    "run_id": run["id"],
                    "status": run["status"],
                    "path": ws["path"],
                    "branch": ws.get("branch"),
                    "exists": state["exists"] and not ws.get("removed"),
                    "changed": ws.get("changed", len(state["dirty"])),
                    "uncommitted": len(state["dirty"]),
                    "ahead": state["ahead"],
                    "removed": bool(ws.get("removed")),
                }
            )
        return out

    def remove_workspace(self, run_id: str, branch: str = "keep") -> dict[str, Any]:
        """Remove a finished run's worktree. The work stays on its branch unless ``branch`` says
        ``delete`` (refused if it holds unmerged commits) or ``force``."""
        run, ws = self._workspace_run(run_id)
        if run["status"] not in _FINISHED:
            raise Conflict(f"run {run_id} is {run['status']}: its worktree is still needed")
        try:
            done = self._env.remove_workspace(ws, branch)
        except ValueError as exc:
            raise Conflict(str(exc)) from exc
        ws.update(
            removed=True,
            note="removed by request"
            + ("; branch deleted" if done["branch_deleted"] else f"; branch {ws['branch']} kept"),
        )
        self._records.put("run", run)
        return run

    def sweep_workspaces(self, mode: str = "clean") -> dict[str, Any]:
        """Remove worktrees of finished runs. ``clean``: only those that hold no work, with their
        branch. ``finished``: every finished run's directory, keeping branches that hold work.
        Worktrees with uncommitted changes and runs still going are never touched."""
        if mode not in ("clean", "finished"):
            raise ValueError("mode must be clean or finished")
        removed, kept = [], []
        for item in self.list_workspaces():
            run = self.get_run(item["run_id"])
            if not item["exists"] or run["status"] not in _FINISHED:
                continue
            if item["uncommitted"]:
                kept.append({"run_id": item["run_id"], "why": "uncommitted changes"})
                continue
            if item["ahead"] and mode == "clean":
                kept.append({"run_id": item["run_id"], "why": f"{item['ahead']} commit(s) of work"})
                continue
            try:
                self.remove_workspace(item["run_id"], "delete" if not item["ahead"] else "keep")
                removed.append(item["run_id"])
            except (Conflict, NotFound) as exc:
                kept.append({"run_id": item["run_id"], "why": str(exc)})
        return {"removed": removed, "kept": kept}

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
        record = self.get_plan(run["plan_id"])
        run.update(status="running", issues=[], error=None)
        self._records.put("run", run)
        self._spawn(self._run(run_id, record, first=False, answer=answer))
        return run

    def answer_run(self, run_id: str, answer: Any) -> dict[str, Any]:
        if answer is None:
            raise ValueError("an answer is required")
        return self._continue_run(run_id, answer, ("waiting",))

    def resume_run(self, run_id: str) -> dict[str, Any]:
        return self._continue_run(run_id, None, ("held", "interrupted", "refused", "error"))
