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

"""Execute an approved plan as data through the SDK's ``add_plan_executor``.

    outcome = await run_plan(plan, inventory, runner, params={"window": "24h"})

One fixed engine graph runs every plan: a planner writes the plan's steps into the
state as tasks, the plan executor runs them by readiness, and a single handler runs
one step. Nothing is generated and the engine graph is never mutated.

A step runs like this:

  1. resolve its input bindings from earlier results and run parameters;
  2. if a result for the same (agent, prompt, inputs, contract, checks) is in the
     ``Store``, reuse it (this is how a second run skips finished work);
  3. otherwise run the agent inside a quality-gate sub-graph on a deterministic
     sub-thread, so the attempt counter survives a crash and a resume;
  4. a step with ``side_effects`` runs under ``IdempotentOp``, so a crashed attempt
     is never silently repeated;
  5. a step with ``may_ask`` may answer with a clarifying question instead of a
     result. The question is answered by ``default_answer``, by a human (``ask_mode=
     "interrupt"``, resumed with :func:`resume_plan`), or it stops the step with
     ``needs_clarification`` (``ask_mode="stop"``, the default); a stopped run is
     continued by a NEW run that carries the answer (``answers=...``) and reuses
     the finished steps from the cache.

An approval that stops being valid (plan edited, inventory changed) is detected before
each step when an ``inventory_source`` is supplied, and the run is HELD, not failed:
the finished steps stay, and :func:`resume_plan` continues once approval is valid.

What is deliberately NOT executable yet is listed by :func:`unsupported_features`
and refused up front, never ignored: ``semantic_check``, budgets, timeouts, artifacts,
``may_ask`` together with ``side_effects``, and check kinds outside
:data:`~agentflow.workflow.checks.CHECKS`.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping
from contextlib import asynccontextmanager, contextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any

from agentflow import END, START, ClarificationRequest, Graph, last
from agentflow.backends.pool import BackendSpec
from agentflow.checkpoint import MemoryCheckpointer
from agentflow.errors import BackendError, NodeError
from agentflow.prebuilt import (
    GateResult,
    GateState,
    IdempotentOp,
    IncompleteEffectError,
    PlanState,
    Task,
    TaskResult,
    add_plan_executor,
    add_quality_gate,
    artifact_key,
)
from agentflow.store import MemoryStore, Store
from agentflow.workflow.bindings import (
    TYPES,
    ancestors_of,
    render_prompt,
    resolve_inputs,
    type_ok,
)
from agentflow.workflow.checks import CHECKS, extract_question, parse_output, run_checks
from agentflow.workflow.inventory import resolve_backend
from agentflow.workflow.issues import Issue, has_errors
from agentflow.workflow.plan import check_approval, plan_hash, validate_plan

__all__ = [
    "AgentRunner",
    "ApprovalRevoked",
    "PlanRefused",
    "RunOutcome",
    "backend_runner",
    "build_workflow_graph",
    "collect_text",
    "make_backend",
    "kiro_policy",
    "step_policy",
    "agent_pool",
    "in_workspace",
    "observing",
    "resume_plan",
    "run_plan",
    "summarize",
    "unsupported_features",
]

#: The Kiro permission that lets an agent load a skill. Harmless: it only reads.
KIRO_SKILL_PERMISSION = "disclose_context"

#: ``(step, backend_entry, prompt) -> the agent's final text``
AgentRunner = Callable[[dict[str, Any], dict[str, Any], str], Awaitable[str]]

CACHE_NS = ("workflow", "step_cache")
EFFECT_NS = ("workflow", "effects")


class PlanRefused(Exception):
    """The plan cannot be run (invalid, unapproved, unsupported, or missing parameters)."""

    def __init__(self, issues: list[Issue]) -> None:
        self.issues = issues
        super().__init__("; ".join(f"{i.code}: {i.message}" for i in issues))


class ApprovalRevoked(Exception):
    """Raised inside a run when the plan's approval stopped being valid between steps."""

    def __init__(self, issues: list[Issue]) -> None:
        self.issues = issues
        super().__init__("; ".join(f"{i.code}: {i.message}" for i in issues))


class _StepFailed(Exception):
    pass


_NO_ANSWER = object()


# ---------------------------------------------------------------------------
# What the executor cannot do yet
# ---------------------------------------------------------------------------


def unsupported_features(plan: dict[str, Any]) -> list[Issue]:
    """Features a valid plan may name that this executor cannot run yet (errors)."""
    out: list[Issue] = []
    if plan.get("budget"):
        out.append(Issue("unsupported_feature", "a plan-level budget is not enforced yet"))
    for step in (plan.get("graph") or {}).get("steps") or []:
        sid = step["id"]
        for key in ("semantic_check", "budget", "timeout"):
            if step.get(key):
                out.append(Issue("unsupported_feature", f"'{key}' is not executable yet", step=sid))
        if step.get("may_ask") and step.get("side_effects"):
            out.append(
                Issue(
                    "unsupported_feature",
                    "'may_ask' together with 'side_effects' is not executable yet",
                    step=sid,
                )
            )
        if step.get("artifacts"):
            out.append(Issue("unsupported_feature", "artifacts are not produced yet", step=sid))
        for name, b in (step.get("inputs") or {}).items():
            if isinstance(b, dict) and "artifact" in b:
                out.append(
                    Issue(
                        "unsupported_feature",
                        "artifact bindings need artifacts, which are not produced yet",
                        step=sid,
                        input=name,
                    )
                )
        for check in step.get("checks") or []:
            if check.get("kind") not in CHECKS:
                out.append(
                    Issue(
                        "unsupported_check_kind",
                        f"check kind {check.get('kind')!r} is not implemented "
                        f"(available: {sorted(CHECKS)})",
                        step=sid,
                    )
                )
    return out


# ---------------------------------------------------------------------------
# Real backends
# ---------------------------------------------------------------------------


def kiro_policy(step: dict[str, Any]) -> Any:
    """The permission policy of a Kiro step.

    Kiro asks ``disclose_context`` before it loads a skill's instructions into the model.
    It writes nothing, so every Kiro step may grant it; without it a skill the agent found
    is silently unusable (the agent answers that it "could not retrieve" it).
    """
    from agentflow.backends import AllowAll, ToolAllowlist

    permission = step.get("permission", "read_only")
    if permission == "read_only":
        return ToolAllowlist({KIRO_SKILL_PERMISSION})
    if permission == "allowlist":
        return ToolAllowlist({*(step.get("tools") or []), KIRO_SKILL_PERMISSION})
    if permission == "full":
        return AllowAll()
    raise ValueError(f"unknown permission {permission!r}")  # pragma: no cover


def step_policy(backend: str, step: dict[str, Any]) -> Any:
    """The permission policy of a step on ``backend``."""
    from agentflow.backends import AllowAll, DenyAll, ToolAllowlist

    if backend == "kiro":
        return kiro_policy(step)
    permission = step.get("permission", "read_only")
    if permission == "read_only":
        return DenyAll()
    if permission == "allowlist":
        return ToolAllowlist(set(step.get("tools") or []))
    if permission == "full":
        return AllowAll()
    raise ValueError(f"unknown permission {permission!r}")  # pragma: no cover


def make_backend(entry: dict[str, Any], step: dict[str, Any], *, cwd: Path | str = "."):
    """Build the SDK backend for one step: the step's permission becomes the policy."""
    name = entry["backend"]
    policy = step_policy(name, step)
    name = entry["backend"]
    if name == "kiro":
        from agentflow.backends.kiro import KiroBackend

        return KiroBackend(entry["agent"], cwd=cwd, permission=policy)
    if name == "claude_code":
        from agentflow.backends.claude_code import ClaudeCodeBackend

        return ClaudeCodeBackend(agent=entry.get("agent"), cwd=cwd, permission=policy)
    if name == "codex":
        from agentflow.backends.codex import CodexBackend

        return CodexBackend(cwd=cwd, permission=policy)
    raise NotImplementedError(
        f"backend {name!r} needs model configuration that an inventory entry cannot carry yet"
    )


#: Receives every event of the turn being collected (text, tool calls, permissions, usage...).
#: Observation only: it cannot change the prompt or the result, and an error in it is ignored.
_observer: ContextVar[Callable[[Any], None] | None] = ContextVar(
    "agentflow_workflow_observer", default=None
)


@contextmanager
def observing(callback: Callable[[Any], None]) -> Iterator[None]:
    """Within this block, :func:`collect_text` reports each backend event to ``callback``."""
    token = _observer.set(callback)
    try:
        yield
    finally:
        _observer.reset(token)


#: The directory the agents of the current run work in, when it is not the one the runner was
#: built with (a run's own worktree). Read when a step starts, so a run cannot leak into another.
_workspace: ContextVar[Path | str | None] = ContextVar("agentflow_workflow_workspace", default=None)


@contextmanager
def in_workspace(path: Path | str) -> Iterator[None]:
    """Within this block, :func:`backend_runner` runs its agents in ``path``."""
    token = _workspace.set(path)
    try:
        yield
    finally:
        _workspace.reset(token)


async def collect_text(backend: Any, prompt: str) -> str:
    """Run one turn on ``backend`` (started and closed here) and return the agent's final text."""
    async with backend:
        return await _run_turn(backend, prompt)


async def _run_turn(backend: Any, prompt: str) -> str:
    """One turn on an already started backend; the caller owns its lifetime."""
    from agentflow.events import ErrorEvent, TextChunk, TurnEnd

    observer = _observer.get()
    text = ""
    async for ev in backend.prompt(prompt):
        if observer is not None:
            with suppress(Exception):  # observation must never break the run
                observer(ev)
        if isinstance(ev, TextChunk):
            text += ev.text
        elif isinstance(ev, ErrorEvent):
            raise BackendError(ev.message)
        elif isinstance(ev, TurnEnd):
            return ev.text or text
    return text


#: The Kiro pool of the current run, when it has one. Read when a step starts.
_pool: ContextVar[Any] = ContextVar("agentflow_workflow_pool", default=None)
_TRUE = frozenset({"1", "true", "yes", "on"})


@asynccontextmanager
async def agent_pool(*, max_size: int = 4, enabled: bool | None = None) -> AsyncIterator[Any]:
    """Within this block, steps reuse long-lived agent processes instead of starting one each.

    Starting an agent CLI and its MCP servers dominates a short step. On Kiro (engine v3) one
    process serves many agents in turn; Claude Code keeps a persistent process per (agent,
    model, permissions); Codex has nothing to keep alive. Every step still gets a fresh
    conversation and its own permission policy. The processes are closed when the block ends,
    however it ends.

    ``enabled=False`` (or ``AGENTFLOW_NO_POOL=1`` in the environment when ``enabled`` is left
    unset) turns reuse off: every step starts its own backend, as before 0.7.0. The block then
    yields ``None``.
    """
    from agentflow.backends.pool import BackendPool

    if enabled is None:
        enabled = os.environ.get("AGENTFLOW_NO_POOL", "").strip().lower() not in _TRUE
    if not enabled:
        yield None
        return
    async with BackendPool(max_size=max_size) as pool:
        token = _pool.set(pool)
        try:
            yield pool
        finally:
            _pool.reset(token)


def backend_runner(*, cwd: Path | str = ".") -> AgentRunner:
    """An :data:`AgentRunner` that runs each attempt on a fresh SDK backend instance.

    A fresh instance per attempt means no conversation leaks between steps or retries
    and no concurrency limit on one instance (Kiro allows one turn at a time). Inside
    :func:`agent_pool` Kiro steps borrow a pooled process instead, with a fresh session.
    """

    async def run(step: dict[str, Any], entry: dict[str, Any], prompt: str) -> str:
        where = _workspace.get() or cwd
        pool = _pool.get()
        kind = entry.get("backend") or ""
        if pool is not None and pool.supports(kind):
            spec = BackendSpec(kind, where, step_policy(kind, step), agent=entry.get("agent"))
            async with pool.lease(spec) as be:
                return await _run_turn(be, prompt)
        return await collect_text(make_backend(entry, step, cwd=where), prompt)

    return run


# ---------------------------------------------------------------------------
# The engine graph
# ---------------------------------------------------------------------------


class WorkflowState(PlanState):
    params: Annotated[dict, last]
    answers: Annotated[dict, last]  # step id -> a human answer supplied with a new run


class StepState(GateState):
    ask: Annotated[dict, last]
    output: Annotated[dict, last]
    parse_reasons: Annotated[list, last]
    status: Annotated[str, last]


def _executor(
    step: dict[str, Any], entry: dict[str, Any], inventory: dict[str, Any]
) -> dict[str, Any]:
    """Who does the step: the backend and its agent, and the role text that shapes it."""
    role = (inventory.get("agents") or {}).get(step["agent"]) or {}
    return {
        "backend": entry.get("backend"),
        "agent": entry.get("agent"),
        "preamble": role.get("preamble", ""),
    }


def _cache_key(step: dict[str, Any], inputs: dict[str, Any], executor: dict[str, Any]) -> str:
    """What the agent sees AND what the result must satisfy: change either, miss the cache. The
    executor is part of it: the same plan on another backend is a different run, not a replay."""
    return artifact_key(
        {
            "executor": executor,
            "agent": step["agent"],
            "prompt": step["prompt"],
            "inputs": inputs,
            "outputs": step.get("outputs") or {},
            "checks": step.get("checks") or [],
        }
    )


def _contract_block(step: dict[str, Any], feedback: str | None, can_ask: bool) -> str:
    outputs = step.get("outputs") or {}
    fields = ", ".join(f'"{name}": <{kind}>' for name, kind in outputs.items())
    block = (
        "### answer format\n"
        f"Finish with ONE JSON object with exactly these fields: {{{fields}}}. "
        "Put nothing after it."
    )
    if can_ask:
        block += (
            "\n\nIf you cannot proceed without a human decision, do NOT guess. Answer instead "
            'with {"needs_clarification": {"question": <string>, "options": [<string>, ...]}} '
            "(options optional)."
        )
    if feedback:
        block += f"\n\n### your previous answer was rejected\n{feedback}"
    return block


def _step_graph(step, entry, inputs, runner: AgentRunner, checkpointer, can_ask: bool):
    outputs = step.get("outputs") or {}
    checks = step.get("checks") or []

    async def execute(state, ctx):
        feedback = ((state.get("gate_results") or {}).get("check") or {}).get("feedback")
        prompt = render_prompt(step, inputs) + "\n\n" + _contract_block(step, feedback, can_ask)
        text = await runner(step, entry, prompt)
        if can_ask:
            question = extract_question(text)
            if question:
                return {"output": {}, "parse_reasons": [], "ask": question}
        fields, reasons = parse_output(text, outputs)
        if fields is None and not can_ask and step.get("may_ask") and extract_question(text):
            reasons = ["you may not ask another question; answer with the required fields"]
        return {"output": fields or {}, "parse_reasons": reasons, "ask": {}}

    async def evaluator(state, ctx) -> GateResult:
        if state.get("ask"):  # a question is a valid way to finish an attempt
            return GateResult(passed=True)
        reasons = list(state.get("parse_reasons") or [])
        if not reasons:
            _, reasons = run_checks(state["output"], checks)
        return GateResult(passed=not reasons, reasons=reasons, feedback="; ".join(reasons) or None)

    async def done(state, ctx):
        return {"status": "asked" if state.get("ask") else "passed"}

    async def give_up(state, ctx):
        return {"status": "escalated"}

    g = Graph(StepState)
    g.add_node("execute", execute)
    g.add_node("done", done)
    g.add_node("give_up", give_up)
    add_quality_gate(
        g,
        "check",
        evaluator,
        on_pass="done",
        on_fail="execute",
        on_escalate="give_up",
        max_attempts=int(step.get("max_attempts", 3)),
    )
    g.add_edge(START, "execute")
    g.add_edge("execute", "check")
    g.add_edge("done", END)
    g.add_edge("give_up", END)
    return g.compile(checkpointer=checkpointer)


def build_workflow_graph(
    plan: dict[str, Any],
    inventory: dict[str, Any],
    runner: AgentRunner,
    *,
    store: Store | None = None,
    checkpointer: Any = None,
    max_fanout: int = 4,
    ask_mode: str = "stop",
    inventory_source: Callable[[], dict[str, Any]] | None = None,
):
    """Compile the fixed engine graph that runs ``plan``. Raises :class:`PlanRefused`.

    ``ask_mode`` decides what a ``may_ask`` question does when the step has no
    ``default_answer``: ``"stop"`` fails the step with ``needs_clarification``;
    ``"interrupt"`` suspends the run for a human. ``inventory_source``, when given, is
    called before every step to re-check the approval against the CURRENT inventory.
    """
    if ask_mode not in ("stop", "interrupt"):
        raise ValueError(f"ask_mode must be 'stop' or 'interrupt', got {ask_mode!r}")
    problems = validate_plan(plan, inventory=inventory) + unsupported_features(plan)
    if has_errors(problems):
        raise PlanRefused([i for i in problems if i.severity == "error"])

    store = store if store is not None else MemoryStore()
    checkpointer = checkpointer if checkpointer is not None else MemoryCheckpointer()
    steps = {s["id"]: s for s in plan["graph"]["steps"]}
    entries = {sid: resolve_backend(s, inventory)[0] for sid, s in steps.items()}
    phash = plan_hash(plan)
    effects = IdempotentOp(store, EFFECT_NS)

    async def gated(step, entry, run_inputs, ctx, sub_thread: str, can_ask: bool) -> dict:
        """Run the gate sub-graph on a durable sub-thread; return its final state."""
        child = _step_graph(step, entry, run_inputs, runner, checkpointer, can_ask)
        cp = await checkpointer.get(sub_thread)
        if cp is not None and cp.done:
            return dict(cp.state)  # finished before a crash or an interrupt: do not run again
        if cp is not None:
            return await child.resume(sub_thread, owner_check=ctx.owner_check)
        return await child.invoke({}, thread=sub_thread, owner_check=ctx.owner_check)

    def envelope_of(final: dict, clar: list[dict]) -> dict[str, Any]:
        attempts = int((final.get("gate_attempts") or {}).get("check", 0))
        return {
            "output": final["output"],
            "artifacts": {},
            "attempts": attempts,
            "clarifications": clar,
        }

    def escalation(final: dict) -> _StepFailed:
        attempts = int((final.get("gate_attempts") or {}).get("check", 0))
        reasons = ((final.get("gate_results") or {}).get("check") or {}).get("reasons")
        return _StepFailed(f"escalated after {attempts} attempt(s): {reasons}")

    async def handler(task: Task, state: Mapping[str, Any], ctx: Any) -> TaskResult:
        if inventory_source is not None:
            revoked = check_approval(plan, inventory_source())
            if revoked:
                raise ApprovalRevoked(revoked)

        step, entry = steps[task.id], entries[task.id]
        executor = _executor(step, entry or {}, inventory)
        results = {r["task_id"]: r for r in (state.get("plan") or {}).get("results") or ()}
        inputs, problems = resolve_inputs(step, results, state.get("params") or {})
        if problems:
            return TaskResult(task.id, failed=True, error="; ".join(problems))

        may = step.get("may_ask")
        clar: list[dict[str, Any]] = []
        supplied = (state.get("answers") or {}).get(task.id)
        if may and supplied is not None:
            clar.append({"question": None, "answer": supplied, "source": "run_parameter"})

        async def one_round(round_no: int) -> tuple[dict | None, dict | None, str]:
            """Run the step once with the clarifications so far.

            Returns (cached envelope | None, final gate state | None, cache key).
            """
            run_inputs = {**inputs, "clarification": clar} if clar else inputs
            key = _cache_key(step, run_inputs, executor)
            cached = await store.get(CACHE_NS, key)
            if cached is not None:
                return {**cached.value, "cached": True}, None, key
            asked = sum(1 for c in clar if c["source"] != "run_parameter")
            can_ask = bool(may) and asked < may["max_questions"]
            sub = f"{ctx.thread}::step::{task.id}" + (f"::q{round_no}" if round_no else "")
            return None, await gated(step, entry, run_inputs, ctx, sub, can_ask), key

        async def execute_once() -> dict[str, Any]:
            cached, final, _ = await one_round(0)
            if cached is not None:
                return cached
            assert final is not None
            if final.get("status") != "passed":
                raise escalation(final)
            return envelope_of(final, clar)

        try:
            if step.get("side_effects"):
                key = _cache_key(step, inputs, executor)
                envelope = await effects.run(f"{phash}:{task.id}:{key}", execute_once)
            elif may:
                envelope = None
                round_no = 0
                while envelope is None:
                    cached, final, key = await one_round(round_no)
                    if cached is not None:
                        return TaskResult(task.id, output=cached)
                    assert final is not None
                    status = final.get("status")
                    if status == "passed":
                        envelope = envelope_of(final, clar)
                    elif status == "asked":
                        q = final["ask"]
                        if may.get("default_answer") is not None:
                            clar.append(
                                {**q, "answer": may["default_answer"], "source": "default_answer"}
                            )
                        elif ask_mode == "interrupt":
                            payload = ClarificationRequest(
                                q["question"], q["options"], {"step": task.id}
                            ).to_payload()
                            answer = await ctx.interrupt(payload)
                            clar.append({**q, "answer": answer, "source": "human"})
                        else:
                            return TaskResult(
                                task.id,
                                failed=True,
                                error=f"needs_clarification: {q['question']}",
                                output={"needs_clarification": {**q, "context": {"step": task.id}}},
                            )
                        round_no += 1
                    else:
                        raise escalation(final)
            else:
                envelope = await execute_once()
        except _StepFailed as exc:
            return TaskResult(task.id, failed=True, error=str(exc))
        except IncompleteEffectError:
            return TaskResult(
                task.id,
                failed=True,
                error="an earlier attempt of this side-effecting step did not finish; "
                "check what it did, then clear its marker or re-plan",
            )
        except NodeError as exc:
            if isinstance(exc.__cause__, (BackendError, OSError, TimeoutError)):
                return TaskResult(task.id, failed=True, error=f"backend error: {exc.__cause__}")
            raise

        assert envelope is not None
        key = _cache_key(step, {**inputs, "clarification": clar} if clar else inputs, executor)
        await store.put(CACHE_NS, key, {k: v for k, v in envelope.items() if k != "cached"})
        return TaskResult(task.id, output=envelope)

    async def planner(state, ctx):
        if (state.get("plan") or {}).get("tasks"):
            return {}
        tasks = [
            Task(s["id"], "agent_step", prerequisites=tuple(s.get("depends_on") or ()))
            for s in plan["graph"]["steps"]
        ]
        return {"plan": {"tasks": tasks}}

    async def idle(state, ctx):
        return {}

    g = Graph(WorkflowState)
    g.add_node("planner", planner)
    add_plan_executor(g, handlers={"agent_step": handler}, on_idle="idle", max_fanout=max_fanout)
    g.add_node("idle", idle)
    g.add_edge(START, "planner")
    g.add_edge("planner", "dispatch")
    g.add_edge("idle", END)
    return g.compile(checkpointer=checkpointer, step_limit=8 * len(steps) + 20)


# ---------------------------------------------------------------------------
# Running and summarising
# ---------------------------------------------------------------------------


@dataclass
class RunOutcome:
    """The honest result of a run: what was produced, what failed, what never ran.

    ``status`` is ``completed | partial | failed`` for a finished run, ``waiting`` when a
    human question is pending (``question``), and ``held`` when the approval stopped
    being valid mid-run (``issues``). A held or waiting run continues with
    :func:`resume_plan`.
    """

    status: str
    thread: str
    final_outputs: list[dict[str, Any]] = field(default_factory=list)  # step, field, value
    failed: list[dict[str, str]] = field(default_factory=list)  # step, error
    blocked: list[dict[str, Any]] = field(default_factory=list)  # step, reason
    steps: dict[str, dict[str, Any]] = field(default_factory=dict)  # per-step status
    needs_clarification: list[dict[str, Any]] = field(default_factory=list)
    question: dict[str, Any] | None = None
    issues: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "thread": self.thread,
            "final_outputs": self.final_outputs,
            "failed": self.failed,
            "blocked": self.blocked,
            "steps": self.steps,
            "needs_clarification": self.needs_clarification,
            "question": self.question,
            "issues": self.issues,
        }


def summarize(plan: dict[str, Any], final: dict[str, Any], thread: str = "") -> RunOutcome:
    """Turn the final state into a :class:`RunOutcome`. Never reports a hollow success."""
    steps = {s["id"]: s for s in plan["graph"]["steps"]}
    results = {r["task_id"]: r for r in (final.get("plan") or {}).get("results") or ()}
    deps = {sid: list(s.get("depends_on") or ()) for sid, s in steps.items()}
    failed_ids = {sid for sid, r in results.items() if r.get("failed")}

    per_step: dict[str, dict[str, Any]] = {}
    failed, blocked, asks = [], [], []
    for sid in steps:
        r = results.get(sid)
        if r is None:
            culprits = sorted(failed_ids & ancestors_of(sid, deps))
            reason = f"blocked by failed step(s) {culprits}" if culprits else "never ran"
            blocked.append({"step": sid, "reason": reason})
            per_step[sid] = {"status": "blocked"}
        elif r.get("failed"):
            failed.append({"step": sid, "error": r.get("error") or ""})
            per_step[sid] = {"status": "failed"}
            ask = (r.get("output") or {}).get("needs_clarification")
            if ask:
                asks.append({"step": sid, **ask})
        else:
            out = r.get("output") or {}
            per_step[sid] = {
                "status": "ok",
                "attempts": out.get("attempts"),
                "cached": bool(out.get("cached")),
                "clarifications": out.get("clarifications") or [],
            }

    produced = []
    for ref in plan["final_outputs"]:
        r = results.get(ref["step"])
        if r is not None and not r.get("failed"):
            fields = (r.get("output") or {}).get("output") or {}
            if ref["field"] in fields:
                produced.append({**ref, "value": fields[ref["field"]]})
    if len(produced) == len(plan["final_outputs"]):
        status = "completed"
    elif produced:
        status = "partial"
    else:
        status = "failed"
    return RunOutcome(status, thread, produced, failed, blocked, per_step, asks)


def _refusals(
    plan: dict[str, Any],
    inventory: dict[str, Any],
    params: dict[str, Any] | None,
    require_approval: bool,
    check_params: bool,
) -> list[Issue]:
    issues: list[Issue] = []
    if require_approval:
        issues += check_approval(plan, inventory)
    if check_params:
        declared = plan.get("parameters") or {}
        given = params or {}
        for name, kind in declared.items():
            if name not in given:
                issues.append(Issue("missing_parameter", f"run parameter {name!r} is required"))
            elif kind in TYPES and not type_ok(given[name], kind):
                issues.append(Issue("bad_parameter", f"run parameter {name!r} must be {kind}"))
        for name in given:
            if name not in declared:
                issues.append(Issue("unknown_parameter", f"run parameter {name!r} is not declared"))
    return issues


async def _outcome(graph, plan, thread: str, final: dict[str, Any] | None, held=None) -> RunOutcome:
    if held is not None:
        return RunOutcome("held", thread, issues=[i.to_dict() for i in held.issues])
    cp = await graph.get_state(thread)
    if cp is not None and cp.interrupted:
        payload = cp.interrupt_payload
        return RunOutcome(
            "waiting", thread, question=payload if isinstance(payload, dict) else None
        )
    return summarize(plan, final if final is not None else dict(cp.state), thread)


def _held(exc: NodeError) -> ApprovalRevoked | None:
    cause = exc.__cause__
    return cause if isinstance(cause, ApprovalRevoked) else None


async def run_plan(
    plan: dict[str, Any],
    inventory: dict[str, Any],
    runner: AgentRunner,
    *,
    params: dict[str, Any] | None = None,
    answers: dict[str, Any] | None = None,
    thread: str | None = None,
    store: Store | None = None,
    checkpointer: Any = None,
    require_approval: bool = True,
    max_fanout: int = 4,
    ask_mode: str = "stop",
    inventory_source: Callable[[], dict[str, Any]] | None = None,
) -> RunOutcome:
    """Run an approved plan and report an honest outcome.

    Refuses (``PlanRefused``) a plan that is invalid, unsupported, not approved (or whose
    approval is stale), or that lacks a declared run parameter. ``answers`` maps a
    ``may_ask`` step id to a human answer given to an earlier, stopped run: this is how
    "answer and start a new run" works, and finished steps come from the cache.
    """
    issues = _refusals(plan, inventory, params, require_approval, check_params=True)
    for sid in answers or {}:
        step = next((s for s in plan["graph"]["steps"] if s["id"] == sid), None)
        if step is None or not step.get("may_ask"):
            issues.append(Issue("unknown_answer", f"step {sid!r} cannot take an answer", step=sid))
    if issues:
        raise PlanRefused(issues)

    store = store if store is not None else MemoryStore()
    checkpointer = checkpointer if checkpointer is not None else MemoryCheckpointer()
    graph = build_workflow_graph(
        plan,
        inventory,
        runner,
        store=store,
        checkpointer=checkpointer,
        max_fanout=max_fanout,
        ask_mode=ask_mode,
        inventory_source=inventory_source,
    )
    thread = thread or f"run-{uuid.uuid4().hex[:12]}"
    try:
        final = await graph.invoke(
            {"params": dict(params or {}), "answers": dict(answers or {})}, thread=thread
        )
    except NodeError as exc:
        held = _held(exc)
        if held is None:
            raise
        return await _outcome(graph, plan, thread, None, held=held)
    return await _outcome(graph, plan, thread, final)


async def resume_plan(
    plan: dict[str, Any],
    inventory: dict[str, Any],
    runner: AgentRunner,
    *,
    thread: str,
    answer: Any = _NO_ANSWER,
    store: Store,
    checkpointer: Any,
    require_approval: bool = True,
    max_fanout: int = 4,
    ask_mode: str = "stop",
    inventory_source: Callable[[], dict[str, Any]] | None = None,
) -> RunOutcome:
    """Continue a run that is waiting for a human, held, or was interrupted by a crash.

    The ``store`` and ``checkpointer`` must be the ones the run used. The approval is
    re-checked first, so a run held because of a changed inventory continues only after
    approval is valid again. A waiting run needs ``answer``.
    """
    issues = _refusals(plan, inventory, None, require_approval, check_params=False)
    cp = await checkpointer.get(thread)
    if cp is None:
        issues.append(Issue("unknown_thread", f"no run {thread!r} in this checkpointer"))
    elif cp.interrupted and answer is _NO_ANSWER:
        issues.append(Issue("missing_answer", "the run is waiting for a human answer"))
    if issues:
        raise PlanRefused(issues)

    graph = build_workflow_graph(
        plan,
        inventory,
        runner,
        store=store,
        checkpointer=checkpointer,
        max_fanout=max_fanout,
        ask_mode=ask_mode,
        inventory_source=inventory_source,
    )
    assert cp is not None
    if cp.done:
        return summarize(plan, dict(cp.state), thread)
    try:
        if cp.interrupted:
            final = await graph.resume(thread, value=answer)
        else:
            final = await graph.resume(thread)
    except NodeError as exc:
        held = _held(exc)
        if held is None:
            raise
        return await _outcome(graph, plan, thread, None, held=held)
    return await _outcome(graph, plan, thread, final)
