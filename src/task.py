"""
Task abstraction.

A Task encapsulates everything specific to one kind of autonomous
optimization job: how to set up its workspace, what prompts to send,
how to read the agent's structured decision, and how to summarise
progress for logging.

The orchestrator (runner.AgentLoopRunner) is task-agnostic: it drives
the iterate-until-converged loop and evaluates stop conditions purely
from the Decision contract below. To add a new autonomous agent, create
a new Task subclass — no changes to the orchestrator are needed.

The contract between orchestrator and any task is the Decision:
the agent writes a JSON file each turn, and the task parses it into
a Decision. The orchestrator only ever reads these fields.
"""

from __future__ import annotations

import abc
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class Decision:
    """
    Structured result the agent produces at the end of every turn.

    This is the stable API between the orchestrator and any task.
    Stop conditions are evaluated only from these fields, so every
    task must map its agent's output onto this shape.
    """

    score: float
    improved: bool
    done: bool
    stop_reason: str | None = None
    # Free-form extra fields for telemetry (timing, penalties, etc.)
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Decision":
        """
        Parse a raw decision dict written by the agent.

        Recognises the canonical keys (score/improved/done/stopReason)
        and preserves everything else under `extra` for telemetry.
        """
        if not isinstance(data, dict):
            raise ValueError("decision must be a JSON object")

        required = {"score", "improved", "done"}
        missing = required - data.keys()
        if missing:
            raise ValueError(f"decision is missing required fields: {sorted(missing)}")

        score = data["score"]
        if isinstance(score, bool) or not isinstance(score, (int, float)):
            raise ValueError("decision.score must be a JSON number")
        score = float(score)
        if not math.isfinite(score):
            raise ValueError("decision.score must be finite")

        for name in ("improved", "done"):
            if not isinstance(data[name], bool):
                raise ValueError(f"decision.{name} must be a JSON boolean")

        stop_reason = data.get("stopReason", data.get("stop_reason"))
        if stop_reason is not None and not isinstance(stop_reason, str):
            raise ValueError("decision.stopReason must be a string or null")

        known = {"score", "improved", "done", "stopReason", "stop_reason"}
        return cls(
            score=score,
            improved=data["improved"],
            done=data["done"],
            stop_reason=stop_reason,
            extra={k: v for k, v in data.items() if k not in known},
        )


class Task(abc.ABC):
    """
    Base class for an autonomous optimization task.

    Subclasses implement the task-specific pieces. The orchestrator
    calls these hooks but never contains any task-specific logic.
    """

    #: Short identifier used to select the task from the CLI.
    name: str = "task"

    #: Human-readable label used in log headers.
    label: str = "Optimization"

    @abc.abstractmethod
    def setup_workspace(self) -> None:
        """
        Prepare the agent's working directory for a fresh run.

        Copy input files into place, clean up stale artifacts from a
        previous run, create output directories, etc.
        """

    @abc.abstractmethod
    def baseline_prompt(self) -> str:
        """Return the prompt text for the baseline (phase 1) turn."""

    @abc.abstractmethod
    def iteration_prompt(self) -> str:
        """Return the prompt text for one optimization iteration."""

    @abc.abstractmethod
    def read_decision(self) -> Decision | None:
        """
        Read and parse the agent's decision for the last turn.

        Return None if the agent did not produce a valid decision
        (missing/corrupt file) — the orchestrator treats this as
        "agent did not respond this turn".
        """

    def summarize(self, decision: Decision, *, baseline: bool = False) -> str:
        """
        Produce a short human-readable summary line for a turn.

        Default implementation prints the score. Override to surface
        task-specific metrics (e.g. COLLSCAN, fan-out, timing).
        """
        prefix = "Baseline" if baseline else "Score"
        return f"{prefix}: {decision.score:g}"

    def run_config_summary(self) -> dict[str, Any]:
        """
        Return task-specific fields to record in the run_start telemetry
        event and log header. Override to include input paths etc.
        """
        return {}

    def artifact_hint(self) -> str:
        """
        Return a short pointer to where the best result is saved,
        shown in the final report. Override per task.
        """
        return "(see task output directory)"

    def verify_candidate(self) -> str | None:
        """Run the task-owned verifier after an agent turn.

        Return ``None`` only when a fresh verifier result was produced. A
        non-empty error message makes the run fail closed before a decision is
        trusted. Tasks without a verifier may retain the default no-op.
        """
        return None

    def is_improvement(self, candidate: Decision, best: Decision) -> bool:
        """Whether independently measured candidate should replace best."""
        return candidate.score > best.score

    def promote_candidate(self, decision: Decision) -> None:
        """Persist an independently verified candidate as the task's best."""
        return

    def restore_best_candidate(self) -> None:
        """Discard a rejected candidate before the next agent turn."""
        return

    def read_best_decision(self) -> Decision | None:
        """Return the persisted verified best, if this task has one."""
        return None


class FileTask(Task):
    """
    Base class for tasks that follow the standard file-on-disk pattern:

      - prompts live in `prompts_dir` as 00_prompt.md (baseline) and
        01_prompt.md (iteration)
      - the agent writes `decision_file` each turn
      - an evaluator writes `result_file`, whose objective numbers may
        override the agent's self-reported decision

    Subclasses set the path attributes below (usually as class attributes
    pointing at agent_workspace/ and the task's prompts/ directory) and
    implement `setup_workspace`. They override `ground_decision` to fold the
    evaluator's result into the Decision, and the reporting hooks as needed.

    This removes the boilerplate that was duplicated across every task:
    prompt reading and the read-decision-then-ground flow.
    """

    #: Directory holding 00_prompt.md and 01_prompt.md.
    prompts_dir: Path
    #: The decision.json the agent writes each turn.
    decision_file: Path
    #: The evaluator's result.json (optional; grounding is skipped if absent).
    result_file: Path | None = None
    #: Whether absence of the evaluator result invalidates an agent decision.
    require_result: bool = False

    def baseline_prompt(self) -> str:
        return (self.prompts_dir / "00_prompt.md").read_text(encoding="utf-8")

    def iteration_prompt(self) -> str:
        return (self.prompts_dir / "01_prompt.md").read_text(encoding="utf-8")

    def read_decision(self) -> Decision | None:
        from . import workspace

        raw = workspace.read_json(self.decision_file)
        if raw is None:
            return None

        try:
            decision = Decision.from_dict(raw)
        except ValueError:
            return None

        if self.result_file is not None:
            result = workspace.read_json(self.result_file)
            if result is None:
                return None if self.require_result else decision
            self.ground_decision(decision, result)

        return decision

    def ground_decision(self, decision: Decision, result: dict[str, Any]) -> None:
        """
        Fold the evaluator's objective result into the agent's decision,
        in place. Default is a no-op (the agent's self-report stands).

        Override to make the evaluator authoritative: set `decision.score`
        from the result, copy metrics into `decision.extra`, and enforce
        any hard gate on `decision.done` / `decision.stop_reason`.
        """
        return

    def read_best_decision(self) -> Decision | None:
        from . import workspace

        raw = workspace.read_json(workspace.BEST_DIR / "decision.json")
        if raw is None:
            return None
        try:
            return Decision.from_dict(raw)
        except ValueError:
            return None
