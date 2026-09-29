"""
Agent loop runner — task-agnostic orchestrator.

Drives a two-phase iterate-until-converged loop for ANY autonomous
agent task:
  Phase 1 — baseline turn
  Phase 2 — iterative optimization with early stopping

All task-specific behavior lives behind the Task interface. The runner
only ever reads the Decision contract (score / improved / done /
stop_reason) to evaluate stop conditions, so a new autonomous agent is
added by writing a new Task subclass — never by editing this file.
"""

import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .acp import AcpError, KiroAcpClient
from .task import Decision, Task
from .telemetry import Telemetry


ROOT = Path(__file__).resolve().parent.parent

# A failed hypothesis often reveals new deterministic evidence (for example a
# type conversion error). Two attempts are not enough for an agent to use that
# evidence, inspect the source, and try a different repair. The explicit
# iteration cap remains the hard budget.
NO_IMPROVEMENT_LIMIT = 4
PERFECT_SCORE = 100


@dataclass
class LoopConfig:
    """Generic loop settings — nothing task-specific here."""
    iterations: int
    agent: str
    model: str
    telemetry_dir: Path
    engine: str = "v3"
    no_improvement_limit: int = NO_IMPROVEMENT_LIMIT
    perfect_score: float = PERFECT_SCORE


class AgentLoopRunner:

    def __init__(self, task: Task, config: LoopConfig):
        self.task = task
        self.config = config
        self.run_id = (
            datetime.now().strftime("%Y%m%d_%H%M%S")
            + "_"
            + uuid.uuid4().hex[:6]
        )
        self.tel = Telemetry(self.run_id, config.telemetry_dir)
        self.client = KiroAcpClient(
            agent=config.agent,
            model=config.model,
            cwd=ROOT,
            engine=config.engine,
        )

        self.baseline_score: float = 0
        self.best_score: float = 0
        self.best_decision: Decision | None = None
        self.no_improvement_streak = 0
        self.final_iteration = 0
        self.stop_reason: str | None = None
        self.completed = False

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def run(self) -> int:
        self._log_header()
        self.tel.record("run_start", {
            "task": self.task.name,
            "max_iterations": self.config.iterations,
            "agent": self.config.agent,
            "model": self.config.model,
            **self.task.run_config_summary(),
        })

        try:
            self.task.setup_workspace()
        except OSError as exc:
            print(f"ERROR setting up workspace: {exc}", file=sys.stderr)
            self.tel.record("error", {"phase": "setup", "reason": str(exc)})
            return 1

        try:
            self.client.start()

            if not self._run_baseline():
                return 1

            if self.stop_reason is not None:
                # Agent finished at baseline
                self._log_final()
                return 0 if self.completed else 2

            self._run_optimization_loop()
            self._log_final()
            return 0 if self.completed else 2

        except (AcpError, OSError, KeyboardInterrupt) as exc:
            print(f"\nERROR: {exc}", file=sys.stderr)
            self.tel.record("error", {"reason": str(exc)})
            return 1

        finally:
            self.client.close()

    # ------------------------------------------------------------------
    # Phase 1 — Baseline
    # ------------------------------------------------------------------

    def _run_baseline(self) -> bool:
        """Returns False on hard failure (missing decision)."""
        self.tel.log("\n=== Phase 1: Baseline ===\n")
        self.tel.record("phase_start", {"phase": "baseline"})

        elapsed = self._prompt_timed(self.task.baseline_prompt())
        if not self._verify_candidate("baseline", 0):
            return False

        decision = self.task.read_decision()
        if decision is None:
            self.tel.log("\nERROR: Agent did not produce a valid decision during baseline.")
            self.tel.record("error", {"phase": "baseline", "reason": "missing decision"})
            return False

        self._record_iteration("baseline", 0, elapsed, decision)

        self.baseline_score = decision.score
        persisted_best = self.task.read_best_decision()
        if persisted_best is not None and not self.task.is_improvement(decision, persisted_best):
            self.best_score = persisted_best.score
            self.best_decision = persisted_best
            self.task.restore_best_candidate()
            self.tel.log("\n— Baseline did not beat persisted best; restored best candidate.")
        else:
            self.best_score = decision.score
            self.best_decision = decision
            self.task.promote_candidate(decision)

        self.tel.log("\n" + self.task.summarize(decision, baseline=True))

        if decision.done:
            self.stop_reason = decision.stop_reason or "done_at_baseline"
            self.completed = True
            self.tel.log(f"\nAgent signalled done at baseline: {self.stop_reason}")

        return True

    # ------------------------------------------------------------------
    # Phase 2 — Optimization loop
    # ------------------------------------------------------------------

    def _run_optimization_loop(self) -> None:
        self.tel.log("\n=== Baseline complete. Starting iterations ===")
        prompt = self.task.iteration_prompt()

        for i in range(1, self.config.iterations + 1):
            self.tel.log(f"\n=== Iteration {i} / {self.config.iterations} ===\n")
            self.tel.record("iteration_start", {
                "iteration": i,
                "best_score_so_far": self.best_score,
            })

            elapsed = self._prompt_timed(prompt)
            if not self._verify_candidate("optimization", i):
                self.stop_reason = "verification_failed"
                break
            decision = self.task.read_decision()

            if decision is None:
                self.tel.log(f"\nWARNING: No decision after iteration {i}. Skipping.")
                self.tel.record("warning", {"iteration": i, "reason": "missing decision"})
                continue

            self.final_iteration = i
            self._record_iteration("optimization", i, elapsed, decision)

            if self._check_stop(decision):
                break

    def _check_stop(self, decision: Decision) -> bool:
        """Update streak/best and evaluate all stop conditions. Returns True to stop."""
        if self.best_decision is None:
            raise RuntimeError("best decision was not initialized")

        if self.task.is_improvement(decision, self.best_decision):
            self.best_score = decision.score
            self.best_decision = decision
            self.task.promote_candidate(decision)
            self.no_improvement_streak = 0
            self.tel.log("\n" + self.task.summarize(decision))
        else:
            self.task.restore_best_candidate()
            self.no_improvement_streak += 1
            self.tel.log(f"\n— No improvement (streak: {self.no_improvement_streak})")

        if decision.done:
            self.stop_reason = decision.stop_reason or "done"
            self.completed = True
            self.tel.log(f"\n>>> Agent signalled DONE: {self.stop_reason}")
            return True

        if self.best_score >= self.config.perfect_score:
            self.stop_reason = "optimal"
            # A task-owned verifier, rather than the agent, established the
            # terminal objective. This is a successful completion even when
            # the agent's advisory decision left `done` false.
            self.completed = True
            self.tel.log(f"\n>>> Perfect score reached ({self.best_score:g}). Stopping.")
            return True

        if self.no_improvement_streak >= self.config.no_improvement_limit:
            self.stop_reason = "converged"
            self.tel.log(
                f"\n>>> No improvement for {self.no_improvement_streak} "
                f"consecutive iterations. Stopping."
            )
            return True

        return False

    def _verify_candidate(self, phase: str, iteration: int) -> bool:
        """Run the task-owned verifier before accepting agent output."""
        error = self.task.verify_candidate()
        if error is None:
            self.tel.record("verification", {
                "phase": phase,
                "iteration": iteration,
                "ok": True,
            })
            return True

        self.tel.log(f"\nERROR: Candidate verification failed: {error}")
        self.tel.record("error", {
            "phase": phase,
            "iteration": iteration,
            "reason": "verification_failed",
            "detail": error,
        })
        return False

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _prompt_timed(self, prompt: str) -> float:
        t0 = time.monotonic()
        self.client.prompt(prompt)
        return time.monotonic() - t0

    def _record_iteration(
        self, phase: str, iteration: int, elapsed: float, decision: Decision
    ) -> None:
        self.tel.record("iteration", {
            "phase": phase,
            "iteration": iteration,
            "elapsed_s": round(elapsed, 2),
            "score": decision.score,
            "improved": decision.improved,
            "done": decision.done,
            "stopReason": decision.stop_reason,
            **decision.extra,
        })

    def _log_header(self) -> None:
        self.tel.log(f"\n{'='*60}")
        self.tel.log(f"Run ID: {self.run_id}")
        self.tel.log(f"Task:   {self.task.name} — {self.task.label}")
        for key, value in self.task.run_config_summary().items():
            self.tel.log(f"  {key}: {value}")
        self.tel.log(f"Max iterations: {self.config.iterations}")
        self.tel.log(f"Agent: {self.config.agent}  Model: {self.config.model}")
        self.tel.log(f"{'='*60}\n")

    def _log_final(self) -> None:
        stop = self.stop_reason or "max_iterations"
        outcome = "COMPLETE" if self.completed else "INCOMPLETE"
        self.tel.log(f"\n{'='*60}")
        self.tel.log(f"{self.task.label.upper()} {outcome}")
        self.tel.log(f"{'='*60}")
        self.tel.log(f"Run ID:              {self.run_id}")
        self.tel.log(f"Iterations:          {self.final_iteration}")
        self.tel.log(f"Baseline score:      {self.baseline_score:g}")
        self.tel.log(f"Final best score:    {self.best_score:g}")
        self.tel.log(f"Stop reason:         {stop}")
        self.tel.log(f"Best result:         {self.task.artifact_hint()}")
        self.tel.log(f"Telemetry:           {self.tel.path}")
        self.tel.log(f"{'='*60}\n")

        self.tel.record("run_end", {
            "iterations_completed": self.final_iteration,
            "baseline_score": self.baseline_score,
            "final_score": self.best_score,
            "improvement": self.best_score - self.baseline_score,
            "stop_reason": stop,
            "completed": self.completed,
        })
