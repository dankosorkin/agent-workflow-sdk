"""
MongoDB aggregation optimization task.

Implements the Task interface for optimizing a MongoDB aggregation
pipeline. All MongoDB-specific behavior lives here — the orchestrator
knows nothing about pipelines, COLLSCANs, or explain plans.

Workspace layout (relative to project root):
  agent_workspace/
    input/aggregation.js   — original pipeline (read-only for agent)
    input/structure.js     — target schema/indexes (read-only for agent)
    aggregation.js         — current candidate pipeline
    result.json            — latest measurement from eval/measure.js
    decision.json          — agent's structured decision each turn
  best/
    aggregation.js         — best pipeline found
    decision.json          — decision from the best run
"""

from dataclasses import dataclass
import hashlib
import math
from pathlib import Path
import subprocess
from typing import Any

from src import workspace
from src.task import Decision, FileTask

TASK_DIR = Path(__file__).resolve().parent
INPUT_DIR = workspace.WORKSPACE / "input"


@dataclass
class MongoAggregationConfig:
    """Task-specific inputs supplied from the CLI."""
    aggregation_file: Path
    structure_file: Path


class MongoAggregationTask(FileTask):

    name = "mongo-aggregation"
    label = "MongoDB Aggregation Optimization"

    prompts_dir = TASK_DIR / "prompts"
    decision_file = workspace.WORKSPACE / "decision.json"
    result_file = workspace.WORKSPACE / "result.json"
    require_result = True

    def __init__(self, config: MongoAggregationConfig):
        self.config = config

    def setup_workspace(self) -> None:
        for label, path in [
            ("--aggregation", self.config.aggregation_file),
            ("--structure", self.config.structure_file),
        ]:
            if not path.exists():
                raise FileNotFoundError(f"{label} file not found: {path}")

        workspace.ensure_dirs(INPUT_DIR, workspace.BEST_DIR)
        workspace.copy_into(self.config.aggregation_file, INPUT_DIR / "aggregation.js")
        workspace.copy_into(self.config.structure_file, INPUT_DIR / "structure.js")
        workspace.clean_files(
            workspace.WORKSPACE / "aggregation.js",
            workspace.WORKSPACE / "result.json",
            self.decision_file,
        )
        print(f"Workspace ready: {workspace.WORKSPACE}")
        print(f"  aggregation: {self.config.aggregation_file}")
        print(f"  structure:   {self.config.structure_file}")

    def ground_decision(self, decision: Decision, result: dict[str, Any]) -> None:
        """Make a completed, sufficiently detailed measurement authoritative."""
        plan = result.get("plan")
        timing = result.get("timing")
        score = result.get("score")

        score_is_valid = (
            not isinstance(score, bool)
            and isinstance(score, (int, float))
            and math.isfinite(float(score))
        )
        timing_is_valid = (
            isinstance(timing, dict)
            and not isinstance(timing.get("avgMs"), bool)
            and isinstance(timing.get("avgMs"), (int, float))
            and math.isfinite(float(timing["avgMs"]))
        )
        plan_is_complete = (
            isinstance(plan, dict)
            and all(
                not isinstance(plan.get(name), bool)
                and isinstance(plan.get(name), (int, float))
                and math.isfinite(float(plan[name]))
                for name in ("docsExamined", "docsReturned")
            )
            and isinstance(result.get("hasCollscan"), bool)
            and isinstance(result.get("hasFanout"), bool)
        )
        measurement_complete = score_is_valid and timing_is_valid and plan_is_complete

        decision.extra.update({
            "measurementComplete": measurement_complete,
            "timingMs": timing.get("avgMs") if isinstance(timing, dict) else None,
            "hasCollscan": result.get("hasCollscan"),
            "hasFanout": result.get("hasFanout"),
            "cardinalityRatio": result.get("cardinalityRatio"),
            "penalties": result.get("penalties", []),
        })

        if not measurement_complete:
            decision.score = 0
            decision.done = False
            decision.stop_reason = None
            decision.extra["measurementError"] = (
                "evaluator result lacks a finite score, timing, or execution counters"
            )
            return

        decision.score = float(score)

    def summarize(self, decision: Decision, *, baseline: bool = False) -> str:
        e = decision.extra
        prefix = "Baseline" if baseline else "Score"
        parts = [f"{prefix}: {decision.score:g}/100"]
        if "timingMs" in e:
            parts.append(f"time={e['timingMs']}ms")
        if "hasCollscan" in e:
            parts.append(f"COLLSCAN={e['hasCollscan']}")
        if "hasFanout" in e:
            parts.append(f"fan-out={e['hasFanout']}")
        if e.get("cardinalityRatio") is not None:
            parts.append(f"cardinality={e['cardinalityRatio']}")
        return "  ".join(parts)

    def run_config_summary(self) -> dict:
        return {
            "aggregation_file": str(self.config.aggregation_file),
            "structure_file": str(self.config.structure_file),
        }

    def artifact_hint(self) -> str:
        return str(workspace.BEST_DIR / "aggregation.js")

    def verify_candidate(self) -> str | None:
        candidate = workspace.WORKSPACE / "aggregation.js"
        if not candidate.exists():
            return f"candidate file not found: {candidate}"

        try:
            completed = subprocess.run(
                ["node", str(TASK_DIR / "eval" / "measure.js"), str(candidate)],
                cwd=workspace.PROJECT_ROOT, text=True,
                capture_output=True, timeout=180, check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return f"could not run aggregation evaluator: {exc}"
        if completed.returncode != 0:
            return f"aggregation evaluator exited {completed.returncode}: {completed.stderr.strip()}"

        result = workspace.read_json(self.result_file)
        expected_hash = hashlib.sha256(candidate.read_bytes()).hexdigest()
        if result is None or result.get("candidateSha256") != expected_hash:
            return "aggregation evaluator did not produce a result for the current candidate"
        return None

    def is_improvement(self, candidate: Decision, best: Decision) -> bool:
        if candidate.score > best.score:
            return True
        if candidate.score != best.score:
            return False
        candidate_time = candidate.extra.get("timingMs")
        best_time = best.extra.get("timingMs")
        return (
            isinstance(candidate_time, (int, float))
            and not isinstance(candidate_time, bool)
            and math.isfinite(candidate_time)
            and isinstance(best_time, (int, float))
            and not isinstance(best_time, bool)
            and math.isfinite(best_time)
            and candidate_time < best_time * 0.9
        )

    def promote_candidate(self, decision: Decision) -> None:
        candidate = workspace.WORKSPACE / "aggregation.js"
        workspace.copy_into_atomic(candidate, workspace.BEST_DIR / "aggregation.js")
        workspace.copy_into_atomic(self.result_file, workspace.BEST_DIR / "result.json")
        workspace.write_json_atomic(workspace.BEST_DIR / "decision.json", _decision_json(decision))

    def restore_best_candidate(self) -> None:
        best = workspace.BEST_DIR / "aggregation.js"
        if best.exists():
            workspace.copy_into_atomic(best, workspace.WORKSPACE / "aggregation.js")
        best_result = workspace.BEST_DIR / "result.json"
        if best_result.exists():
            workspace.copy_into_atomic(best_result, self.result_file)


def _decision_json(decision: Decision) -> dict[str, Any]:
    return {
        "score": decision.score,
        "improved": decision.improved,
        "done": decision.done,
        "stopReason": decision.stop_reason,
        **decision.extra,
    }
