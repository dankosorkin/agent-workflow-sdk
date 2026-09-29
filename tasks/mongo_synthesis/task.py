"""
MongoDB aggregation synthesis task.

Synthesizes an aggregation pipeline that transforms N source collections
into a target structure. Unlike the mongo-aggregation task (which tunes
an existing pipeline), here the agent BUILDS the pipeline from scratch.

Two-phase progress, with correctness as a hard gate:
  - synthesis phase:    output must match the target structure (presence + type)
  - optimization phase: only unlocked once correctness == 100

The evaluator (eval/evaluate.js) runs the candidate in-memory via mingo and
writes result.json with the structural diff and correctness. This task grounds
the agent's decision in that result — the agent cannot declare the job done
while required target paths are still missing.

Downstream: once synthesis reaches 100% correctness, best/aggregation.js can
be fed to the mongo-aggregation task for real-MongoDB performance tuning.
"""

from dataclasses import dataclass
import hashlib
from pathlib import Path
import subprocess
from typing import Any

from src import workspace
from src.task import Decision, FileTask

TASK_DIR = Path(__file__).resolve().parent

DEFAULT_SOURCES_DIR = TASK_DIR / "data"
DEFAULT_TARGET_FILE = TASK_DIR / "structure" / "PartyContact.json"

FULL_CORRECTNESS = 100


@dataclass
class MongoSynthesisConfig:
    sources_dir: Path
    target_file: Path


class MongoSynthesisTask(FileTask):

    name = "mongo-synthesis"
    label = "MongoDB Aggregation Synthesis"

    prompts_dir = TASK_DIR / "prompts"
    decision_file = workspace.WORKSPACE / "decision.json"
    result_file = workspace.WORKSPACE / "result.json"
    require_result = True

    def __init__(self, config: MongoSynthesisConfig):
        self.config = config

    def setup_workspace(self) -> None:
        if not self.config.sources_dir.exists():
            raise FileNotFoundError(f"--sources dir not found: {self.config.sources_dir}")
        if not self.config.target_file.exists():
            raise FileNotFoundError(f"--target file not found: {self.config.target_file}")

        workspace.ensure_dirs(workspace.WORKSPACE, workspace.BEST_DIR)
        workspace.clean_files(self.result_file, self.decision_file)
        best_candidate = workspace.BEST_DIR / "aggregation.js"
        if best_candidate.exists():
            workspace.copy_into_atomic(best_candidate, workspace.WORKSPACE / "aggregation.js")
            print(f"  seeded candidate: {best_candidate}")
        else:
            workspace.clean_files(workspace.WORKSPACE / "aggregation.js")
        source_count = len(list(self.config.sources_dir.glob("*.json")))
        print(f"Workspace ready: {workspace.WORKSPACE}")
        print(f"  sources: {self.config.sources_dir} ({source_count} collections)")
        print(f"  target:  {self.config.target_file}")

    def ground_decision(self, decision: Decision, result: dict[str, Any]) -> None:
        # The agent writes decision.json, but correctness/score come from
        # evaluate.js. This prevents the agent from claiming success while the
        # output does not match the target structure.
        correctness = result.get("correctness", 0)
        decision.score = result.get("score", correctness)
        decision.extra["correctness"] = correctness
        decision.extra["phase"] = result.get("phase", "synthesis")
        decision.extra["outputDocCount"] = result.get("outputDocCount")
        decision.extra["missingRequired"] = result.get("missingRequired", [])

        # Completion is an evaluator fact, not an agent opinion. Below the
        # gate a premature done is rejected; at the gate the harness records
        # the terminal state even if the agent forgot to set it.
        if correctness >= FULL_CORRECTNESS:
            decision.done = True
            decision.stop_reason = "optimal"
        else:
            decision.done = False
            if decision.stop_reason in ("optimal", "resolved"):
                decision.stop_reason = None

    def summarize(self, decision: Decision, *, baseline: bool = False) -> str:
        e = decision.extra
        prefix = "Baseline" if baseline else "Correctness"
        parts = [f"{prefix}: {e.get('correctness', decision.score):g}%"]
        if "phase" in e:
            parts.append(f"phase={e['phase']}")
        missing = e.get("missingRequired") or []
        if missing:
            parts.append(f"missing={len(missing)} paths")
        if e.get("outputDocCount") is not None:
            parts.append(f"docs={e['outputDocCount']}")
        return "  ".join(parts)

    def run_config_summary(self) -> dict:
        return {
            "sources_dir": str(self.config.sources_dir),
            "target_file": str(self.config.target_file),
        }

    def artifact_hint(self) -> str:
        return str(workspace.BEST_DIR / "aggregation.js")

    def verify_candidate(self) -> str | None:
        candidate = workspace.WORKSPACE / "aggregation.js"
        if not candidate.exists():
            return f"candidate file not found: {candidate}"

        command = [
            "node", str(TASK_DIR / "eval" / "evaluate.js"), str(candidate),
            "--sources", str(self.config.sources_dir),
            "--target", str(self.config.target_file),
        ]
        try:
            completed = subprocess.run(
                command, cwd=workspace.PROJECT_ROOT, text=True,
                capture_output=True, timeout=60, check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            return f"could not run synthesis evaluator: {exc}"
        if completed.returncode != 0:
            return f"synthesis evaluator exited {completed.returncode}: {completed.stderr.strip()}"

        result = workspace.read_json(self.result_file)
        expected_hash = hashlib.sha256(candidate.read_bytes()).hexdigest()
        if result is None or result.get("candidateSha256") != expected_hash:
            return "synthesis evaluator did not produce a result for the current candidate"
        return None

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
