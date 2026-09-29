"""
MongoDB schema analysis task.

Analyzes a live MongoDB to discover, for each source collection:
  - indexes and document counts
  - field cardinality
  - unique keys (guaranteed from unique indexes, plus inferred+verified)
  - relationships between collections (hypotheses from the target mapping,
    confirmed against real data with cardinality)

Produces `structure.js` — the physical schema artifact that the
mongo-aggregation task consumes for optimization. This is the first step of
the mongo-full chain; its output feeds synthesis and optimization.

The evaluator (eval/analyze.js) is read-only and both introspects the live
database and scores the agent's analysis.json by VERIFYING every claimed key
($group test) and relationship (value overlap) against real data — the agent
cannot claim a key or link that the data does not support.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src import workspace
from src.task import Decision, FileTask

TASK_DIR = Path(__file__).resolve().parent

ANALYSIS_FILE = workspace.WORKSPACE / "analysis.json"
STRUCTURE_FILE = workspace.WORKSPACE / "structure.js"

DEFAULT_TARGET_FILE = (
    workspace.PROJECT_ROOT / "tasks" / "mongo_synthesis" / "structure" / "PartyContact.json"
)

# Coverage at or above this counts as "analysis complete".
COMPLETE_THRESHOLD = 100


@dataclass
class MongoAnalyzeConfig:
    target_file: Path


class MongoAnalyzeTask(FileTask):

    name = "mongo-analyze"
    label = "MongoDB Schema Analysis"

    prompts_dir = TASK_DIR / "prompts"
    decision_file = workspace.WORKSPACE / "decision.json"
    result_file = workspace.WORKSPACE / "result.json"

    def __init__(self, config: MongoAnalyzeConfig):
        self.config = config

    def setup_workspace(self) -> None:
        if not self.config.target_file.exists():
            raise FileNotFoundError(f"--target file not found: {self.config.target_file}")

        workspace.ensure_dirs(workspace.WORKSPACE, workspace.BEST_DIR)
        workspace.clean_files(
            self.result_file, self.decision_file, ANALYSIS_FILE, STRUCTURE_FILE
        )
        print(f"Workspace ready: {workspace.WORKSPACE}")
        print(f"  target: {self.config.target_file}")
        print("  analyzing live MongoDB (see .env for connection)")

    def ground_decision(self, decision: Decision, result: dict[str, Any]) -> None:
        score = result.get("score", 0)
        scoring = result.get("scoring") or {}
        decision.score = score
        decision.extra["collectionCoverage"] = scoring.get("collectionCoverage")
        decision.extra["keyQuality"] = scoring.get("keyQuality")
        decision.extra["relationshipQuality"] = scoring.get("relationshipQuality")
        decision.extra["liveCollections"] = len(result.get("liveCollections", []))

        # Hard gate: cannot be done until analysis is complete AND the
        # structure.js artifact has been produced (the chain needs it).
        if score < COMPLETE_THRESHOLD or not STRUCTURE_FILE.exists():
            decision.done = False
            if decision.stop_reason in ("optimal", "resolved"):
                decision.stop_reason = None

    def summarize(self, decision: Decision, *, baseline: bool = False) -> str:
        e = decision.extra
        prefix = "Baseline" if baseline else "Analysis"
        parts = [f"{prefix}: {decision.score:g}%"]
        if e.get("collectionCoverage") is not None:
            parts.append(f"coverage={e['collectionCoverage']}%")
        if e.get("keyQuality") is not None:
            parts.append(f"keys={e['keyQuality']}%")
        if e.get("relationshipQuality") is not None:
            parts.append(f"rels={e['relationshipQuality']}%")
        return "  ".join(parts)

    def run_config_summary(self) -> dict:
        return {"target_file": str(self.config.target_file)}

    def artifact_hint(self) -> str:
        return str(workspace.BEST_DIR / "structure.js")
