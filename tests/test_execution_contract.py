import math
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from src.runner import AgentLoopRunner, LoopConfig
from src.task import Decision, Task
from src import workspace
from tasks.mongo_aggregation.task import MongoAggregationConfig, MongoAggregationTask
from tasks.mongo_synthesis.task import MongoSynthesisConfig, MongoSynthesisTask


class _FakeClient:
    def start(self):
        pass

    def prompt(self, _text):
        pass

    def close(self):
        pass


class _Task(Task):
    name = "test"
    label = "Test"

    def __init__(self, decisions):
        self._decisions = iter(decisions)

    def setup_workspace(self):
        pass

    def baseline_prompt(self):
        return "baseline"

    def iteration_prompt(self):
        return "iteration"

    def read_decision(self):
        return next(self._decisions)


class _VerifiedTask(_Task):
    def __init__(self, decisions, verification_error=None):
        super().__init__(decisions)
        self.events = []
        self.verification_error = verification_error

    def verify_candidate(self):
        self.events.append("verify")
        return self.verification_error

    def read_decision(self):
        self.events.append("read_decision")
        return super().read_decision()

    def restore_best_candidate(self):
        self.events.append("restore_best")


class DecisionContractTests(unittest.TestCase):
    def test_rejects_string_booleans_and_non_finite_scores(self):
        with self.assertRaises(ValueError):
            Decision.from_dict({"score": "NaN", "improved": "false", "done": "false"})
        with self.assertRaises(ValueError):
            Decision.from_dict({"score": math.inf, "improved": False, "done": False})

    def test_accepts_strict_json_types(self):
        decision = Decision.from_dict({"score": 7.5, "improved": True, "done": False})
        self.assertEqual(7.5, decision.score)
        self.assertTrue(decision.improved)

    def test_non_object_json_is_not_a_workspace_result(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "result.json"
            path.write_text("[]", encoding="utf-8")
            self.assertIsNone(workspace.read_json(path))


class RunnerCompletionTests(unittest.TestCase):
    def _runner(self, task):
        directory = Path(tempfile.mkdtemp())
        runner = AgentLoopRunner(
            task,
            LoopConfig(
                iterations=1,
                agent="test-agent",
                model="test-model",
                telemetry_dir=directory,
            ),
        )
        runner.client = _FakeClient()
        return runner

    def test_max_iterations_without_done_is_failure(self):
        runner = self._runner(_Task([
            Decision(score=10, improved=False, done=False),
            Decision(score=10, improved=False, done=False),
        ]))
        self.assertEqual(2, runner.run())

    def test_done_decision_is_success(self):
        runner = self._runner(_Task([
            Decision(score=100, improved=True, done=True, stop_reason="resolved"),
        ]))
        self.assertEqual(0, runner.run())

    def test_verifier_confirmed_perfect_score_is_success_without_agent_done(self):
        runner = self._runner(_Task([
            Decision(score=0, improved=False, done=False),
            Decision(score=100, improved=False, done=False),
        ]))
        self.assertEqual(0, runner.run())
        self.assertTrue(runner.completed)
        self.assertEqual("optimal", runner.stop_reason)

    def test_verifier_runs_before_the_decision_is_read(self):
        task = _VerifiedTask([
            Decision(score=100, improved=True, done=True, stop_reason="resolved"),
        ])
        self.assertEqual(0, self._runner(task).run())
        self.assertEqual(["verify", "read_decision"], task.events)

    def test_verification_failure_fails_closed(self):
        task = _VerifiedTask([], verification_error="stale result")
        self.assertEqual(1, self._runner(task).run())
        self.assertEqual(["verify"], task.events)

    def test_rejected_candidate_is_restored_from_best(self):
        task = _VerifiedTask([
            Decision(score=10, improved=False, done=False),
            Decision(score=10, improved=True, done=False),
        ])
        self.assertEqual(2, self._runner(task).run())
        self.assertIn("restore_best", task.events)


class AggregationGroundingTests(unittest.TestCase):
    def setUp(self):
        self.task = MongoAggregationTask(
            MongoAggregationConfig(Path("pipeline.js"), Path("structure.js"))
        )

    def test_incomplete_measurement_cannot_complete_task(self):
        decision = Decision(score=100, improved=True, done=True, stop_reason="optimal")
        self.task.ground_decision(decision, {
            "score": 100,
            "timing": {"avgMs": 1.0},
            "plan": {"docsExamined": None, "docsReturned": None},
            "hasCollscan": False,
            "hasFanout": False,
        })
        self.assertEqual(0, decision.score)
        self.assertFalse(decision.done)
        self.assertFalse(decision.extra["measurementComplete"])

    def test_complete_measurement_overrides_agent_score(self):
        decision = Decision(score=1, improved=False, done=True, stop_reason="optimal")
        self.task.ground_decision(decision, {
            "score": 88,
            "timing": {"avgMs": 4.5},
            "plan": {"docsExamined": 100, "docsReturned": 10},
            "hasCollscan": False,
            "hasFanout": False,
            "cardinalityRatio": 10,
        })
        self.assertEqual(88, decision.score)
        self.assertTrue(decision.done)
        self.assertTrue(decision.extra["measurementComplete"])


class SynthesisEvaluatorRegressionTests(unittest.TestCase):
    def test_perfect_evaluator_result_completes_advisory_decision(self):
        task = MongoSynthesisTask(
            MongoSynthesisConfig(Path("data"), Path("target.json"))
        )
        decision = Decision(score=0, improved=False, done=False)

        task.ground_decision(decision, {
            "score": 100,
            "correctness": 100,
            "phase": "optimization",
            "missingRequired": [],
        })

        self.assertEqual(100, decision.score)
        self.assertTrue(decision.done)
        self.assertEqual("optimal", decision.stop_reason)

    def test_structural_only_pipeline_fails_the_semantic_gate(self):
        """Typed containers alone cannot satisfy source-derived mappings."""
        root = Path(__file__).resolve().parent.parent
        evaluator = root / "tasks" / "mongo_synthesis" / "eval" / "evaluate.js"
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "structural_only.js"
            candidate.write_text(
                """const item = (kind) => ({ $map: { input: { $filter: {
  input: \"$contacts\", cond: { $eq: [\"$$this.SUG_MIVNE_KTOVET\", kind] }
} }, as: \"c\", in: { addressSerialId: \"$$c.MISPAR_KTOVET\", contactAttributes: {
  partyAddressUsage: [{ code: \"$$c.SUG_KTOVET\" }],
  partyAddressUsages: [{ partyAddressUsageCode: \"$$c.SUG_KTOVET\", isInvalidAddress: { $ne: [\"$$c.KOD_KTOVET_MSB\", 0] } }]
} } } });
module.exports = [
  { $group: { _id: \"$SIFRUR_LAKOACH\", contacts: { $push: \"$$ROOT\" } } },
  { $project: { _id: 1, contact: {
    localAddress: item(12), abroadAddress: item(16), pOB: item(20),
    pOBInBranch: item(40), swiftAddress: item(50), localPhone: item(60),
    abroadPhone: item(66), email: item(90)
  } } }
];
""",
                encoding="utf-8",
            )
            environment = os.environ | {"PROJECT_ROOT": directory}
            completed = subprocess.run(
                ["node", str(evaluator), str(candidate)],
                cwd=root,
                env=environment,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            self.assertEqual(0, completed.returncode, completed.stderr)
            result = json.loads(
                (Path(directory) / "agent_workspace" / "result.json").read_text(
                    encoding="utf-8"
                )
            )

        self.assertEqual(25, result["correctness"])
        self.assertEqual(100, result["structuralCorrectness"])
        self.assertEqual(25, result["semanticCorrectness"])
        self.assertEqual(21, result["satisfiedPaths"])
        self.assertEqual(21, result["requiredPaths"])
        self.assertEqual(6, len(result["semanticFailures"]))
        self.assertIsNone(result["runError"])


if __name__ == "__main__":
    unittest.main()
