"""
Linear task chain.

A CHAIN is an ordered list of Steps executed one after another. It is NOT a
graph engine: there is no branching, no loop-back, no parallelism, no shared
in-memory state. Steps are executed by a plain `for` loop, and they hand work
to each other through ARTIFACTS on disk (best/structure.js, best/aggregation.js)
— the same way `make` targets depend on files.

If a step does not reach a successful terminal state (its Decision is not
`done`, or its produced artifact is missing), the chain stops there and does
not run later steps.

When the flow ever needs branching, loop-back, or parallel steps, this list
stops being enough and a real graph engine becomes warranted. Until then, a
straight chain is the honest, minimal model.
"""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import shutil

from .registry import get_plugin
from .runner import AgentLoopRunner, LoopConfig
from .telemetry import Telemetry


ROOT = Path(__file__).resolve().parent.parent
BEST_DIR = ROOT / "best"


@dataclass
class Step:
    """One task in a chain, with fixed CLI-style args and its artifacts."""
    task: str
    #: Task-specific args applied to the parsed namespace (e.g. {"target": Path(...)}).
    args: dict = field(default_factory=dict)
    #: Artifact filenames (under best/) this step must produce to be considered done.
    produces: list[str] = field(default_factory=list)
    #: Artifact filenames (under best/) required to exist before this step runs.
    consumes: list[str] = field(default_factory=list)
    #: Override the default agent for this step.
    agent: str | None = None


# ---------------------------------------------------------------------------
# Chain definitions — declarative, in-code (symmetric to PLUGINS).
# Kept intentionally simple: a straight list per chain name.
# ---------------------------------------------------------------------------

CHAINS: dict[str, list[Step]] = {
    "mongo-full": [
        # 1. Analyze the live database → structure.js
        Step(
            task="mongo-analyze",
            produces=["structure.js"],
        ),
        # 2. Synthesize an aggregation that maps sources → target
        Step(
            task="mongo-synthesis",
            produces=["aggregation.js"],
        ),
        # 3. Optimize that aggregation against the live database,
        #    using the discovered structure.js
        Step(
            task="mongo-aggregation",
            consumes=["structure.js", "aggregation.js"],
            args={"aggregation": BEST_DIR / "aggregation.js",
                  "structure": BEST_DIR / "structure.js"},
        ),
    ],
}


def get_chain(name: str) -> list[Step]:
    if name not in CHAINS:
        raise KeyError(name)
    return CHAINS[name]


# ---------------------------------------------------------------------------
# Chain runner
# ---------------------------------------------------------------------------

class ChainRunner:
    """Executes a chain of Steps in order, stopping on the first failed step."""

    def __init__(self, name: str, steps: list[Step], loop_kwargs: dict,
                 seed: dict[str, Path] | None = None):
        self.name = name
        self.steps = steps
        # Shared loop settings (iterations, model, engine, telemetry_dir) that
        # every step's AgentLoopRunner receives; agent is per-step.
        self.loop_kwargs = loop_kwargs
        # Externally supplied artifacts: {artifact_filename: source_path}. Placed
        # into best/ before the chain runs; steps whose entire produces set is
        # already present are skipped (e.g. seed a baseline aggregation.js to
        # skip the synthesis step).
        self.seed = seed or {}
        # Chain-level telemetry: a separate JSONL recording the chain's own
        # decisions (step boundaries, halts) independent of each step's log.
        run_id = "chain_" + datetime.now().strftime("%Y%m%d_%H%M%S")
        self.tel = Telemetry(run_id, loop_kwargs["telemetry_dir"])

    def _chain_artifacts(self) -> set[str]:
        """All artifact filenames any step in this chain produces."""
        arts: set[str] = set()
        for s in self.steps:
            arts.update(s.produces)
        return arts

    def _prepare_best(self) -> int:
        """
        Clean this chain's artifacts from best/, then lay down any seeds.
        Returns 0 on success, non-zero on a seed validation error.
        """
        BEST_DIR.mkdir(parents=True, exist_ok=True)
        chain_artifacts = self._chain_artifacts()

        # Validate seeds: name must be a produced artifact of this chain, and
        # the source file must exist.
        for name, src in self.seed.items():
            if name not in chain_artifacts:
                self.tel.log(
                    f"ERROR: --seed '{name}' is not produced by any step of "
                    f"chain '{self.name}'. Producible artifacts: {sorted(chain_artifacts)}"
                )
                self.tel.record("error", {"reason": "invalid_seed_name", "seed": name})
                return 1
            if not Path(src).exists():
                self.tel.log(f"ERROR: --seed source file not found: {src}")
                self.tel.record("error", {"reason": "seed_file_missing", "path": str(src)})
                return 1

        # Clean stale chain artifacts so "artifact present" unambiguously means
        # "produced this run or seeded now" — never leftover from a past run.
        for art in chain_artifacts:
            p = BEST_DIR / art
            if p.exists():
                p.unlink()

        # Lay down seeds.
        for name, src in self.seed.items():
            shutil.copy2(src, BEST_DIR / name)
            self.tel.log(f"[seed] {name} <- {src}")
            self.tel.record("seed_applied", {"artifact": name, "source": str(src)})

        return 0

    def run(self) -> int:
        self.tel.log(f"\n{'#'*60}")
        self.tel.log(f"# CHAIN: {self.name}  ({len(self.steps)} steps)")
        self.tel.log(f"{'#'*60}")
        self.tel.record("chain_start", {
            "chain": self.name,
            "steps": [s.task for s in self.steps],
            "seeds": sorted(self.seed.keys()),
        })

        rc = self._prepare_best()
        if rc != 0:
            return rc

        ran = 0
        skipped = 0

        for i, step in enumerate(self.steps, start=1):
            # Auto-skip: if this step's entire produces set already exists in
            # best/ (because it was seeded), there is nothing for it to do.
            if step.produces and all((BEST_DIR / p).exists() for p in step.produces):
                self.tel.log(f"\n# Step {i}/{len(self.steps)}: {step.task} — SKIPPED (artifacts seeded)")
                self.tel.record("step_skipped", {
                    "index": i, "task": step.task,
                    "reason": "seeded", "produces": step.produces,
                })
                skipped += 1
                continue

            self.tel.log(f"\n{'#'*60}")
            self.tel.log(f"# Step {i}/{len(self.steps)}: {step.task}")
            self.tel.log(f"{'#'*60}")
            self.tel.record("step_start", {
                "index": i, "task": step.task,
                "consumes": step.consumes, "produces": step.produces,
            })

            # Verify required input artifacts exist before running.
            missing = [c for c in step.consumes if not (BEST_DIR / c).exists()]
            if missing:
                self.tel.log(
                    f"CHAIN halted: step '{step.task}' requires artifacts not "
                    f"found in best/: {missing}. Provide them via a prior step or --seed."
                )
                self.tel.record("chain_halted", {
                    "index": i, "task": step.task,
                    "reason": "missing_input_artifacts", "missing": missing,
                })
                return 1

            rc = self._run_step(step)
            if rc != 0:
                self.tel.log(f"\nCHAIN halted: step '{step.task}' failed (exit {rc}).")
                self.tel.record("chain_halted", {
                    "index": i, "task": step.task,
                    "reason": "step_failed", "exit_code": rc,
                })
                return rc

            # Verify this step produced its declared artifacts.
            not_produced = [p for p in step.produces if not (BEST_DIR / p).exists()]
            if not_produced:
                self.tel.log(
                    f"\nCHAIN halted: step '{step.task}' did not produce "
                    f"required artifacts: {not_produced}."
                )
                self.tel.record("chain_halted", {
                    "index": i, "task": step.task,
                    "reason": "missing_output_artifacts", "notProduced": not_produced,
                })
                return 1

            self.tel.record("step_end", {"index": i, "task": step.task, "ok": True})
            ran += 1

        if ran == 0:
            self.tel.log(
                "\nWARNING: every step was skipped (all outputs seeded). "
                "Nothing to do."
            )
            self.tel.record("chain_noop", {"skipped": skipped})

        self.tel.log(f"\n{'#'*60}")
        self.tel.log(f"# CHAIN COMPLETE: {self.name}  (ran {ran}, skipped {skipped})")
        self.tel.log(f"{'#'*60}\n")
        self.tel.record("chain_complete", {
            "chain": self.name, "steps_run": ran, "steps_skipped": skipped,
        })
        return 0

    def _run_step(self, step: Step) -> int:
        plugin = get_plugin(step.task)

        # Build a minimal namespace-like object from the step's fixed args.
        # Task plugins read attributes off it in their build() function.
        ns = _StepNamespace(step.args)

        task = plugin.build(ns)

        config = LoopConfig(
            agent=step.agent or plugin.default_agent,
            **self.loop_kwargs,
        )
        return AgentLoopRunner(task, config).run()


class _StepNamespace:
    """
    Adapter that lets a chain Step supply the same attributes a task plugin's
    build() reads from an argparse.Namespace. Any attribute not set on the step
    resolves to None, so plugins fall back to their defaults.
    """

    def __init__(self, args: dict):
        self._args = args

    def __getattr__(self, name: str):
        return self._args.get(name)
