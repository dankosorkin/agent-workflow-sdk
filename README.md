# Autonomous Agent Optimization Orchestrator

A task-agnostic harness that runs a Kiro AI agent in an
iterate-until-converged loop. The orchestrator drives the loop, records
telemetry, and stops early on convergence. Each concrete job is a
plugin under `tasks/`.

Three tasks ship today, and they compose into one chain:

- `mongo-analyze` — introspects a live MongoDB (read-only) to discover
  indexes, cardinality, unique keys (guaranteed vs inferred), and
  relationships, emitting a `structure.js` physical schema.
- `mongo-synthesis` — builds an aggregation from scratch that maps several
  source collections into a target structure, scored on structural
  correctness (in-memory via mingo, no live MongoDB).
- `mongo-aggregation` — optimizes an aggregation pipeline using real
  `explain()` metrics (execution time, COLLSCAN, fan-out) against a live DB.

The `mongo-full` chain runs them in order: analyze → synthesis → aggregation,
passing artifacts between steps.

## Architecture

The core is split into a reusable orchestrator and pluggable tasks.

```text
main.py                    generic entrypoint (single task or chain)
src/                       task-agnostic core
  acp.py                   Kiro ACP client (JSON-RPC over subprocess, v2/v3)
  telemetry.py             JSONL telemetry logging
  task.py                  Task interface + Decision contract
  runner.py                AgentLoopRunner (baseline + loop + stop rules)
  registry.py              task name to plugin mapping
  cli.py                   argument parsing (subcommand per task, plus chain)
  chain.py                 CHAIN + Step: linear task chains (not a graph)
tasks/                     task plugins
  mongo_analyze/           analyze a live DB → structure.js
  mongo_synthesis/         synthesize a pipeline from sources to a target
  mongo_aggregation/       optimize an existing pipeline
```

The orchestrator only understands the Decision contract. Everything
task-specific lives behind the `Task` interface.

## The Decision contract

Every task's agent writes a JSON decision at the end of each turn. It is a
strict JSON contract: `score` must be a finite JSON number, `improved` and
`done` must be JSON booleans, and `stopReason` must be a string or `null`.
Malformed decisions are rejected rather than coerced. The orchestrator reads
these fields to drive stop conditions:

| Field | Meaning |
|-------|---------|
| `score` | Quality score, higher is better |
| `improved` | Whether this turn beat the previous best |
| `done` | Whether the agent considers the job finished |
| `stopReason` | Why it stopped (`optimal`, `converged`, `resolved`, `exhausted`) |

Any other fields (timing, penalties, metrics) are carried through to
telemetry untouched.

## Prerequisites

- Python 3.11+ (no external Python dependencies — stdlib only)
- `kiro-cli` installed and authenticated
- Node.js 18+ for task evaluators
- A live MongoDB for `mongo-analyze` and `mongo-aggregation`
  (`mongo-synthesis` runs in-memory and needs none)

## Usage

Single task:

```bash
python main.py <task> [task args] [loop args]
```

Run the aggregation task with the bundled example:

```bash
cd tasks/mongo_aggregation/eval && npm install && cd -

python main.py mongo-aggregation \
  --aggregation tasks/mongo_aggregation/example/aggregation.js \
  --structure tasks/mongo_aggregation/example/structure.js
```

Run the full chain (analyze → synthesis → aggregation):

```bash
python main.py chain mongo-full
```

Each step selects its own agent; the chain passes artifacts between steps
via `best/` (structure.js, aggregation.js) and halts if a step fails to
produce its declared artifact.

### Seeding artifacts (skipping steps)

If you already have an artifact a step would produce, supply it with `--seed
ARTIFACT=PATH`. The chain places it into `best/` before running and skips the
step that produces it. For example, to optimize an existing baseline
aggregation without re-synthesizing it:

```bash
python main.py chain mongo-full --seed aggregation.js=path/to/baseline.js
```

This runs analyze → (synthesis skipped) → aggregation. `--seed` is repeatable
and validates that each artifact name is one the chain produces. If you only
need to optimize a pipeline and already have `structure.js`, run the
`mongo-aggregation` task directly instead of a chain.

Shared loop arguments:

```text
--iterations, -n N       Max optimization iterations per task (default: 10)
--agent           NAME   Kiro agent (single-task only; chain uses per-step agents)
--model           MODEL  LLM model (default: claude-opus-4.5)
--engine          {v2,v3} Kiro ACP agent engine (default: v3)
--telemetry-dir   DIR    Telemetry output directory (default: ./telemetry)
```

## Early stopping

The loop stops before `--iterations` when any of these hold:

| Condition | Stop reason |
|-----------|-------------|
| Agent sets `done: true` | agent's `stopReason` |
| Best score reaches the perfect score | `optimal` |
| No improvement for 4 consecutive iterations | `converged` |

For `mongo-synthesis`, a score of 100 requires both structural paths and
source-derived semantics: every ATAKL10 link must be materialized under its
customer/type, and available detail values from the matching ATAK collection
must be propagated. Empty typed arrays cannot pass the gate.

Stopping is not itself success. A run exits successfully only after a valid
decision sets `done: true`; reaching the iteration limit or an automatic stop
without that decision exits non-zero and is recorded as `INCOMPLETE`.

For `mongo-aggregation`, `tasks/mongo_aggregation/eval/measure.js` is the
authoritative source of score and metrics. A result without finite timing and
execution counters is incomplete and cannot mark the task done.

For synthesis and aggregation, the harness runs the task verifier after every
agent turn. The verifier records the SHA-256 of the candidate it measured;
the harness rejects a result whose hash does not match the current candidate.
Only the harness promotes an independently verified candidate into `best/`.
It snapshots both the candidate and its verifier result; after a rejection it
restores that matching pair before the next agent turn.

## Telemetry

Each task run writes `telemetry/<run_id>.jsonl`, one JSON event per line
(`run_start`, `iteration`, `run_end`, warnings, errors). A chain run also
writes `telemetry/chain_<timestamp>.jsonl` recording the chain's own
decisions (`chain_start`, `step_start`, `step_end`, `chain_halted` with a
reason, `chain_complete`). To list iteration scores for a run:

```bash
grep '"event":"iteration"' telemetry/<run_id>.jsonl
```

## Adding a new task

A new autonomous agent is a new folder under `tasks/` plus one registry
entry. No change to the orchestrator core is needed.

Step 1. Create `tasks/my_task/task.py` with a `Task` subclass:

```python
from src.task import Decision, Task
from src import workspace

class MyTask(Task):
    name = "my-task"
    label = "My Optimization"

    def __init__(self, config):
        self.config = config

    def setup_workspace(self):
        ...                       # copy inputs, clean stale artifacts

    def baseline_prompt(self) -> str:
        ...                       # return the phase 1 prompt text

    def iteration_prompt(self) -> str:
        ...                       # return the iteration prompt text

    def read_decision(self):
        raw = workspace.read_json(MY_DECISION_FILE)
        return Decision.from_dict(raw) if raw else None
```

Step 2. Add prompt files under `tasks/my_task/prompts/` and any eval
tooling the task needs.

Step 3. Register the task in `src/registry.py`:

```python
def _my_add_arguments(parser):
    parser.add_argument("--input", "-i", required=True, type=Path)

def _my_build(args):
    from tasks.my_task.task import MyTask, MyConfig
    return MyTask(MyConfig(input=args.input))

PLUGINS["my-task"] = TaskPlugin(
    name="my-task",
    help="Describe the task",
    add_arguments=_my_add_arguments,
    build=_my_build,
)
```

Step 4. Create the agent at `.kiro/agents/<name>.json`. Use JSON (the CLI
loads JSON agents; a `permissions` block currently breaks loading, so omit
it — the orchestrator auto-approves tool prompts). Kiro discovers agents only
under `.kiro/agents/` (workspace, must be trusted) or `~/.kiro/agents/`
(global), so the agent file lives there, not inside the task folder. Validate
with `kiro-cli agent validate --path .kiro/agents/<name>.json`.

Run it:

```bash
python main.py my-task --input data.txt
```

The agent must write a decision file each turn that maps onto the
Decision contract (at minimum `score`, `improved`, `done`).

## Adding a chain

Chains are declared in `src/chain.py` as a list of `Step`s — a linear
sequence, not a graph (no branching, loop-back, or parallelism). Steps hand
work to each other through artifacts under `best/`:

```python
CHAINS["my-chain"] = [
    Step(task="step-one", produces=["out.js"]),
    Step(task="step-two", consumes=["out.js"], args={"input": BEST_DIR / "out.js"}),
]
```

Run with `python main.py chain my-chain`. A step that does not reach a
successful terminal state, or fails to produce its declared artifact, halts
the chain. A step whose entire `produces` set is already present (supplied via
`--seed`) is skipped.

## Requirements the agent must satisfy

For the loop to make progress and stop correctly, the task's agent must:

- Write a decision file at the end of every turn
- Report a numeric `score` (higher is better)
- Set `improved` truthfully relative to the previous best
- Set `done: true` with a `stopReason` when it converges or exhausts options

## Project structure

```text
.
├── main.py                     Generic entrypoint (single task or chain)
├── src/                        Task-agnostic core
│   ├── acp.py
│   ├── telemetry.py
│   ├── task.py
│   ├── runner.py
│   ├── registry.py
│   ├── cli.py
│   └── chain.py
├── tasks/
│   ├── mongo_analyze/          Live-DB schema analysis → structure.js
│   ├── mongo_synthesis/        Synthesize aggregation (in-memory)
│   └── mongo_aggregation/      Optimize aggregation (live DB)
│       ├── task.py
│       ├── README.md
│       ├── .env
│       ├── prompts/
│       ├── eval/
│       └── example/
├── .kiro/
│   └── agents/                 schema-analyst.json, synthesis-expert.json,
│                               aggregation-expert.json
├── agent_workspace/            Agent working directory (ephemeral)
├── best/                       Artifacts passed between chain steps
└── telemetry/                  Per-run + per-chain JSONL logs
```
