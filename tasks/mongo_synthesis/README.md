# MongoDB Aggregation Synthesis task

Synthesizes an aggregation pipeline from scratch that transforms several
source collections into a required target structure. Unlike
`mongo-aggregation` (which tunes an existing pipeline), this task builds the
pipeline and is scored on structural correctness first.

## Layout

- `task.py` — `MongoSynthesisTask`, implements the `Task` interface
- `prompts/00_prompt.md` — synthesis baseline prompt
- `prompts/01_prompt.md` — synthesis iteration prompt
- `eval/evaluate.js` — in-memory evaluator (mingo) + structural diff
- `eval/package.json` — mingo dependency
- `data/` — source collections, one JSON file per collection
- `structure/PartyContact.json` — the target structure

## Scoring

The evaluator runs the candidate pipeline in-memory and compares its output to
the target structure, path by path (presence and type). Two phases:

- synthesis phase: correctness below 100, score equals correctness percentage
- optimization phase: correctness at 100, structure fully matched

Correctness is a hard gate. The task overrides the agent's `done` back to
false whenever correctness is below 100, so the loop cannot stop on an
incomplete structure.

## Agent

This task's agent lives at `.kiro/agents/synthesis-expert.json`. Kiro discovers
agents only under `.kiro/agents/` or `~/.kiro/agents/`, so it cannot live in
this task folder. The default agent name is `synthesis-expert`.

## Run

```bash
cd tasks/mongo_synthesis/eval && npm install && cd -

python main.py mongo-synthesis
# uses tasks/mongo_synthesis/data and structure/PartyContact.json by default

# or with explicit paths:
python main.py mongo-synthesis \
  --sources tasks/mongo_synthesis/data \
  --target tasks/mongo_synthesis/structure/PartyContact.json
```

No live MongoDB is needed — the evaluator runs pipelines in-memory via mingo.

## Pipeline: synthesis then optimization

The two MongoDB tasks chain into one workflow:

Step 1 — synthesize a correct pipeline (in-memory, no server):

```bash
python main.py mongo-synthesis
# best/aggregation.js now holds a structurally correct pipeline
```

Step 2 — tune that pipeline for real-world performance (needs MongoDB):

```bash
python main.py mongo-aggregation \
  --aggregation best/aggregation.js \
  --structure tasks/mongo_synthesis/structure/PartyContact.json
```

`mongo-synthesis` guarantees the output shape is correct; `mongo-aggregation`
then reduces COLLSCANs, fan-out, and execution time against a live database.
Both tasks share the same orchestrator core — only the task plugin differs.
