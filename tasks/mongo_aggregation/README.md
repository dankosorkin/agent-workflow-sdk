# MongoDB Aggregation task

Optimizes a MongoDB aggregation pipeline against measurable metrics:
execution time, COLLSCAN presence, fan-out, and cardinality.

## Layout

- `task.py` — `MongoAggregationTask`, implements the `Task` interface
- `prompts/00_prompt.md` — baseline turn prompt
- `prompts/01_prompt.md` — optimization iteration prompt
- `eval/measure.js` — Node.js eval runner (runs `explain()`, scores the plan)
- `eval/package.json` — mongodb driver dependency
- `example/aggregation.js` — sample pipeline (intentionally suboptimal)
- `example/structure.js` — sample schema and indexes
- `.env` — MongoDB connection config for this task

## Agent

This task's agent lives at `.kiro/agents/aggregation-expert.json`, not here.

Kiro only discovers agents under `.kiro/agents/` (workspace) or
`~/.kiro/agents/` (global), so the agent file cannot be moved into this
task folder. The default agent name is `aggregation-expert`; override
with `--agent` on the command line.

## Run

```bash
cd tasks/mongo_aggregation/eval && npm install && cd -
python main.py mongo-aggregation \
  --aggregation tasks/mongo_aggregation/example/aggregation.js \
  --structure tasks/mongo_aggregation/example/structure.js
```

Configure the MongoDB connection in `tasks/mongo_aggregation/.env`.
