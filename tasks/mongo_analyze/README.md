# MongoDB Schema Analysis task

Analyzes a live MongoDB (read-only) to discover the physical schema and emit
`structure.js` for the optimizer. It is the first step of the `mongo-full`
chain.

## What it discovers

- indexes and document counts per collection
- field cardinality
- unique keys, split into:
  - guaranteed — backed by a unique index (a fact)
  - inferred — empirically unique on the data, with confidence and sample size
- relationships between collections, confirmed against real data, with
  cardinality (1:1, 1:N, N:M)

## Layout

- `task.py` — `MongoAnalyzeTask`, implements the `Task` interface
- `prompts/00_prompt.md` — introspection + first analysis draft
- `prompts/01_prompt.md` — verify and complete, then emit `structure.js`
- `eval/analyze.js` — read-only evaluator: introspects the live DB and verifies
  the agent's claims (keys via a `$group` duplicate test, relationships via
  value overlap)
- `eval/package.json` — mongodb driver
- `.env` — MongoDB connection for this task

## Scoring

The score is the average of three verified components:

- `collectionCoverage` — fraction of live collections the agent analyzed
- `keyQuality` — fraction of claimed unique keys that actually verified
- `relationshipQuality` — fraction of claimed relationships confirmed on data

Every claim is checked against the live database, so the agent cannot assert a
key or link the data does not support. `done` is gated: it requires score 100
AND a produced `structure.js`.

## Agent

This task's agent lives at `.kiro/agents/schema-analyst.json`. Default agent
name is `schema-analyst`.

## Run

```bash
cd tasks/mongo_analyze/eval && npm install && cd -

python main.py mongo-analyze
```

Configure the MongoDB connection in `tasks/mongo_analyze/.env`. The analysis is
read-only — it never writes to your database.

Note: the analyzer needs a live MongoDB with the source collections loaded.
Its introspection/verification path has been unit-checked but not yet run
end-to-end against a live instance.
