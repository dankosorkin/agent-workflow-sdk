# Phase 1: Schema Analysis Baseline

You are analyzing a live MongoDB database to produce a physical schema
description (`structure.js`) that a later optimization step will use. You
work read-only — never write to the database.

## Step 1: Introspect the live database

Run the analyzer with no analysis yet — it reports raw facts about every
collection (indexes, doc counts, field cardinality, guaranteed and inferred
unique keys):

```bash
node tasks/mongo_analyze/eval/analyze.js
```

Read `agent_workspace/result.json` → `introspection`. For each collection you
get: `docCount`, `indexes`, `cardinality` (distinct per field), `guaranteedKeys`
(from unique indexes — these are facts), and `inferredKey` (empirically unique
on the data, with a confidence and note).

## Step 2: Read the target to get relationship hypotheses

```bash
cat tasks/mongo_synthesis/structure/PartyContact.json
```

The mapping hints (e.g. `- ATAK112.SHEM_YISHUV_TIKNI`) tell you which source
collections feed the target document, and shared fields (like `MISPAR_KTOVET`)
hint at how collections relate. Use these as HYPOTHESES to confirm — do not
trust them blindly.

## Step 3: Draft the analysis

Write `agent_workspace/analysis.json` capturing what you found:

```json
{
  "collections": [
    {
      "name": "ATAK112",
      "docCount": 1,
      "uniqueKey": { "fields": ["MISPAR_KTOVET", "SUG_KTOVET"], "source": "inferred" }
    }
  ],
  "relationships": [
    { "from": "ATAKL10", "fromField": "MISPAR_KTOVET",
      "to": "ATAK112", "toField": "MISPAR_KTOVET", "cardinality": "1:N" }
  ]
}
```

For each collection state its unique key — prefer a `guaranteedKeys` entry
(unique index) when one exists; otherwise use the verified `inferredKey`, and
mark `"source": "inferred"`. For relationships, list the links you believe
connect the collections into the target document.

## Step 4: Score the analysis

Run the analyzer again — now it will find your `analysis.json` and VERIFY every
claim against the live data:

```bash
node tasks/mongo_analyze/eval/analyze.js
```

Read `agent_workspace/result.json` → `scoring`:
- `collectionCoverage` — did you analyze all live collections?
- `keyQuality` — how many claimed unique keys actually verified ($group test)?
- `relationshipQuality` — how many claimed relationships confirmed on data?
- `keyResults` / `relResults` — per-claim detail showing what failed

## Step 5: If the analysis is already complete, emit structure.js

If `scoring.score` is already 100 (all collections covered, all keys verified,
all relationships confirmed), you must emit the artifact NOW — do not defer it
to a later iteration. Generate `agent_workspace/structure.js` (fields with
types, indexes from introspection, `approximateDocCount`, the unique key, and
confirmed relationships, plus a `notes` array of optimization hints), then:

```bash
cp agent_workspace/structure.js best/structure.js
cp agent_workspace/analysis.json best/analysis.json
cp agent_workspace/decision.json best/decision.json
```

Only set `done: true` once `structure.js` exists — the harness forces `done`
back to false otherwise, and a chain will halt without it.

## Step 6: Write decision.json

```json
{
  "improved": false,
  "done": false,
  "score": 0,
  "changes": "Baseline introspection and first analysis draft",
  "reasoning": "Covered N collections; keys/relationships need verification",
  "nextSteps": "Fix unverified keys and confirm remaining relationships",
  "stopReason": null
}
```

The evaluator determines the real `score`. Your decision records intent.

## Step 7: Report

Summarize: how many collections, which keys are guaranteed vs inferred, which
relationships confirmed, and what still needs work.
