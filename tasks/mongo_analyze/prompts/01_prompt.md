# Phase 2: Schema Analysis Iteration

You are refining the schema analysis until it is complete and verified, then
emitting the `structure.js` artifact the next chain step needs.

## Step 1: Read current state

```bash
cat agent_workspace/analysis.json
cat agent_workspace/result.json
cat tasks/mongo_synthesis/structure/PartyContact.json
```

Focus on `result.json` → `scoring`:
- `keyResults` — any `verified: false` means the claimed key is NOT unique on
  the data (see `duplicateGroups`); pick a better key.
- `relResults` — any `confirmed: false` means the relationship did not hold
  (no value overlap); fix the fields or drop the link.
- `collectionCoverage < 100` — you missed collections; add them.

## Step 2: Fix the gaps

Priority order:

1. Unverified keys: consult the collection's `inferredKey` and `cardinality`
   in `introspection`. Choose fields whose combined values are actually unique.
   Prefer a `guaranteedKeys` (unique index) entry when present.
2. Unconfirmed relationships: check that `fromField` values really occur in
   `toField`. Correct the field names or remove links the data does not support.
3. Missing collections: add every live collection to `analysis.json`.

Update `agent_workspace/analysis.json` accordingly.

## Step 3: Re-score

```bash
node tasks/mongo_analyze/eval/analyze.js
```

Repeat until `scoring.score` is 100 (all collections covered, all keys
verified, all relationships confirmed).

## Step 4: Emit structure.js (required before done)

Once the analysis is complete, generate `agent_workspace/structure.js` — the
physical schema for the optimizer. Include, per collection: fields with types,
indexes (from introspection), `approximateDocCount`, the unique key, and the
confirmed relationships. Add a `notes` array with optimization hints derived
from what you found (which fields are indexed, where fan-out will occur from
1:N links, which joins have supporting indexes).

```js
module.exports = {
  ATAK112: {
    fields: { /* ... */ },
    indexes: [ /* from introspection */ ],
    approximateDocCount: 1,
    uniqueKey: ["MISPAR_KTOVET", "SUG_KTOVET"],
    relationships: [ /* confirmed links */ ]
  },
  notes: [ /* optimization hints */ ]
};
```

Then copy it to `best/` so the chain can pick it up:

```bash
cp agent_workspace/structure.js best/structure.js
cp agent_workspace/analysis.json best/analysis.json
cp agent_workspace/decision.json best/decision.json
```

## Step 5: Write decision.json

Set `done: true` only when `scoring.score` is 100 AND `structure.js` exists.
The harness forces `done` back to false otherwise. Valid `stopReason`:
`resolved` (analysis complete, structure emitted), `converged` (no recent
improvement), `exhausted` (no further ideas).

```json
{
  "improved": true,
  "done": true,
  "score": 100,
  "changes": "Verified all keys, confirmed all relationships, emitted structure.js",
  "reasoning": "All collections covered; keys and links verified against data",
  "nextSteps": "Analysis complete",
  "stopReason": "resolved"
}
```

## Step 6: Report

Summarize final coverage, verified keys (guaranteed vs inferred with
confidence), confirmed relationships with cardinality, and confirm
`structure.js` was written.
