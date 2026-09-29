# Phase 1: Synthesis Baseline

You are building a MongoDB aggregation pipeline from scratch that transforms
several SOURCE collections into a single TARGET structure. If
`agent_workspace/aggregation.js` already exists, it is the last verified best
candidate: inspect and improve it rather than starting over. The evaluator
normalizes Mongo Extended JSON wrappers into native runtime scalars.

## Step 1: Understand the target structure

Read the target schema. Its leaf values describe the expected type and the
source mapping, e.g. `"city": "string | omitted - ATAK112.SHEM_YISHUV_TIKNI"`
means the `city` field is a string sourced from `ATAK112.SHEM_YISHUV_TIKNI`,
and `omitted` marks it as optional.

```bash
cat tasks/mongo_synthesis/structure/PartyContact.json
```

Note:
- Keys starting with `_` (like `_note`, `_ref`) are documentation, not fields.
- Arrays (like `localAddress`, `pOB`, `email`) expect an array of objects.
- `"same structure as ..."` refs mean the item shares another object's shape.

## Step 2: Understand the source collections

List and inspect the sources. Each JSON file is one collection.

```bash
ls tasks/mongo_synthesis/data/
cat tasks/mongo_synthesis/data/ATAK112.json
cat tasks/mongo_synthesis/data/ATAKL10.json
```

Map source fields to target fields using the schema's mapping hints. The
`SUG_MIVNE_KTOVET` code often selects which address type a row belongs to
(12 = local address, 16 = abroad, 20 = P.O.B, 40 = branch P.O.B, 50 = swift,
60 = local phone, 66 = abroad phone, 90 = email).

## Step 3: Build the first aggregation

Write your pipeline to `agent_workspace/aggregation.js` as a module export:

```js
module.exports = [
  { $match: { ... } },
  { $lookup: { from: "ATAKL10", ... } },
  // ... build toward the target shape
];
```

Aim to produce documents shaped like the target. Do not worry about
performance yet — correctness first. Use `$lookup` to join related
collections, `$unwind` where the target expects merged single objects,
`$group` / `$project` to assemble the final shape.

## Step 4: Let the harness measure correctness

Do not run the evaluator and do not copy files into `best/`. After this turn,
the harness runs the evaluator itself and records a result tied to the exact
candidate file you wrote. On the next turn, read `agent_workspace/result.json`:
- `correctness` — percentage of required target paths matched (presence + type)
- `phase` — `synthesis` until correctness hits 100, then `optimization`
- `missingRequired` — the exact paths still missing or mistyped
- `diff` — full per-path breakdown
- `outputDocCount` — how many documents your pipeline produced
- `runError` — set if the pipeline threw

## Step 5: Write decision.json

Write `agent_workspace/decision.json`:

```json
{
  "improved": false,
  "done": false,
  "score": 0,
  "changes": "Initial synthesis: joined ATAK112/ATAKL10, built localAddress",
  "reasoning": "First pass. Covered local address; abroad/POB/phone/email still missing.",
  "nextSteps": "Add abroadAddress from ATAK116 and email from ATAK190",
  "stopReason": null
}
```

Note: the evaluator determines the real `correctness` and `score`. Your
`decision.json` records your intent and plan; the harness reads the objective
result from `result.json`.

## Step 6: Leave promotion to the harness

The harness promotes the baseline atomically after its verifier accepts it.

## Step 7: Report

Print a short summary: current correctness, which target sections you covered,
which are still missing, and your plan for the next iteration.
