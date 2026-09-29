# Phase 2: Synthesis Iteration

You are improving the aggregation you are synthesizing. Progress has two
phases, and correctness is a hard gate:

- synthesis phase (correctness < 100): close missing/mistyped target paths
- optimization phase (correctness == 100): now reduce complexity and cost

## Step 1: Read current state

```bash
cat agent_workspace/decision.json
cat agent_workspace/result.json
cat agent_workspace/aggregation.js
cat tasks/mongo_synthesis/structure/PartyContact.json
```

Focus on `result.json`:
- `correctness` and `phase`
- `missingRequired` — the exact paths to fix next
- `runError` — if set, the pipeline is broken; fixing it is priority zero

## Step 2: If done, stop

If `result.json` shows `correctness: 100` AND you have no further structural or
performance improvement to make, you are finished. Write a decision with
`done: true` and an appropriate `stopReason`, then stop.

## Step 3: Pick the work for this iteration

Priority zero — pipeline error: if `runError` is set, fix the pipeline so it
runs at all.

Synthesis phase (correctness < 100): pick the most impactful missing paths
from `missingRequired` and extend the pipeline to produce them. Typical moves:
- add a `$lookup` for a source collection you have not joined yet
- map an unmapped source field into its target field via `$project` / `$set`
- fix a type mismatch (e.g. wrap/convert a value to the expected type)
- adjust `$unwind` so nested arrays land at the right target path
- filter by `SUG_MIVNE_KTOVET` to route rows into the correct address type

Make focused changes — a few related paths per iteration — so the correctness
delta is easy to attribute.

Optimization phase (correctness == 100): keep correctness at 100 while
simplifying. Remove redundant stages, push `$match` earlier, drop unused
fields with an early `$project`. Never trade away correctness for speed — if a
change drops correctness below 100, revert it.

## Step 4: Save the candidate

Write the updated pipeline to `agent_workspace/aggregation.js`. Do not run the
evaluator and do not update `best/`: after the turn the harness evaluates this
exact file and promotes it atomically only when its measured quality improves.

## Step 5: Write the next decision

## Step 6: Write decision.json

```json
{
  "improved": true,
  "done": false,
  "score": 0,
  "changes": "Added abroadAddress from ATAK116 and email from ATAK190",
  "reasoning": "missingRequired listed contact.abroadAddress[] and contact.email[]",
  "nextSteps": "Map phone types via SUG_MIVNE_KTOVET routing",
  "stopReason": null
}
```

The verifier requires both structure and semantics: every ATAKL10 link must
appear under the matching customer and contact type, and, when a matching
ATAK112/116/... detail row exists, its mapped business value must be present.
Empty typed arrays do not count as a solution.

Set `done: true` only when correctness is 100 and nothing worthwhile remains.
Valid `stopReason` values:
- `resolved` — correctness 100 and structure fully matched (synthesis complete)
- `optimal` — correctness 100 and pipeline is also well optimized
- `converged` — no improvement across recent iterations
- `exhausted` — no further ideas

The harness overrides `done` to false if correctness is below 100, so a
premature `done` will simply be ignored — always keep pushing correctness first.

## Step 7: Report

Print correctness, phase, what you changed, and the next target paths.
