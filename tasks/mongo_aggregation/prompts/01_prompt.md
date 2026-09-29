# Phase 2: Optimization Iteration

You are continuing an aggregation optimization session. One iteration has already been completed (or this is the first optimization after baseline).

## Step 1: Read current state

```bash
cat agent_workspace/decision.json
cat best/decision.json
cat agent_workspace/aggregation.js
cat agent_workspace/result.json
cat agent_workspace/input/structure.js
```

Understand:
- What was the last score and timing?
- What changes were made last iteration?
- What did you plan to try next?
- Are there still penalties remaining?

## Step 2: Decide whether to continue

If `decision.json` has `done: true`, stop immediately. Write:
> "Optimization complete. Reason: <stopReason>. Best score: <score>. Best pipeline is in best/aggregation.js."

Otherwise, proceed.

## Step 3: Apply one focused optimization

Pick the highest-priority remaining issue:

**Priority 1 — COLLSCAN present:**
- Check `structure.js` for indexes on fields used in `$match`
- Add `{ $match: { <indexed_field>: ... } }` as the first stage
- Or use `.hint()` — write it as a comment in the pipeline file: `// hint: { <index> }`
- Ensure the `$match` filter uses an indexed field

**Priority 2 — High cardinality ratio (> 10):**
- Move all `$match` stages earlier
- Add more selective filters before expensive stages
- Use `$limit` early if the query allows it

**Priority 3 — Fan-out from $unwind:**
- Check if `$unwind` is truly required
- If yes, move it as late as possible, after all filtering
- Consider `$lookup` with pipeline that filters inside instead of unwinding

**Priority 4 — Execution time:**
- Add early `$project` to drop unused fields
- Replace `$lookup` with denormalized data if `structure.js` shows embedded fields
- Use `$sortByCount` instead of `$group` + `$sort` where applicable

Make exactly ONE focused change per iteration. This makes it easier to measure the impact.

## Step 4: Save the modified pipeline

Write the updated pipeline to `agent_workspace/aggregation.js`.

## Step 5: Leave measurement and comparison to the harness

Do not run `measure.js` and do not update `best/`. The harness measures the
exact candidate after this turn, rejects incomplete measurements, and promotes
only an objectively better score or a same-score candidate at least 10% faster.

Set `improved` as your hypothesis; the harness determines the actual outcome.

**Set `done: true` if:**
- Score is 100
- All penalties resolved: no COLLSCAN, no fan-out, cardinality < 5
- You have tried all reasonable optimizations with no remaining ideas

Do not use `done: true` merely because a candidate did not improve. The
harness owns convergence and permits four non-improving attempts so it can
learn from verifier feedback.

## Step 7: Write decision.json

Write `agent_workspace/decision.json` with full details.

If `done: true`, set appropriate `stopReason`:
- `"optimal"` — score 100
- `"converged"` — reserved for the harness after 4 non-improving iterations
- `"resolved"` — all penalties cleared
- `"exhausted"` — no more options

## Step 8: Print summary table

Print the summary table as specified in your instructions.

End with one sentence describing what you changed and why, and (if not done) what you will try next.
