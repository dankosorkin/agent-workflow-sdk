# Phase 1: Baseline Measurement

You are starting a new aggregation optimization session. Your first task is to establish a baseline — measure the original pipeline as-is, without any changes.

## Step 1: Read input files

Read both input files to understand what you're working with:

```bash
cat agent_workspace/input/aggregation.js
cat agent_workspace/input/structure.js
```

## Step 2: Copy original pipeline to workspace

```bash
cp agent_workspace/input/aggregation.js agent_workspace/aggregation.js
```

## Step 3: Leave baseline measurement to the harness

Do not run `measure.js` and do not copy files into `best/`. After this turn,
the harness measures the exact candidate it finds and records the result.

## Step 4: State the baseline plan

You cannot know new metrics until the harness has measured them. Record the
questions you will answer from `agent_workspace/result.json` next turn:

1. What is the baseline score?
2. Are there COLLSCANs? How many and on which stages?
3. Is there fan-out from $unwind? Is it avoidable given the query intent?
4. What is the cardinality ratio? Is it high (> 10)?
5. What indexes are available from `structure.js`? Which ones are relevant?
6. What is the execution time baseline?

## Step 5: Write baseline decision.json

Write `agent_workspace/decision.json` with `improved: false` (this is baseline, not an improvement):

```json
{
  "improved": false,
  "done": false,
  "score": <baseline_score>,
  "previousScore": null,
  "timingMs": <avg_ms>,
  "hasCollscan": <true|false>,
  "hasFanout": <true|false>,
  "cardinalityRatio": <ratio_or_null>,
  "changes": "Baseline measurement — no changes applied",
  "reasoning": "<brief analysis of what is wrong and why>",
  "nextSteps": "<what you plan to try in iteration 1>",
  "stopReason": null
}
```

## Step 6: Leave best promotion to the harness

The harness promotes a successfully measured baseline atomically.

## Step 7: Print baseline summary table

Print the summary table as specified in your instructions.

Then write a brief paragraph (3–5 sentences) explaining:
- The main performance problems you identified
- Which indexes from `structure.js` can be leveraged
- Your optimization plan for the next iterations
