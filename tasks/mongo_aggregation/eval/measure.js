#!/usr/bin/env node

/**
 * MongoDB Aggregation Evaluation Runner
 *
 * Measures an aggregation pipeline against defined quality metrics:
 *   - Execution time (average over N runs)
 *   - COLLSCAN presence (full collection scan — bad)
 *   - Fan-out detection (array unwind multiplying documents — watch)
 *   - Cardinality stability (docs in vs docs out ratio)
 *
 * Usage:
 *   node eval/measure.js <aggregation_file> [--runs N]
 *
 * Output:
 *   Writes JSON result to agent_workspace/result.json
 *   Exits 0 on success, 1 on error.
 */

"use strict";

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { MongoClient } = require("mongodb");

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

// Paths are resolved relative to the project root so the eval runner
// can live under tasks/<task>/eval/ while agent_workspace/ stays at the
// project root. Override via env vars when invoked from the orchestrator.
//   PROJECT_ROOT — where agent_workspace/ lives (default: two levels up)
//   ENV_FILE     — path to the .env file (default: <this dir>/../.env)
const TASK_DIR = path.resolve(__dirname, "..");
const PROJECT_ROOT = process.env.PROJECT_ROOT
  ? path.resolve(process.env.PROJECT_ROOT)
  : path.resolve(TASK_DIR, "..", "..");
const WORKSPACE = path.join(PROJECT_ROOT, "agent_workspace");
const RESULT_FILE = path.join(WORKSPACE, "result.json");

function loadEnv() {
  const envPath = process.env.ENV_FILE
    ? path.resolve(process.env.ENV_FILE)
    : path.join(TASK_DIR, ".env");
  if (!fs.existsSync(envPath)) return;
  const lines = fs.readFileSync(envPath, "utf8").split("\n");
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const eq = trimmed.indexOf("=");
    if (eq === -1) continue;
    const key = trimmed.slice(0, eq).trim();
    const val = trimmed.slice(eq + 1).trim();
    if (!process.env[key]) process.env[key] = val;
  }
}

loadEnv();

const MONGODB_URI = process.env.MONGODB_URI || "mongodb://localhost:27017";
const DATABASE = process.env.MONGODB_DATABASE;
const COLLECTION = process.env.MONGODB_COLLECTION;
const DEFAULT_RUNS = parseInt(process.env.MEASURE_RUNS || "5", 10);

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------

function parseArgs() {
  const args = process.argv.slice(2);
  let aggregationFile = null;
  let runs = DEFAULT_RUNS;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--runs" && args[i + 1]) {
      runs = parseInt(args[i + 1], 10);
      i++;
    } else if (!aggregationFile) {
      aggregationFile = args[i];
    }
  }

  if (!aggregationFile) {
    console.error("Usage: node eval/measure.js <aggregation_file> [--runs N]");
    process.exit(1);
  }

  return { aggregationFile, runs };
}

// ---------------------------------------------------------------------------
// Load pipeline from file
// ---------------------------------------------------------------------------

function loadPipeline(filePath) {
  const resolved = path.isAbsolute(filePath)
    ? filePath
    : path.resolve(process.cwd(), filePath);

  if (!fs.existsSync(resolved)) {
    throw new Error(`Aggregation file not found: ${resolved}`);
  }

  // Support: module.exports = [...] or just [...]
  // We use require() so the file can use JS expressions
  delete require.cache[require.resolve(resolved)];
  const mod = require(resolved);

  const pipeline = Array.isArray(mod) ? mod : mod.pipeline || mod.default;

  if (!Array.isArray(pipeline)) {
    throw new Error(
      `File must export an array (pipeline). Got: ${typeof pipeline}`
    );
  }

  return pipeline;
}

function sha256File(filePath) {
  return crypto.createHash("sha256").update(fs.readFileSync(filePath)).digest("hex");
}

// ---------------------------------------------------------------------------
// Metrics extraction from explain output
// ---------------------------------------------------------------------------

/**
 * Recursively walks explain plan stages and collects info.
 */
function walkStages(stage, acc) {
  if (!stage || typeof stage !== "object") return;

  const stageName = stage.stage || stage.type || "";

  // COLLSCAN detection
  if (stageName === "COLLSCAN") {
    acc.collscans.push({
      stage: stageName,
      filter: stage.filter || null,
    });
  }

  // IXSCAN — good
  if (stageName === "IXSCAN") {
    acc.ixscans.push({
      index: stage.indexName || "unknown",
      direction: stage.direction || "forward",
    });
  }

  // UNWIND fan-out
  if (stageName === "UNWIND" || stage.$unwind !== undefined) {
    acc.unwinds.push({
      field: stage.pathPrefix || stage.field || stage.$unwind || "unknown",
    });
  }

  // Recurse into inputStage / inputStages / stages
  if (stage.inputStage) walkStages(stage.inputStage, acc);
  if (Array.isArray(stage.inputStages)) {
    stage.inputStages.forEach((s) => walkStages(s, acc));
  }
  if (Array.isArray(stage.stages)) {
    stage.stages.forEach((s) => walkStages(s, acc));
  }
}

function extractPlanMetrics(explainResult) {
  const acc = {
    collscans: [],
    ixscans: [],
    unwinds: [],
  };

  // executionStats path
  const execStats = explainResult.executionStats;
  const queryPlanner = explainResult.queryPlanner;

  // Walk winning plan
  const winningPlan =
    queryPlanner?.winningPlan ||
    explainResult.stages?.[0]?.winningPlan ||
    null;

  if (winningPlan) {
    walkStages(winningPlan, acc);
  }

  // Also walk executionStages for runtime info
  if (execStats?.executionStages) {
    walkStages(execStats.executionStages, acc);
  }

  // For aggregate explain, walk each stage
  if (Array.isArray(explainResult.stages)) {
    for (const s of explainResult.stages) {
      walkStages(s, acc);
      // Each stage may have its own queryPlanner
      if (s.queryPlanner?.winningPlan) {
        walkStages(s.queryPlanner.winningPlan, acc);
      }
      if (s.executionStats?.executionStages) {
        walkStages(s.executionStats.executionStages, acc);
      }
    }
  }

  const docsExamined =
    execStats?.totalDocsExamined ??
    explainResult.executionStats?.totalDocsExamined ??
    null;

  const docsReturned =
    execStats?.nReturned ??
    explainResult.executionStats?.nReturned ??
    null;

  return {
    collscans: acc.collscans,
    ixscans: acc.ixscans,
    unwinds: acc.unwinds,
    docsExamined,
    docsReturned,
  };
}

// ---------------------------------------------------------------------------
// Cardinality ratio
// ---------------------------------------------------------------------------

function cardinalityRatio(docsExamined, docsReturned) {
  if (docsExamined === null || docsReturned === null) return null;
  if (docsReturned === 0) return null;
  return Math.round((docsExamined / docsReturned) * 100) / 100;
}

// ---------------------------------------------------------------------------
// Score calculation (0–100, higher is better)
// ---------------------------------------------------------------------------

/**
 * Scoring model:
 *   - COLLSCAN: -40 points each (critical)
 *   - Fan-out (unwind): -10 points each (warning)
 *   - Cardinality ratio > 10: -10 points (too many docs scanned per returned)
 *   - Cardinality ratio > 100: -20 points total
 *   - Execution time: relative, handled by orchestrator across iterations
 *
 * Max score: 100
 */
function calculateScore(metrics) {
  let score = 100;
  const penalties = [];

  for (const cs of metrics.collscans) {
    score -= 40;
    penalties.push({ reason: "COLLSCAN", impact: -40, detail: cs });
  }

  for (const uw of metrics.unwinds) {
    score -= 10;
    penalties.push({ reason: "UNWIND fan-out", impact: -10, detail: uw });
  }

  const ratio = metrics.cardinalityRatio;
  if (ratio !== null) {
    if (ratio > 100) {
      score -= 20;
      penalties.push({
        reason: "high cardinality ratio",
        impact: -20,
        detail: { ratio },
      });
    } else if (ratio > 10) {
      score -= 10;
      penalties.push({
        reason: "moderate cardinality ratio",
        impact: -10,
        detail: { ratio },
      });
    }
  }

  return {
    score: Math.max(0, score),
    penalties,
  };
}

// ---------------------------------------------------------------------------
// Main measurement
// ---------------------------------------------------------------------------

async function measure(pipeline, db, collectionName, runs) {
  const collection = db.collection(collectionName);

  // --- Explain (run once for plan metrics) ---
  const explainResult = await collection
    .aggregate(pipeline)
    .explain("executionStats");

  const planMetrics = extractPlanMetrics(explainResult);
  planMetrics.cardinalityRatio = cardinalityRatio(
    planMetrics.docsExamined,
    planMetrics.docsReturned
  );

  // --- Timing (run N times, discard first as warmup) ---
  const timings = [];

  for (let i = 0; i < runs + 1; i++) {
    const start = process.hrtime.bigint();
    const cursor = collection.aggregate(pipeline);
    // Drain the cursor to get full execution time
    await cursor.toArray();
    const end = process.hrtime.bigint();

    if (i > 0) {
      // skip warmup
      timings.push(Number(end - start) / 1_000_000); // ns -> ms
    }
  }

  const avgMs = timings.reduce((a, b) => a + b, 0) / timings.length;
  const minMs = Math.min(...timings);
  const maxMs = Math.max(...timings);

  return {
    timing: {
      avgMs: Math.round(avgMs * 100) / 100,
      minMs: Math.round(minMs * 100) / 100,
      maxMs: Math.round(maxMs * 100) / 100,
      runs: timings.length,
      allMs: timings.map((t) => Math.round(t * 100) / 100),
    },
    plan: planMetrics,
  };
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

async function main() {
  const { aggregationFile, runs } = parseArgs();

  if (!DATABASE) {
    console.error("ERROR: MONGODB_DATABASE not set in .env");
    process.exit(1);
  }
  if (!COLLECTION) {
    console.error("ERROR: MONGODB_COLLECTION not set in .env");
    process.exit(1);
  }

  let pipeline;
  try {
    pipeline = loadPipeline(aggregationFile);
  } catch (err) {
    console.error(`ERROR loading pipeline: ${err.message}`);
    process.exit(1);
  }

  console.error(
    `Measuring: ${aggregationFile} | db=${DATABASE} col=${COLLECTION} runs=${runs}`
  );

  const client = new MongoClient(MONGODB_URI);

  try {
    await client.connect();
    const db = client.db(DATABASE);

    const result = await measure(pipeline, db, COLLECTION, runs);

    const { score, penalties } = calculateScore(result.plan);

    const output = {
      timestamp: new Date().toISOString(),
      aggregationFile,
      candidateSha256: sha256File(
        path.isAbsolute(aggregationFile)
          ? aggregationFile
          : path.resolve(process.cwd(), aggregationFile)
      ),
      runs,
      timing: result.timing,
      plan: result.plan,
      score,
      penalties,
      // Convenience booleans for orchestrator
      hasCollscan: result.plan.collscans.length > 0,
      hasFanout: result.plan.unwinds.length > 0,
      cardinalityRatio: result.plan.cardinalityRatio,
    };

    fs.mkdirSync(WORKSPACE, { recursive: true });
    fs.writeFileSync(RESULT_FILE, JSON.stringify(output, null, 2), "utf8");

    // Also print to stdout for orchestrator to read directly
    console.log(JSON.stringify(output));

    console.error(`\nScore: ${score}/100`);
    console.error(`Avg time: ${result.timing.avgMs}ms`);
    console.error(`COLLSCAN: ${output.hasCollscan}`);
    console.error(`Fan-out: ${output.hasFanout}`);
    if (output.cardinalityRatio !== null) {
      console.error(`Cardinality ratio: ${output.cardinalityRatio}`);
    }
    if (penalties.length > 0) {
      console.error("Penalties:");
      for (const p of penalties) {
        console.error(`  ${p.reason}: ${p.impact}`);
      }
    }
  } finally {
    await client.close();
  }
}

main().catch((err) => {
  console.error(`FATAL: ${err.message}`);
  process.exit(1);
});
