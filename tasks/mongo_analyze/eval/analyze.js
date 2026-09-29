#!/usr/bin/env node

/**
 * MongoDB Schema Analysis Evaluator (live database, read-only).
 *
 * Two responsibilities:
 *   1. INTROSPECT a live MongoDB: for each source collection, gather indexes,
 *      document count, field cardinality, and candidate unique keys. Verify
 *      relationship hypotheses (from the target mapping) against real data.
 *   2. SCORE the agent's analysis: the agent writes agent_workspace/analysis.json
 *      describing collections, relationships, and keys. This evaluator VERIFIES
 *      every claim against the live database (unique keys via a $group test,
 *      relationships via value-overlap) and scores coverage.
 *
 * Unique keys are split into:
 *   - guaranteed: backed by a unique index (fact, from getIndexes)
 *   - inferred:   empirically unique on observed data ($group test), tagged
 *                 with sample size and confidence — NOT a guarantee.
 *
 * Read-only: never writes to the database.
 *
 * Usage:
 *   node analyze.js --uri <mongodb-uri> --db <name> \
 *        [--analysis <agent_workspace/analysis.json>] \
 *        [--target <PartyContact.json>] [--sample N]
 *
 * Output: JSON to stdout + agent_workspace/result.json
 */

"use strict";

const fs = require("fs");
const path = require("path");
const { MongoClient } = require("mongodb");

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
  for (const line of fs.readFileSync(envPath, "utf8").split("\n")) {
    const t = line.trim();
    if (!t || t.startsWith("#")) continue;
    const eq = t.indexOf("=");
    if (eq === -1) continue;
    const k = t.slice(0, eq).trim();
    const v = t.slice(eq + 1).trim();
    if (!process.env[k]) process.env[k] = v;
  }
}
loadEnv();

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------

function parseArgs() {
  const args = process.argv.slice(2);
  const opts = {
    uri: process.env.MONGODB_URI || "mongodb://localhost:27017",
    db: process.env.MONGODB_DATABASE || null,
    analysis: path.join(WORKSPACE, "analysis.json"),
    target: null,
    sample: parseInt(process.env.ANALYZE_SAMPLE || "1000", 10),
  };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--uri" && args[i + 1]) opts.uri = args[++i];
    else if (args[i] === "--db" && args[i + 1]) opts.db = args[++i];
    else if (args[i] === "--analysis" && args[i + 1]) opts.analysis = args[++i];
    else if (args[i] === "--target" && args[i + 1]) opts.target = args[++i];
    else if (args[i] === "--sample" && args[i + 1]) opts.sample = parseInt(args[++i], 10);
  }
  return opts;
}

// ---------------------------------------------------------------------------
// Introspection primitives (read-only)
// ---------------------------------------------------------------------------

async function listCollections(db) {
  const cols = await db.listCollections({}, { nameOnly: true }).toArray();
  return cols.map((c) => c.name).filter((n) => !n.startsWith("system."));
}

async function collectionIndexes(coll) {
  const idx = await coll.indexes();
  return idx.map((i) => ({
    name: i.name,
    keys: i.key,
    unique: !!i.unique,
    sparse: !!i.sparse,
  }));
}

/**
 * Guaranteed unique keys: any unique index (excluding _id if requested).
 * These are facts enforced by the database.
 */
function guaranteedKeys(indexes) {
  return indexes
    .filter((i) => i.unique)
    .map((i) => ({
      fields: Object.keys(i.keys),
      source: "unique-index",
      indexName: i.name,
    }));
}

/**
 * Field cardinality: distinct-count per top-level field over a sample.
 * Returns [{ field, distinct, coverage }] sorted by distinct desc.
 */
async function fieldCardinality(coll, sampleDocs) {
  if (sampleDocs.length === 0) return [];
  const fields = new Set();
  for (const doc of sampleDocs) {
    for (const k of Object.keys(doc)) fields.add(k);
  }
  const total = sampleDocs.length;
  const out = [];
  for (const field of fields) {
    const distinct = new Set(
      sampleDocs.map((d) => JSON.stringify(d[field] ?? null))
    ).size;
    out.push({
      field,
      distinct,
      coverage: Math.round((distinct / total) * 100) / 100,
    });
  }
  return out.sort((a, b) => b.distinct - a.distinct);
}

/**
 * Verify a candidate key is unique on the live collection via a $group test.
 * Returns { unique: bool, duplicateCount, checkedDocs }.
 * This is the deterministic gate — the agent cannot fake it.
 */
async function verifyUnique(coll, fields, totalDocs) {
  const groupId = {};
  for (const f of fields) groupId[f] = `$${f}`;
  const dupes = await coll
    .aggregate([
      { $group: { _id: groupId, n: { $sum: 1 } } },
      { $match: { n: { $gt: 1 } } },
      { $count: "duplicateGroups" },
    ])
    .toArray();
  const duplicateGroups = dupes.length ? dupes[0].duplicateGroups : 0;
  return {
    unique: duplicateGroups === 0,
    duplicateGroups,
    checkedDocs: totalDocs,
  };
}

/**
 * Confidence for an inferred key: how much of the collection we could vouch
 * for. If we checked the full collection, 100%. On a sample, scaled down and
 * capped, and flagged when the collection is too small to trust at all.
 */
function inferredConfidence(totalDocs) {
  if (totalDocs < 3) return { confidence: 0, note: "insufficient data (<3 docs)" };
  if (totalDocs < 50) return { confidence: 40, note: "low sample size" };
  if (totalDocs < 1000) return { confidence: 70, note: "moderate sample size" };
  return { confidence: 95, note: "large collection" };
}

/**
 * Search for a MINIMAL inferred unique key: singles first, then pairs, from
 * high-cardinality candidate fields only. Stops at the first unique set.
 * Verified against the live collection with $group.
 */
async function inferUniqueKey(coll, cardinality, totalDocs) {
  if (totalDocs < 3) {
    return { key: null, ...inferredConfidence(totalDocs) };
  }
  // Candidate fields: skip constants (distinct <= 1), prefer high cardinality.
  const candidates = cardinality
    .filter((c) => c.distinct > 1 && c.field !== "_id")
    .slice(0, 8) // cap the search space
    .map((c) => c.field);

  // Singles
  for (const f of candidates) {
    const v = await verifyUnique(coll, [f], totalDocs);
    if (v.unique) {
      return { key: [f], verified: v, ...inferredConfidence(totalDocs) };
    }
  }
  // Pairs (minimal composite)
  for (let i = 0; i < candidates.length; i++) {
    for (let j = i + 1; j < candidates.length; j++) {
      const pair = [candidates[i], candidates[j]];
      const v = await verifyUnique(coll, pair, totalDocs);
      if (v.unique) {
        return { key: pair, verified: v, ...inferredConfidence(totalDocs) };
      }
    }
  }
  return { key: null, note: "no minimal unique key found in candidate fields (up to pairs)" };
}

module.exports = {
  guaranteedKeys,
  fieldCardinality,
  verifyUnique,
  inferUniqueKey,
  inferredConfidence,
};

// analyze.js continues in the next section (main + relationship verification + scoring)


// ---------------------------------------------------------------------------
// Relationship verification: confirm a hypothesized link between two
// collections by checking that values of a field in A actually appear in a
// field in B (value overlap on samples), and infer cardinality.
// ---------------------------------------------------------------------------

async function verifyRelationship(db, rel, sample) {
  // rel: { from, fromField, to, toField }
  const fromColl = db.collection(rel.from);
  const toColl = db.collection(rel.to);

  const fromVals = await fromColl
    .aggregate([{ $sample: { size: sample } }, { $project: { v: `$${rel.fromField}` } }])
    .toArray();
  const values = [...new Set(fromVals.map((d) => d.v).filter((v) => v != null))];
  if (values.length === 0) {
    return { confirmed: false, reason: "no values in fromField sample" };
  }

  // How many of those values exist in the target field?
  const matchCount = await toColl.countDocuments({ [rel.toField]: { $in: values } });

  // Cardinality: for a sampled from-value, how many to-docs match?
  const probe = values[0];
  const matchesForOne = await toColl.countDocuments({ [rel.toField]: probe });
  let cardinality = "unknown";
  if (matchCount > 0) {
    cardinality = matchesForOne > 1 ? "1:N" : "1:1";
  }

  return {
    confirmed: matchCount > 0,
    matchedValues: matchCount,
    sampledValues: values.length,
    cardinality,
  };
}

// ---------------------------------------------------------------------------
// Score the agent's analysis.json against the live database.
//
// analysis.json shape (written by the agent):
// {
//   "collections": [{ "name", "docCount", "uniqueKey": {...} }, ...],
//   "relationships": [{ "from","fromField","to","toField","cardinality" }, ...]
// }
//
// Score components (equal weight):
//   - collectionCoverage: fraction of live collections the agent analyzed
//   - keyQuality:        fraction of analyzed collections with a verified key
//   - relationshipQuality: fraction of claimed relationships confirmed on data
// ---------------------------------------------------------------------------

async function scoreAnalysis(db, analysis, liveCollections, sample) {
  const claimedCols = new Set((analysis.collections || []).map((c) => c.name));
  const collectionCoverage = liveCollections.length
    ? [...claimedCols].filter((c) => liveCollections.includes(c)).length /
      liveCollections.length
    : 0;

  // Verify each claimed unique key with the deterministic $group test.
  const keyResults = [];
  for (const c of analysis.collections || []) {
    if (!liveCollections.includes(c.name)) continue;
    const coll = db.collection(c.name);
    const total = await coll.estimatedDocumentCount();
    const fields = c.uniqueKey && c.uniqueKey.fields;
    if (!fields || fields.length === 0) {
      keyResults.push({ collection: c.name, verified: false, reason: "no key claimed" });
      continue;
    }
    const v = await verifyUnique(coll, fields, total);
    keyResults.push({ collection: c.name, fields, verified: v.unique, duplicateGroups: v.duplicateGroups });
  }
  const analyzed = keyResults.length || 1;
  const keyQuality = keyResults.filter((k) => k.verified).length / analyzed;

  // Verify each claimed relationship against live data.
  const relResults = [];
  for (const rel of analysis.relationships || []) {
    try {
      const r = await verifyRelationship(db, rel, sample);
      relResults.push({ ...rel, ...r });
    } catch (err) {
      relResults.push({ ...rel, confirmed: false, error: err.message });
    }
  }
  const relTotal = relResults.length || 1;
  const relationshipQuality = relResults.filter((r) => r.confirmed).length / relTotal;

  const score = Math.round(
    ((collectionCoverage + keyQuality + relationshipQuality) / 3) * 100
  );

  return {
    score,
    collectionCoverage: Math.round(collectionCoverage * 100),
    keyQuality: Math.round(keyQuality * 100),
    relationshipQuality: Math.round(relationshipQuality * 100),
    keyResults,
    relResults,
  };
}


// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const opts = parseArgs();
  if (!opts.db) {
    console.error("ERROR: --db (or MONGODB_DATABASE) is required");
    process.exit(1);
  }

  const client = new MongoClient(opts.uri);
  try {
    await client.connect();
    const db = client.db(opts.db);
    const liveCollections = await listCollections(db);

    // Always produce an introspection report of the live database, so the
    // agent has real facts to build analysis.json from.
    const introspection = [];
    for (const name of liveCollections) {
      const coll = db.collection(name);
      const total = await coll.estimatedDocumentCount();
      const indexes = await collectionIndexes(coll);
      const sampleDocs = await coll
        .aggregate([{ $sample: { size: Math.min(opts.sample, Math.max(total, 1)) } }])
        .toArray();
      const cardinality = await fieldCardinality(coll, sampleDocs);
      const guaranteed = guaranteedKeys(indexes);
      const inferred = await inferUniqueKey(coll, cardinality, total);
      introspection.push({
        name,
        docCount: total,
        indexes,
        cardinality: cardinality.slice(0, 15),
        guaranteedKeys: guaranteed,
        inferredKey: inferred,
      });
    }

    // If the agent has written analysis.json, score it. Otherwise this is a
    // pure introspection run (baseline) and score reflects raw discovery.
    let scoring = null;
    let analysis = null;
    if (fs.existsSync(opts.analysis)) {
      try {
        analysis = JSON.parse(fs.readFileSync(opts.analysis, "utf8"));
        scoring = await scoreAnalysis(db, analysis, liveCollections, opts.sample);
      } catch (err) {
        scoring = { score: 0, error: `invalid analysis.json: ${err.message}` };
      }
    }

    const out = {
      timestamp: new Date().toISOString(),
      database: opts.db,
      liveCollections,
      introspection,
      analysisPresent: analysis !== null,
      score: scoring ? scoring.score : 0,
      scoring,
    };

    fs.mkdirSync(WORKSPACE, { recursive: true });
    fs.writeFileSync(RESULT_FILE, JSON.stringify(out, null, 2), "utf8");
    console.log(JSON.stringify(out));

    console.error(`\nDatabase: ${opts.db}`);
    console.error(`Collections: ${liveCollections.length}`);
    if (scoring) {
      console.error(`Score: ${scoring.score}%`);
      console.error(`  collection coverage: ${scoring.collectionCoverage}%`);
      console.error(`  key quality:         ${scoring.keyQuality}%`);
      console.error(`  relationship quality:${scoring.relationshipQuality}%`);
    } else {
      console.error("Introspection only (no analysis.json yet).");
    }
  } finally {
    await client.close();
  }
}

if (require.main === module) {
  main().catch((err) => {
    console.error(`FATAL: ${err.message}`);
    process.exit(1);
  });
}
