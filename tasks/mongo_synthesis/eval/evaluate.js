#!/usr/bin/env node

/**
 * MongoDB Aggregation Synthesis Evaluator (in-memory).
 *
 * Evaluates a candidate aggregation that must transform SOURCE collections
 * into a TARGET structure. Runs the pipeline in-memory via `mingo` (no live
 * MongoDB), then does a STRUCTURAL DIFF of the output against the target
 * schema: for every target path, checks presence AND type.
 *
 * Two-phase score (correctness is a hard gate):
 *   - correctness < 100  -> phase "synthesis",   score = correctness
 *   - correctness == 100 -> phase "optimization", score = 100 (+ perf handled
 *                            downstream by the mongo-aggregation task)
 *
 * Usage:
 *   node evaluate.js <aggregation_file> \
 *        --sources <dir> --target <file> [--primary <collection>]
 *
 * Output: JSON to stdout + agent_workspace/result.json
 */

"use strict";

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { Aggregator } = require("mingo");
// Enable the full operator set (lookup, unwind, group, etc.)
require("mingo/init/system");

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

const TASK_DIR = path.resolve(__dirname, "..");
const PROJECT_ROOT = process.env.PROJECT_ROOT
  ? path.resolve(process.env.PROJECT_ROOT)
  : path.resolve(TASK_DIR, "..", "..");
const WORKSPACE = path.join(PROJECT_ROOT, "agent_workspace");
const RESULT_FILE = path.join(WORKSPACE, "result.json");

// ---------------------------------------------------------------------------
// CLI args
// ---------------------------------------------------------------------------

function parseArgs() {
  const args = process.argv.slice(2);
  let aggregationFile = null;
  let sourcesDir = path.join(TASK_DIR, "data");
  let targetFile = path.join(TASK_DIR, "structure", "PartyContact.json");
  let primary = null;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--sources" && args[i + 1]) sourcesDir = args[++i];
    else if (args[i] === "--target" && args[i + 1]) targetFile = args[++i];
    else if (args[i] === "--primary" && args[i + 1]) primary = args[++i];
    else if (!aggregationFile) aggregationFile = args[i];
  }

  if (!aggregationFile) {
    console.error(
      "Usage: node evaluate.js <aggregation_file> [--sources DIR] [--target FILE] [--primary COLLECTION]"
    );
    process.exit(1);
  }
  return { aggregationFile, sourcesDir, targetFile, primary };
}

// ---------------------------------------------------------------------------
// Load sources: each <name>.json in the sources dir becomes a collection.
// Files may hold a single document or an array of documents.
//
// The fixtures are MongoDB Extended JSON. Convert its scalar wrappers before
// giving documents to mingo so the evaluator observes the same logical types
// an aggregation sees in MongoDB, rather than implementation artifacts such
// as { "$numberLong": "24730668" } objects.
// ---------------------------------------------------------------------------

function normalizeExtendedJson(value) {
  if (Array.isArray(value)) return value.map(normalizeExtendedJson);
  if (!value || typeof value !== "object") return value;

  const keys = Object.keys(value);
  if (keys.length === 1 && keys[0] === "$numberLong") {
    const number = Number(value.$numberLong);
    if (!Number.isSafeInteger(number)) {
      throw new Error(`unsafe Extended JSON $numberLong: ${value.$numberLong}`);
    }
    return number;
  }
  if (keys.length === 1 && keys[0] === "$date") {
    const date = new Date(normalizeExtendedJson(value.$date));
    if (Number.isNaN(date.getTime())) {
      throw new Error(`invalid Extended JSON $date: ${value.$date}`);
    }
    return date;
  }
  if (keys.length === 1 && keys[0] === "$oid") return value.$oid;

  return Object.fromEntries(
    Object.entries(value).map(([key, nested]) => [key, normalizeExtendedJson(nested)])
  );
}

function loadSources(dir) {
  if (!fs.existsSync(dir)) throw new Error(`Sources dir not found: ${dir}`);
  const collections = {};
  for (const file of fs.readdirSync(dir)) {
    if (!file.endsWith(".json")) continue;
    const name = path.basename(file, ".json");
    const raw = normalizeExtendedJson(
      JSON.parse(fs.readFileSync(path.join(dir, file), "utf8"))
    );
    collections[name] = Array.isArray(raw) ? raw : [raw];
  }
  return collections;
}

// ---------------------------------------------------------------------------
// Load the candidate pipeline.
// ---------------------------------------------------------------------------

function loadPipeline(filePath) {
  const resolved = path.isAbsolute(filePath)
    ? filePath
    : path.resolve(process.cwd(), filePath);
  if (!fs.existsSync(resolved)) {
    throw new Error(`Aggregation file not found: ${resolved}`);
  }
  delete require.cache[require.resolve(resolved)];
  const mod = require(resolved);
  const pipeline = Array.isArray(mod) ? mod : mod.pipeline || mod.default;
  if (!Array.isArray(pipeline)) {
    throw new Error(`Aggregation must export an array. Got: ${typeof pipeline}`);
  }
  return pipeline;
}

function sha256File(filePath) {
  return crypto.createHash("sha256").update(fs.readFileSync(filePath)).digest("hex");
}

// ---------------------------------------------------------------------------
// Parse the TARGET schema into a list of expected paths with types.
//
// The target is a schema document, not sample data. Leaf values are strings
// describing type + source mapping, e.g.:
//   "number - MISPAR_KTOVET"
//   "string | omitted - ATAK112.SHEM_YISHUV_TIKNI"
//   "boolean - KOD_KTOVET_MSB != 0"
// We extract the leading type token and whether the field is optional
// (contains "omitted"). Keys starting with "_" are metadata and skipped.
// Arrays are described by their first element (the item schema).
// ---------------------------------------------------------------------------

const TYPE_TOKENS = new Set([
  "string", "number", "boolean", "date", "array", "object", "any",
]);

function parseLeafType(desc) {
  if (typeof desc !== "string") return { type: "any", optional: false };
  const optional = /\bomitted\b/i.test(desc);
  // First word before space/pipe is the type token
  const token = desc.trim().split(/[\s|]/)[0].toLowerCase();
  const type = TYPE_TOKENS.has(token) ? token : "any";
  return { type, optional };
}

/**
 * Walk the target schema, producing { path, type, optional } entries.
 * `path` uses dot notation; arrays contribute their item schema under the
 * array path (we validate the array itself is an array, and its items match).
 */
function collectTargetPaths(node, prefix, out) {
  if (Array.isArray(node)) {
    // Array field: record the array itself, then descend into item schema
    out.push({ path: prefix, type: "array", optional: false });
    if (node.length > 0 && typeof node[0] === "object" && node[0] !== null) {
      // Skip pure-ref item markers like { "_ref": "..." }
      const item = node[0];
      const keys = Object.keys(item).filter((k) => !k.startsWith("_"));
      if (keys.length > 0) {
        collectTargetPaths(item, prefix + "[]", out);
      }
    }
    return;
  }

  if (node && typeof node === "object") {
    for (const [key, value] of Object.entries(node)) {
      if (key.startsWith("_")) continue; // metadata: _id note excluded? keep _id
      const childPath = prefix ? `${prefix}.${key}` : key;
      collectTargetPaths(value, childPath, out);
    }
    return;
  }

  // Leaf
  const { type, optional } = parseLeafType(node);
  out.push({ path: prefix, type, optional });
}

// ---------------------------------------------------------------------------
// Inspect the ACTUAL output: gather the set of present paths with JS types.
// We sample across all output docs (a path counts as present if any doc has it,
// and array items are inspected element-wise).
// ---------------------------------------------------------------------------

function jsType(value) {
  if (value === null || value === undefined) return "missing";
  if (Array.isArray(value)) return "array";
  if (value instanceof Date) return "date";
  return typeof value; // string | number | boolean | object
}

/**
 * Resolve a target path (with optional "[]" array hops) against a doc,
 * returning the set of observed JS types for that path across the doc.
 */
function observeTypes(doc, pathParts, acc) {
  let current = [doc];
  for (let i = 0; i < pathParts.length; i++) {
    const part = pathParts[i];
    const next = [];
    for (const node of current) {
      if (node === null || node === undefined) continue;
      if (part === "[]") {
        if (Array.isArray(node)) next.push(...node);
      } else if (typeof node === "object" && part in node) {
        next.push(node[part]);
      }
    }
    current = next;
    if (current.length === 0) break;
  }
  for (const v of current) acc.add(jsType(v));
  return acc;
}

function pathToParts(targetPath) {
  // Convert "a.b[].c" -> ["a","b","[]","c"]
  const parts = [];
  for (const seg of targetPath.split(".")) {
    if (seg.endsWith("[]")) {
      parts.push(seg.slice(0, -2));
      parts.push("[]");
    } else {
      parts.push(seg);
    }
  }
  return parts;
}

// Whether an observed JS type set satisfies the expected schema type.
function typeMatches(expected, observedSet) {
  if (observedSet.size === 0) return false;
  if (expected === "any") return !observedSet.has("missing");
  const compatible = {
    string: ["string"],
    number: ["number"],
    boolean: ["boolean"],
    date: ["date", "string"], // dates often serialize to string
    array: ["array"],
    object: ["object"],
  }[expected] || [];
  for (const t of observedSet) {
    if (t === "missing") continue;
    if (compatible.includes(t)) return true;
  }
  return false;
}

// ---------------------------------------------------------------------------
// Correctness scoring: presence + type across all target paths.
// ---------------------------------------------------------------------------

function scoreCorrectness(targetPaths, output) {
  const results = [];
  let satisfied = 0;
  let required = 0;

  for (const { path: tp, type, optional } of targetPaths) {
    const parts = pathToParts(tp);
    const observed = new Set();
    for (const doc of output) observeTypes(doc, parts, observed);

    const present = observed.size > 0 && !(observed.size === 1 && observed.has("missing"));
    const typeOk = typeMatches(type, observed);
    const ok = present && typeOk;

    if (!optional) {
      required += 1;
      if (ok) satisfied += 1;
    }

    results.push({
      path: tp,
      expectedType: type,
      optional,
      present,
      typeOk,
      observed: [...observed],
      ok,
    });
  }

  // Correctness is measured over REQUIRED paths (optional ones don't penalize,
  // but their failures are still reported for the agent to see).
  const correctness = required === 0 ? 100 : Math.round((satisfied / required) * 100);
  return { correctness, satisfied, required, results };
}

// ---------------------------------------------------------------------------
// Semantic scoring: prove that the output represents the source links, not
// merely that it contains correctly typed empty arrays.
// ---------------------------------------------------------------------------

const CONTACT_TYPES = [
  { code: 12, path: "localAddress", collection: "ATAK112", field: "SHEM_YISHUV_TIKNI", output: "city" },
  { code: 16, path: "abroadAddress", collection: "ATAK116", field: "SHEM_YISHUV_CHUL", output: "city" },
  { code: 20, path: "pOB", collection: "ATAK120", field: "TA_DOAR", output: "pOBNumber" },
  { code: 40, path: "pOBInBranch", collection: "ATAK140", field: "MISPAR_SNIF", output: "branchNumber" },
  { code: 50, path: "swiftAddress", collection: "ATAK150", field: "KOD_BANK_SWIFT", output: "swiftBank" },
  { code: 60, path: "localPhone", collection: "ATAK160", field: "MISPAR_TELEPHON", output: "phoneNumber" },
  { code: 66, path: "abroadPhone", collection: "ATAK166", field: "MPR_TELEPHON_CHUL", output: "phoneNumber" },
  { code: 90, path: "email", collection: "ATAK190", field: "KTOVET_E_MAIL", output: "emailAddress" },
];

function sameScalar(left, right) {
  if (left instanceof Date && right instanceof Date) return left.getTime() === right.getTime();
  return left === right;
}

function hasMeaningfulValue(value) {
  return value !== undefined && value !== null &&
    (typeof value !== "string" || value.trim().length > 0);
}

function semanticChecks(collections, output) {
  const links = collections.ATAKL10;
  if (!Array.isArray(links)) {
    return [{ kind: "source", message: "ATAKL10 is required as the contact-link source" }];
  }

  const outputByCustomer = new Map(output.map((doc) => [String(doc._id), doc]));
  const detailIndexes = new Map();
  const failures = [];

  for (const link of links) {
    const config = CONTACT_TYPES.find((item) => item.code === link.SUG_MIVNE_KTOVET);
    if (!config) continue;

    const customer = String(link.SIFRUR_LAKOACH);
    const document = outputByCustomer.get(customer);
    if (!document) {
      failures.push({ kind: "customer", customer, message: "missing output document" });
      continue;
    }

    const expectedId = link.MISPAR_KTOVET;
    const contacts = document.contact?.[config.path];
    const contact = Array.isArray(contacts)
      ? contacts.find((item) => sameScalar(item?.addressSerialId, expectedId))
      : undefined;
    if (!contact) {
      failures.push({
        kind: "link",
        customer,
        contactType: config.path,
        addressSerialId: expectedId,
        message: "source contact link was not materialized",
      });
      continue;
    }

    if (!detailIndexes.has(config.collection)) {
      const index = new Map(
        (collections[config.collection] || []).map((item) => [String(item.MISPAR_KTOVET), item])
      );
      detailIndexes.set(config.collection, index);
    }
    const detail = detailIndexes.get(config.collection).get(String(expectedId));
    const expectedValue = detail?.[config.field];
    if (hasMeaningfulValue(expectedValue) && !sameScalar(contact[config.output], expectedValue)) {
      failures.push({
        kind: "detail",
        customer,
        contactType: config.path,
        addressSerialId: expectedId,
        outputPath: `${config.path}[].${config.output}`,
        expected: expectedValue,
        observed: contact[config.output],
        message: `mapped ${config.collection}.${config.field} is missing or incorrect`,
      });
    }
  }
  return failures;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

function main() {
  const { aggregationFile, sourcesDir, targetFile, primary } = parseArgs();

  const collections = loadSources(sourcesDir);
  const collectionNames = Object.keys(collections);
  if (collectionNames.length === 0) {
    console.error(`ERROR: no source collections found in ${sourcesDir}`);
    process.exit(1);
  }

  const target = JSON.parse(fs.readFileSync(targetFile, "utf8"));
  const targetPaths = [];
  collectTargetPaths(target, "", targetPaths);

  const pipeline = loadPipeline(aggregationFile);
  const resolvedAggregationFile = path.isAbsolute(aggregationFile)
    ? aggregationFile
    : path.resolve(process.cwd(), aggregationFile);

  // Choose the primary collection the pipeline runs against.
  // Default: the one the agent names via --primary, else the largest,
  // else the first alphabetically. $lookup stages reference the others.
  const primaryName =
    primary ||
    collectionNames.sort((a, b) => collections[b].length - collections[a].length)[0];

  // mingo runs a pipeline over an input array; $lookup resolves other
  // collections via options.collectionResolver.
  const options = {
    collectionResolver: (name) => collections[name] || [],
  };

  let output;
  let runError = null;
  try {
    const agg = new Aggregator(pipeline, options);
    output = agg.run(collections[primaryName]);
  } catch (err) {
    runError = err.message;
    output = [];
  }

  const { correctness: structuralCorrectness, satisfied, required, results } = scoreCorrectness(
    targetPaths,
    output
  );

  const semanticFailures = semanticChecks(collections, output);
  const semanticRequired = collections.ATAKL10?.filter((link) =>
    CONTACT_TYPES.some((item) => item.code === link.SUG_MIVNE_KTOVET)
  ).length || 0;
  const semanticCorrectness = semanticRequired === 0
    ? 0
    : Math.round(((semanticRequired - semanticFailures.length) / semanticRequired) * 100);
  // Both contracts are hard gates. A typed shape without source-derived
  // values is not a valid synthesis result.
  const correctness = Math.min(structuralCorrectness, semanticCorrectness);

  const phase = correctness === 100 ? "optimization" : "synthesis";
  const score = correctness; // phase 1 = correctness gate; perf handled downstream

  const missingRequired = results.filter((r) => !r.optional && !r.ok);

  const out = {
    timestamp: new Date().toISOString(),
    aggregationFile,
    candidateSha256: sha256File(resolvedAggregationFile),
    primaryCollection: primaryName,
    sourceCollections: collectionNames,
    outputDocCount: output.length,
    runError,
    phase,
    correctness,
    structuralCorrectness,
    semanticCorrectness,
    semanticFailures,
    requiredPaths: required,
    satisfiedPaths: satisfied,
    score,
    // Full per-path diff so the agent can see exactly what is missing/mistyped
    diff: results,
    missingRequired: missingRequired.map((r) => ({
      path: r.path,
      expectedType: r.expectedType,
      observed: r.observed,
    })),
  };

  fs.mkdirSync(WORKSPACE, { recursive: true });
  fs.writeFileSync(RESULT_FILE, JSON.stringify(out, null, 2), "utf8");
  console.log(JSON.stringify(out));

  console.error(`\nPhase: ${phase}`);
  console.error(`Correctness: ${correctness}% (${satisfied}/${required} required paths)`);
  console.error(`Output docs: ${output.length}`);
  if (runError) console.error(`Pipeline error: ${runError}`);
  if (missingRequired.length > 0) {
    console.error(`Missing/mistyped required paths (first 10):`);
    for (const r of missingRequired.slice(0, 10)) {
      console.error(`  ${r.path} (want ${r.expectedType}, saw ${r.observed.join(",") || "nothing"})`);
    }
  }
}

main();
