/**
 * Target schema and index structure.
 *
 * Describes the collections involved, their fields, and the indexes
 * available for the aggregation to leverage.
 *
 * The agent should use this to:
 *   1. Identify which fields are indexed for $match filters
 *   2. Understand the data shape for denormalization opportunities
 *   3. Choose optimal $lookup join fields
 */

module.exports = {

  // -----------------------------------------------------------------------
  // Collection: users
  // -----------------------------------------------------------------------
  users: {
    description: "Application users",

    fields: {
      _id:       { type: "ObjectId" },
      status:    { type: "string", enum: ["active", "inactive", "banned"] },
      email:     { type: "string" },
      createdAt: { type: "Date" },
      updatedAt: { type: "Date" },
    },

    indexes: [
      { name: "_id_",        keys: { _id: 1 },     unique: true },
      { name: "status_1",    keys: { status: 1 },  sparse: false },
      { name: "email_1",     keys: { email: 1 },   unique: true },
    ],

    approximateDocCount: 500_000,
  },

  // -----------------------------------------------------------------------
  // Collection: orders
  // -----------------------------------------------------------------------
  orders: {
    description: "Customer orders",

    fields: {
      _id:       { type: "ObjectId" },
      userId:    { type: "ObjectId", ref: "users" },
      status:    { type: "string", enum: ["pending", "paid", "shipped", "cancelled"] },
      createdAt: { type: "Date" },
      updatedAt: { type: "Date" },

      // Embedded array — each order has 1–20 items
      items: {
        type: "array",
        itemShape: {
          sku:      { type: "string" },
          category: { type: "string" },
          price:    { type: "number" },
          qty:      { type: "number" },
        },
      },

      // Denormalized summary — pre-computed on write
      summary: {
        totalAmount: { type: "number" },
        itemCount:   { type: "number" },
      },
    },

    indexes: [
      { name: "_id_",                  keys: { _id: 1 },                    unique: true },
      { name: "userId_1",              keys: { userId: 1 } },
      { name: "userId_1_createdAt_-1", keys: { userId: 1, createdAt: -1 },
        description: "Compound index — ideal for per-user recent orders lookup" },
      { name: "createdAt_-1",          keys: { createdAt: -1 } },
      { name: "status_1_createdAt_-1", keys: { status: 1, createdAt: -1 } },
    ],

    approximateDocCount: 8_000_000,
  },

  // -----------------------------------------------------------------------
  // Optimization notes for the agent
  // -----------------------------------------------------------------------
  notes: [
    "users.status is indexed — always $match on status BEFORE $lookup",
    "orders has a compound index on {userId, createdAt} — use this for the $lookup pipeline filter to avoid scanning all orders per user",
    "orders.items is an embedded array — $unwind is required but should happen AFTER $lookup filtering",
    "orders.summary.totalAmount is pre-computed — consider using it instead of $unwind + $multiply when only total is needed",
    "For revenue-per-category queries, $unwind on items is unavoidable but cardinality can be controlled by filtering orders first",
  ],
};
