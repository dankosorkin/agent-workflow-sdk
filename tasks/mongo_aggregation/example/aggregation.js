/**
 * Example aggregation pipeline — intentionally suboptimal.
 *
 * Business goal: for each active user, find their orders from the last
 * 30 days, expand order items, and compute total revenue per category.
 *
 * Problems embedded for the agent to find and fix:
 *   1. $lookup before $match — joins everything, then filters
 *   2. $unwind before $group — explodes documents early
 *   3. No index hint — likely triggers COLLSCAN on orders
 *   4. $project placed at the end — carries all fields through the pipeline
 *   5. $sort after $group without $limit — sorts the full result set
 */

module.exports = [
  // BAD: joins all orders before filtering active users
  {
    $lookup: {
      from: "orders",
      localField: "_id",
      foreignField: "userId",
      as: "orders",
    },
  },

  // BAD: filter on users happens after the expensive $lookup
  {
    $match: {
      status: "active",
      "orders.createdAt": {
        $gte: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000),
      },
    },
  },

  // BAD: unwinds before any grouping or further filtering
  {
    $unwind: "$orders",
  },

  // BAD: unwinds again — double fan-out
  {
    $unwind: "$orders.items",
  },

  // Group by user + category to compute revenue
  {
    $group: {
      _id: {
        userId: "$_id",
        category: "$orders.items.category",
      },
      totalRevenue: { $sum: { $multiply: ["$orders.items.price", "$orders.items.qty"] } },
      orderCount: { $sum: 1 },
    },
  },

  // Sort by revenue descending
  {
    $sort: { totalRevenue: -1 },
  },

  // BAD: $project at the end — all fields carried through the whole pipeline
  {
    $project: {
      _id: 0,
      userId: "$_id.userId",
      category: "$_id.category",
      totalRevenue: 1,
      orderCount: 1,
    },
  },
];
