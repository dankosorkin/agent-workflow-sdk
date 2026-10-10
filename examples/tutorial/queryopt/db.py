# Copyright 2026 Daniel Sorkin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The problem: a seeded SQLite database and the slow query we optimize.

Four related tables, filled deterministically from a fixed seed so every run of
the tutorial sees the same data and the same plans. The row counts are chosen
so that a missing index genuinely hurts — small enough to build in a second,
large enough that a full scan is clearly worse than an index search.

The query under optimization is an analytical aggregation: the top products by
revenue in a date window for one customer segment, joining all four tables.
"""

from __future__ import annotations

import random
import sqlite3

__all__ = ["SCHEMA", "QUERY", "build_db", "connect"]

SCHEMA = """
CREATE TABLE customers (
    id       INTEGER PRIMARY KEY,
    name     TEXT NOT NULL,
    segment  TEXT NOT NULL          -- 'smb' | 'mid' | 'enterprise'
);
CREATE TABLE products (
    id        INTEGER PRIMARY KEY,
    name      TEXT NOT NULL,
    category  TEXT NOT NULL
);
CREATE TABLE orders (
    id           INTEGER PRIMARY KEY,
    customer_id  INTEGER NOT NULL,
    order_date   TEXT NOT NULL      -- ISO date 'YYYY-MM-DD'
);
CREATE TABLE order_items (
    id          INTEGER PRIMARY KEY,
    order_id    INTEGER NOT NULL,
    product_id  INTEGER NOT NULL,
    quantity    INTEGER NOT NULL,
    unit_price  REAL NOT NULL
);
"""

# Top 10 products by revenue, for enterprise customers, in a 2026 quarter.
# Joins all four tables, filters on segment and date, groups and ranks.
QUERY = """
SELECT p.id, p.name, SUM(oi.quantity * oi.unit_price) AS revenue
FROM order_items oi
JOIN orders o   ON o.id = oi.order_id
JOIN customers c ON c.id = o.customer_id
JOIN products p ON p.id = oi.product_id
WHERE c.segment = 'enterprise'
  AND o.order_date >= '2026-04-01'
  AND o.order_date <  '2026-07-01'
GROUP BY p.id, p.name
ORDER BY revenue DESC
LIMIT 10;
"""

_SEGMENTS = ("smb", "mid", "enterprise")
_CATEGORIES = ("hardware", "software", "services", "support")


def build_db(
    conn: sqlite3.Connection,
    *,
    customers: int = 2000,
    products: int = 500,
    orders: int = 40000,
    seed: int = 1234,
) -> None:
    """Create the schema and fill it deterministically from ``seed``.

    Defaults produce ~40k orders and ~120k order_items — enough that scanning
    orders by date or customer without an index is measurably worse than an
    index search.
    """
    rng = random.Random(seed)
    conn.executescript(SCHEMA)

    conn.executemany(
        "INSERT INTO customers (id, name, segment) VALUES (?, ?, ?)",
        [(i, f"cust{i}", rng.choice(_SEGMENTS)) for i in range(1, customers + 1)],
    )
    conn.executemany(
        "INSERT INTO products (id, name, category) VALUES (?, ?, ?)",
        [(i, f"prod{i}", rng.choice(_CATEGORIES)) for i in range(1, products + 1)],
    )

    # Orders spread across 2026; dates as ISO strings so a lexical index works.
    order_rows = []
    for oid in range(1, orders + 1):
        cust = rng.randint(1, customers)
        month = rng.randint(1, 12)
        day = rng.randint(1, 28)
        order_rows.append((oid, cust, f"2026-{month:02d}-{day:02d}"))
    conn.executemany(
        "INSERT INTO orders (id, customer_id, order_date) VALUES (?, ?, ?)",
        order_rows,
    )

    # 1-5 items per order.
    item_rows = []
    iid = 1
    for oid in range(1, orders + 1):
        for _ in range(rng.randint(1, 5)):
            item_rows.append(
                (iid, oid, rng.randint(1, products), rng.randint(1, 10), rng.uniform(5.0, 500.0))
            )
            iid += 1
    conn.executemany(
        "INSERT INTO order_items (id, order_id, product_id, quantity, unit_price) "
        "VALUES (?, ?, ?, ?, ?)",
        item_rows,
    )
    conn.commit()


def connect(seed: int = 1234, **sizes: int) -> sqlite3.Connection:
    """An in-memory database, built and seeded. Each call is independent."""
    conn = sqlite3.connect(":memory:")
    build_db(conn, seed=seed, **sizes)
    return conn
