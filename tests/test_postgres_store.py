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

"""PostgresStore contract against a real server (marker: pg).

Mirrors tests/test_store.py (which covers MemoryStore) and adds the SQL-only
paths that never run in the in-memory suite: the conditional ``if_absent``
write and its ``StoreConflict``, exercised against actual Postgres.
"""

from __future__ import annotations

import asyncio

import pytest
from agentflow import StoreConflict

from conftest import POSTGRES_TEST_DSN, drop_table, unique_table

pytestmark = [
    pytest.mark.pg,
    pytest.mark.skipif(not POSTGRES_TEST_DSN, reason="POSTGRES_TEST_DSN not set"),
]


@pytest.fixture
async def store():
    from agentflow.store import PostgresStore

    table = unique_table("store_test")
    s = PostgresStore(POSTGRES_TEST_DSN, table=table)
    try:
        yield s
    finally:
        await s.close()
        await drop_table(POSTGRES_TEST_DSN, table)


async def test_put_get_roundtrip(store):
    item = await store.put(("users", "u1", "mem"), "name", "Ada")
    assert item.value == "Ada"
    got = await store.get(("users", "u1", "mem"), "name")
    assert got is not None and got.value == "Ada"
    assert got.created_at and got.updated_at


async def test_get_missing_returns_none(store):
    assert await store.get(("nope",), "x") is None


async def test_upsert_preserves_created_at(store):
    first = await store.put(("ns",), "k", 1)
    await asyncio.sleep(0.01)
    second = await store.put(("ns",), "k", 2)
    assert second.value == 2
    assert second.created_at == first.created_at
    assert second.updated_at >= first.updated_at


async def test_delete(store):
    await store.put(("ns",), "k", 1)
    assert await store.delete(("ns",), "k") is True
    assert await store.delete(("ns",), "k") is False
    assert await store.get(("ns",), "k") is None


async def test_search_by_prefix_ordered(store):
    await store.put(("users", "u1"), "b", 2)
    await store.put(("users", "u1"), "a", 1)
    await store.put(("users", "u2"), "a", 3)
    await store.put(("orgs", "o1"), "a", 9)
    got = await store.search(("users",))
    assert [(i.namespace, i.key) for i in got] == [
        (("users", "u1"), "a"),
        (("users", "u1"), "b"),
        (("users", "u2"), "a"),
    ]


async def test_search_pagination(store):
    for i in range(5):
        await store.put(("ns",), f"k{i}", i)
    page = await store.search(("ns",), limit=2, offset=2)
    assert [i.key for i in page] == ["k2", "k3"]


async def test_list_namespaces(store):
    await store.put(("users", "u1"), "a", 1)
    await store.put(("users", "u2"), "a", 1)
    await store.put(("orgs", "o1"), "a", 1)
    assert await store.list_namespaces(prefix=("users",)) == [("users", "u1"), ("users", "u2")]


async def test_ttl_expiry_hides_item(store):
    await store.put(("ns",), "k", "v", ttl=0.05)
    assert (await store.get(("ns",), "k")).value == "v"
    await asyncio.sleep(0.12)
    assert await store.get(("ns",), "k") is None
    assert await store.search(("ns",)) == []


# --- the SQL-only conditional write (never exercised by MemoryStore tests) ---


async def test_if_absent_first_write_succeeds(store):
    item = await store.put(("claims",), "k", {"status": "in_flight"}, if_absent=True)
    assert item.value == {"status": "in_flight"}


async def test_if_absent_second_write_conflicts(store):
    await store.put(("claims",), "k", 1, if_absent=True)
    with pytest.raises(StoreConflict) as ei:
        await store.put(("claims",), "k", 2, if_absent=True)
    assert ei.value.namespace == ("claims",)
    assert ei.value.key == "k"
    assert (await store.get(("claims",), "k")).value == 1  # unchanged


async def test_if_absent_ignores_unconditional_write(store):
    await store.put(("ns",), "k", 1, if_absent=True)
    await store.put(("ns",), "k", 2)  # unconditional overwrite, no conflict
    assert (await store.get(("ns",), "k")).value == 2


async def test_if_absent_reclaims_expired_key(store):
    await store.put(("ns",), "k", "old", ttl=0.05, if_absent=True)
    await asyncio.sleep(0.12)
    item = await store.put(("ns",), "k", "new", if_absent=True)  # expired == absent
    assert item.value == "new"


async def test_concurrent_if_absent_gives_one_winner(store):
    # Several conditional creates race the same key on a real server as the
    # FIRST operations on a fresh store — so this also exercises concurrent
    # schema bootstrap (ensure_schema's advisory lock). Exactly one wins and
    # the rest raise StoreConflict (atomic INSERT ... ON CONFLICT).
    async def claim():
        try:
            await store.put(("race",), "k", "mine", if_absent=True)
            return "won"
        except StoreConflict:
            return "lost"

    results = await asyncio.gather(claim(), claim(), claim())
    assert results.count("won") == 1
    assert results.count("lost") == 2


async def test_concurrent_first_use_creates_schema_once(store):
    # Many concurrent writes as the very first operations must not collide on
    # schema creation — ensure_schema serializes the CREATE TABLE under a
    # Postgres advisory lock. Each write is to a distinct key, so all succeed.
    async def write(i: int):
        await store.put(("boot",), f"k{i}", i)

    await asyncio.gather(*[write(i) for i in range(10)])
    got = await store.search(("boot",))
    assert len(got) == 10
