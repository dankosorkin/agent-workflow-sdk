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

"""Store contract, run against every offline backend (memory and SQLite):
get/put/delete, namespace search, TTL expiry, conditional ``if_absent`` writes."""

from __future__ import annotations

import asyncio

import pytest
from agentflow import Item, MemoryStore, SqliteStore, Store


@pytest.fixture(params=["memory", "sqlite"])
def store(request, tmp_path):
    if request.param == "memory":
        return MemoryStore()
    return SqliteStore(tmp_path / "store.db")


async def test_put_get_roundtrip(store):
    s = store
    item = await s.put(("users", "u1", "mem"), "name", "Ada")
    assert isinstance(item, Item)
    got = await s.get(("users", "u1", "mem"), "name")
    assert got is not None and got.value == "Ada"
    assert got.created_at and got.updated_at


async def test_conforms_to_protocol(store):
    assert isinstance(store, Store)


async def test_get_missing_returns_none(store):
    s = store
    assert await s.get(("nope",), "x") is None


async def test_put_upsert_preserves_created_at(store):
    s = store
    first = await s.put(("ns",), "k", 1)
    await asyncio.sleep(0.01)
    second = await s.put(("ns",), "k", 2)
    assert second.value == 2
    assert second.created_at == first.created_at
    assert second.updated_at >= first.updated_at


async def test_delete(store):
    s = store
    await s.put(("ns",), "k", 1)
    assert await s.delete(("ns",), "k") is True
    assert await s.delete(("ns",), "k") is False
    assert await s.get(("ns",), "k") is None


async def test_search_by_prefix_ordered(store):
    s = store
    await s.put(("users", "u1"), "b", 2)
    await s.put(("users", "u1"), "a", 1)
    await s.put(("users", "u2"), "a", 3)
    await s.put(("orgs", "o1"), "a", 9)
    got = await s.search(("users",))
    assert [(i.namespace, i.key) for i in got] == [
        (("users", "u1"), "a"),
        (("users", "u1"), "b"),
        (("users", "u2"), "a"),
    ]


async def test_search_pagination(store):
    s = store
    for i in range(5):
        await s.put(("ns",), f"k{i}", i)
    page = await s.search(("ns",), limit=2, offset=2)
    assert [i.key for i in page] == ["k2", "k3"]


async def test_list_namespaces(store):
    s = store
    await s.put(("users", "u1"), "a", 1)
    await s.put(("users", "u2"), "a", 1)
    await s.put(("orgs", "o1"), "a", 1)
    assert await s.list_namespaces(prefix=("users",)) == [("users", "u1"), ("users", "u2")]
    assert ("orgs", "o1") in await s.list_namespaces()


async def test_ttl_expiry_hides_item(store):
    s = store
    await s.put(("ns",), "k", "v", ttl=0.05)
    assert (await s.get(("ns",), "k")).value == "v"
    await asyncio.sleep(0.08)
    assert await s.get(("ns",), "k") is None
    # Expired item is excluded from search and namespace listing too.
    assert await s.search(("ns",)) == []
    assert ("ns",) not in await s.list_namespaces()


async def test_empty_namespace_rejected(store):
    s = store
    with pytest.raises(ValueError):
        await s.put(("",), "k", 1)
    with pytest.raises(ValueError):
        await s.put((), "k", 1)  # empty namespace is not addressable


# --- conditional write: put(if_absent=True) ---


async def test_if_absent_first_write_succeeds(store):
    s = store
    item = await s.put(("claims",), "k", {"status": "in_flight"}, if_absent=True)
    assert item.value == {"status": "in_flight"}


async def test_if_absent_second_write_conflicts(store):
    from agentflow import StoreConflict

    s = store
    await s.put(("claims",), "k", 1, if_absent=True)
    with pytest.raises(StoreConflict) as ei:
        await s.put(("claims",), "k", 2, if_absent=True)
    assert ei.value.namespace == ("claims",)
    assert ei.value.key == "k"
    # the live value is unchanged by the failed conditional write
    assert (await s.get(("claims",), "k")).value == 1


async def test_if_absent_ignores_unconditional_write(store):
    # A normal put still overwrites; if_absent only guards when set.
    s = store
    await s.put(("ns",), "k", 1, if_absent=True)
    await s.put(("ns",), "k", 2)  # unconditional, no conflict
    assert (await s.get(("ns",), "k")).value == 2


async def test_if_absent_reclaims_expired_key(store):
    # An expired item counts as absent, so its key can be claimed again.
    s = store
    await s.put(("ns",), "k", "old", ttl=0.05, if_absent=True)
    await asyncio.sleep(0.08)
    item = await s.put(("ns",), "k", "new", if_absent=True)  # no conflict
    assert item.value == "new"


async def test_if_absent_resets_created_at_on_reclaim(store):
    # Reclaiming an expired key is a fresh insert, not an upsert.
    s = store
    first = await s.put(("ns",), "k", 1, ttl=0.05, if_absent=True)
    await asyncio.sleep(0.08)
    second = await s.put(("ns",), "k", 2, if_absent=True)
    assert second.created_at >= first.created_at
