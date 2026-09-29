# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""MemoryStore contract: get/put/delete, namespace search, TTL expiry."""

from __future__ import annotations

import asyncio

import pytest
from agentflow import Item, MemoryStore, Store


def _store():
    return MemoryStore()


async def test_put_get_roundtrip():
    s = _store()
    item = await s.put(("users", "u1", "mem"), "name", "Ada")
    assert isinstance(item, Item)
    got = await s.get(("users", "u1", "mem"), "name")
    assert got is not None and got.value == "Ada"
    assert got.created_at and got.updated_at


async def test_conforms_to_protocol():
    assert isinstance(_store(), Store)


async def test_get_missing_returns_none():
    s = _store()
    assert await s.get(("nope",), "x") is None


async def test_put_upsert_preserves_created_at():
    s = _store()
    first = await s.put(("ns",), "k", 1)
    await asyncio.sleep(0.01)
    second = await s.put(("ns",), "k", 2)
    assert second.value == 2
    assert second.created_at == first.created_at
    assert second.updated_at >= first.updated_at


async def test_delete():
    s = _store()
    await s.put(("ns",), "k", 1)
    assert await s.delete(("ns",), "k") is True
    assert await s.delete(("ns",), "k") is False
    assert await s.get(("ns",), "k") is None


async def test_search_by_prefix_ordered():
    s = _store()
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


async def test_search_pagination():
    s = _store()
    for i in range(5):
        await s.put(("ns",), f"k{i}", i)
    page = await s.search(("ns",), limit=2, offset=2)
    assert [i.key for i in page] == ["k2", "k3"]


async def test_list_namespaces():
    s = _store()
    await s.put(("users", "u1"), "a", 1)
    await s.put(("users", "u2"), "a", 1)
    await s.put(("orgs", "o1"), "a", 1)
    assert await s.list_namespaces(prefix=("users",)) == [("users", "u1"), ("users", "u2")]
    assert ("orgs", "o1") in await s.list_namespaces()


async def test_ttl_expiry_hides_item():
    s = _store()
    await s.put(("ns",), "k", "v", ttl=0.05)
    assert (await s.get(("ns",), "k")).value == "v"
    await asyncio.sleep(0.08)
    assert await s.get(("ns",), "k") is None
    # Expired item is excluded from search and namespace listing too.
    assert await s.search(("ns",)) == []
    assert ("ns",) not in await s.list_namespaces()


async def test_empty_namespace_rejected():
    s = _store()
    with pytest.raises(ValueError):
        await s.put(("",), "k", 1)
    with pytest.raises(ValueError):
        await s.put((), "k", 1)  # empty namespace is not addressable
