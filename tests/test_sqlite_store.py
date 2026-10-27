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

"""SqliteStore behaviour beyond the shared contract in ``test_store.py``.

What only a file-backed store can get wrong: surviving a restart, atomic
``if_absent`` between independent store objects sharing one file, ordering
parity with ``MemoryStore`` (so swapping backends never reorders results),
namespace encoding edge cases, and the guarantees ``IdempotentOp`` builds on.
"""

from __future__ import annotations

import asyncio

import pytest
from agentflow import MemoryStore, SqliteStore, StoreConflict, StoreError
from agentflow.prebuilt import IdempotentOp


async def test_survives_a_restart(tmp_path):
    path = tmp_path / "s.db"
    await SqliteStore(path).put(("users", "u1"), "name", {"first": "Ада", "tags": ["a", "б"]})

    reopened = SqliteStore(path)  # a new object over the same file: "the process restarted"
    got = await reopened.get(("users", "u1"), "name")
    assert got is not None and got.value == {"first": "Ада", "tags": ["a", "б"]}
    assert got.namespace == ("users", "u1")


async def test_ttl_is_persisted_and_still_enforced_after_reopen(tmp_path):
    path = tmp_path / "s.db"
    await SqliteStore(path).put(("ns",), "k", "v", ttl=0.05)
    await asyncio.sleep(0.08)
    assert await SqliteStore(path).get(("ns",), "k") is None


async def test_if_absent_has_exactly_one_winner_across_store_objects(tmp_path):
    """Independent objects (separate connections) racing for one key: one claim wins."""
    path = tmp_path / "s.db"
    stores = [SqliteStore(path) for _ in range(12)]
    await stores[0].put(("warmup",), "x", 1)  # create the schema before the race

    async def claim(i: int) -> bool:
        try:
            await stores[i].put(("claims",), "job", {"worker": i}, if_absent=True)
            return True
        except StoreConflict:
            return False

    wins = await asyncio.gather(*[claim(i) for i in range(len(stores))])
    assert sum(wins) == 1
    winner = wins.index(True)
    assert (await stores[0].get(("claims",), "job")).value == {"worker": winner}


async def test_failed_conditional_write_leaves_the_live_value_untouched(tmp_path):
    s = SqliteStore(tmp_path / "s.db")
    await s.put(("ns",), "k", "first", if_absent=True)
    with pytest.raises(StoreConflict):
        await s.put(("ns",), "k", "second", if_absent=True)
    assert (await s.get(("ns",), "k")).value == "first"


async def test_ordering_matches_memory_store(tmp_path):
    """Same data, same prefix queries -> identical results, so a swap never reorders."""
    namespaces = [
        ("a",), ("a", "b"), ("a", "z"), ("ab",), ("a b",), ("a", "b", "c"),
        ("b",), ("A",), ("é",), ("a%",), ("a_",),
    ]  # fmt: skip
    mem, sql = MemoryStore(), SqliteStore(tmp_path / "s.db")
    for ns in namespaces:
        for key in ("k2", "k1", "K"):
            await mem.put(ns, key, [ns, key])
            await sql.put(ns, key, [list(ns), key])

    def view(items):
        return [(i.namespace, i.key) for i in items]

    for prefix in [(), ("a",), ("a", "b"), ("ab",), ("a%",), ("a_",), ("é",), ("none",)]:
        assert view(await sql.search(prefix, limit=1000)) == view(
            await mem.search(prefix, limit=1000)
        ), prefix
        assert await sql.list_namespaces(prefix=prefix, limit=1000) == await mem.list_namespaces(
            prefix=prefix, limit=1000
        ), prefix
    # pagination agrees too
    assert view(await sql.search((), limit=5, offset=7)) == view(
        await mem.search((), limit=5, offset=7)
    )


async def test_prefix_search_is_by_segment_not_by_pattern(tmp_path):
    s = SqliteStore(tmp_path / "s.db")
    for ns in [("a%",), ("ab",), ("a_",), ("a",), ("a", "b")]:
        await s.put(ns, "k", 1)
    assert await s.list_namespaces(prefix=("a%",)) == [("a%",)]  # '%' is not a wildcard
    assert await s.list_namespaces(prefix=("a_",)) == [("a_",)]  # '_' is not a wildcard
    assert await s.list_namespaces(prefix=("a",)) == [("a",), ("a", "b")]  # not ("ab",)


async def test_nul_in_a_segment_is_rejected(tmp_path):
    s = SqliteStore(tmp_path / "s.db")
    with pytest.raises(ValueError):
        await s.put(("a\x00b",), "k", 1)
    with pytest.raises(ValueError):
        await s.get(("a\x00b",), "k")
    with pytest.raises(ValueError):
        await s.search(("a\x00",))


async def test_unserializable_value_is_a_store_error_and_stores_nothing(tmp_path):
    s = SqliteStore(tmp_path / "s.db")
    with pytest.raises(StoreError):
        await s.put(("ns",), "k", {"tags": {"a", "b"}})  # a set is not JSON
    assert await s.get(("ns",), "k") is None


async def test_value_roundtrips_json_types(tmp_path):
    s = SqliteStore(tmp_path / "s.db")
    value = {"n": 1, "f": 1.5, "b": True, "none": None, "list": [1, [2]], "text": "日本語 ✓"}
    await s.put(("ns",), "k", value)
    assert (await s.get(("ns",), "k")).value == value


async def test_custom_table_isolates_data_and_rejects_unsafe_names(tmp_path):
    path = tmp_path / "s.db"
    one, two = SqliteStore(path, table="one"), SqliteStore(path, table="two")
    await one.put(("ns",), "k", 1)
    assert await two.get(("ns",), "k") is None
    with pytest.raises(ValueError):
        SqliteStore(path, table="x; drop table one")


async def test_creates_the_parent_directory(tmp_path):
    s = SqliteStore(tmp_path / "deep" / "er" / "s.db")
    await s.put(("ns",), "k", 1)
    assert (tmp_path / "deep" / "er" / "s.db").exists()


async def test_idempotent_op_marker_survives_a_restart(tmp_path):
    """The point of a durable store for IdempotentOp: after a restart a finished
    effect is recognised and not repeated."""
    path = tmp_path / "s.db"
    sent: list[str] = []

    async def send() -> str:
        sent.append("email")
        return "m-1"

    first = IdempotentOp(SqliteStore(path), ("effects", "email"))
    assert await first.run("welcome:u7", send) == "m-1"

    after_restart = IdempotentOp(SqliteStore(path), ("effects", "email"))  # new objects, same file
    assert await after_restart.run("welcome:u7", send) == "m-1"
    assert sent == ["email"]  # the effect ran once, across the "restart"


async def test_idempotent_op_in_flight_marker_survives_a_restart(tmp_path):
    """A crash inside the effect leaves in_flight on disk; the next process sees it."""
    from agentflow.prebuilt import IncompleteEffectError

    path = tmp_path / "s.db"

    async def crash() -> str:
        raise RuntimeError("worker died mid-effect")

    with pytest.raises(RuntimeError):
        await IdempotentOp(SqliteStore(path), ("e",)).run("k", crash)

    async def retry() -> str:
        return "ran again"

    with pytest.raises(IncompleteEffectError):  # default policy: ask, do not guess
        await IdempotentOp(SqliteStore(path), ("e",)).run("k", retry)
