"""Cross-backend contract tests: revision tracking, CAS (if_revision), and
retention (delete_thread / prune) for the offline checkpointers.

Redis is covered separately (fakeredis) in test_redis_checkpointer.py; Postgres
has its own live test. These three run everywhere with no external service.
"""

from __future__ import annotations

import pytest
from agentflow import FileCheckpointer, MemoryCheckpointer, SqliteCheckpointer, ThreadInfo
from agentflow.checkpoint.base import Checkpoint
from agentflow.errors import CheckpointConflict


def _make(kind, tmp_path):
    if kind == "memory":
        return MemoryCheckpointer()
    if kind == "sqlite":
        return SqliteCheckpointer(tmp_path / "cp.db")
    if kind == "file":
        return FileCheckpointer(tmp_path / "runs")
    raise AssertionError(kind)


BACKENDS = ["memory", "sqlite", "file"]


@pytest.mark.parametrize("kind", BACKENDS)
async def test_revision_starts_at_one_and_bumps(kind, tmp_path):
    cp = _make(kind, tmp_path)
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 1}))
    assert (await cp.get("t", 1)).revision == 1
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 2}))  # overwrite
    got = await cp.get("t", 1)
    assert got.revision == 2 and got.state["n"] == 2


@pytest.mark.parametrize("kind", BACKENDS)
async def test_if_revision_zero_on_first_write(kind, tmp_path):
    cp = _make(kind, tmp_path)
    # Nothing stored yet -> expected revision 0 must succeed.
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 1}), if_revision=0)
    assert (await cp.get("t", 1)).revision == 1


@pytest.mark.parametrize("kind", BACKENDS)
async def test_if_revision_match_succeeds(kind, tmp_path):
    cp = _make(kind, tmp_path)
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 1}))
    rev = (await cp.get("t", 1)).revision
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 2}), if_revision=rev)
    assert (await cp.get("t", 1)).state["n"] == 2


@pytest.mark.parametrize("kind", BACKENDS)
async def test_if_revision_mismatch_raises_conflict(kind, tmp_path):
    cp = _make(kind, tmp_path)
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 1}))  # revision now 1
    with pytest.raises(CheckpointConflict) as ei:
        await cp.put(Checkpoint(thread="t", step=1, state={"n": 9}), if_revision=0)
    assert ei.value.expected == 0 and ei.value.actual == 1
    # The losing write must not have applied.
    assert (await cp.get("t", 1)).state["n"] == 1


@pytest.mark.parametrize("kind", BACKENDS)
async def test_racing_resume_one_wins(kind, tmp_path):
    """Two resumes both read revision 1 and try to write; second must conflict."""
    cp = _make(kind, tmp_path)
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 1}))
    base = (await cp.get("t", 1)).revision
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 2}), if_revision=base)
    with pytest.raises(CheckpointConflict):
        await cp.put(Checkpoint(thread="t", step=1, state={"n": 3}), if_revision=base)


@pytest.mark.parametrize("kind", BACKENDS)
async def test_delete_thread(kind, tmp_path):
    cp = _make(kind, tmp_path)
    for i in range(1, 4):
        await cp.put(Checkpoint(thread="t", step=i, state={"n": i}))
    await cp.delete_thread("t")
    assert await cp.get("t") is None
    assert [c async for c in cp.history("t")] == []


@pytest.mark.parametrize("kind", BACKENDS)
async def test_prune_before_step(kind, tmp_path):
    cp = _make(kind, tmp_path)
    for i in range(1, 6):
        await cp.put(Checkpoint(thread="t", step=i, state={"n": i}))
    removed = await cp.prune("t", before_step=3)
    assert removed == 2
    steps = [c.step async for c in cp.history("t")]
    assert steps == [3, 4, 5]


@pytest.mark.parametrize("kind", BACKENDS)
async def test_prune_older_than(kind, tmp_path):
    cp = _make(kind, tmp_path)
    await cp.put(Checkpoint(thread="t", step=1, state={"n": 1}, ts="2020-01-01T00:00:00"))
    await cp.put(Checkpoint(thread="t", step=2, state={"n": 2}, ts="2030-01-01T00:00:00"))
    removed = await cp.prune("t", older_than="2025-01-01T00:00:00")
    assert removed == 1
    steps = [c.step async for c in cp.history("t")]
    assert steps == [2]


@pytest.mark.parametrize("kind", BACKENDS)
async def test_list_threads_empty(kind, tmp_path):
    cp = _make(kind, tmp_path)
    assert await cp.list_threads() == []


@pytest.mark.parametrize("kind", BACKENDS)
async def test_list_threads_summarizes_latest(kind, tmp_path):
    cp = _make(kind, tmp_path)
    # thread "a": two steps, latest not done (has frontier)
    await cp.put(Checkpoint(thread="a", step=1, state={"n": 1}, next=("x",)))
    await cp.put(Checkpoint(thread="a", step=2, state={"n": 2}, next=("y",), ts="2030"))
    # thread "b": finished (no frontier, not interrupted)
    await cp.put(Checkpoint(thread="b", step=1, state={"n": 1}, next=()))
    # thread "c": interrupted
    await cp.put(Checkpoint(thread="c", step=1, state={"n": 1}, next=("z",), interrupted=True))
    infos = await cp.list_threads()
    assert all(isinstance(i, ThreadInfo) for i in infos)
    by = {i.thread: i for i in infos}
    assert set(by) == {"a", "b", "c"}
    assert by["a"].latest_step == 2 and by["a"].done is False and by["a"].ts == "2030"
    assert by["b"].done is True and by["b"].interrupted is False
    assert by["c"].interrupted is True and by["c"].done is False


@pytest.mark.parametrize("kind", BACKENDS)
async def test_list_threads_pagination_and_order(kind, tmp_path):
    cp = _make(kind, tmp_path)
    for name in ("t3", "t1", "t2"):
        await cp.put(Checkpoint(thread=name, step=1, state={"n": 1}))
    infos = await cp.list_threads()
    assert [i.thread for i in infos] == ["t1", "t2", "t3"]  # sorted by thread
    page = await cp.list_threads(limit=1, offset=1)
    assert [i.thread for i in page] == ["t2"]


@pytest.mark.parametrize("kind", BACKENDS)
async def test_delete_thread_drops_from_listing(kind, tmp_path):
    cp = _make(kind, tmp_path)
    await cp.put(Checkpoint(thread="a", step=1, state={"n": 1}))
    await cp.put(Checkpoint(thread="b", step=1, state={"n": 1}))
    await cp.delete_thread("a")
    assert [i.thread for i in await cp.list_threads()] == ["b"]
