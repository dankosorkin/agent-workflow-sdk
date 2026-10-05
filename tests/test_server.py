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

"""HTTP control-plane server tests against a MemoryRunQueue (hermetic).

Uses FastAPI's TestClient, so no network and no running server — the whole
suite stays offline. Covers every exposed route plus the 404/validation paths,
and asserts the worker protocol is NOT reachable over HTTP.
"""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from agentflow import MemoryRunQueue, PoolHealth  # noqa: E402
from agentflow.server import create_app  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402


def _client(pool_health=None):
    return TestClient(create_app(MemoryRunQueue(), pool_health=pool_health))


# --- enqueue / get / list --------------------------------------------------


def test_enqueue_returns_201_queued():
    c = _client()
    r = c.post("/runs", json={"graph": "g", "input": {"n": 1}})
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "queued"
    assert body["graph"] == "g"
    assert body["input"] == {"n": 1}
    assert body["thread"] == body["run_id"]  # thread defaults to run id


def test_enqueue_with_explicit_thread():
    c = _client()
    r = c.post("/runs", json={"graph": "g", "thread": "t-1"})
    assert r.json()["thread"] == "t-1"


def test_enqueue_requires_graph():
    c = _client()
    r = c.post("/runs", json={"input": {}})  # missing required 'graph'
    assert r.status_code == 422  # FastAPI request validation


def test_get_run_roundtrip():
    c = _client()
    rid = c.post("/runs", json={"graph": "g"}).json()["run_id"]
    r = c.get(f"/runs/{rid}")
    assert r.status_code == 200
    assert r.json()["run_id"] == rid


def test_get_missing_run_404():
    c = _client()
    r = c.get("/runs/nope")
    assert r.status_code == 404
    assert "nope" in r.json()["detail"]


def test_list_and_filter():
    c = _client()
    c.post("/runs", json={"graph": "a"})
    c.post("/runs", json={"graph": "b"})
    assert len(c.get("/runs").json()) == 2
    only_a = c.get("/runs", params={"graph": "a"}).json()
    assert len(only_a) == 1 and only_a[0]["graph"] == "a"


def test_list_pagination_bounds():
    c = _client()
    for _ in range(3):
        c.post("/runs", json={"graph": "g"})
    assert len(c.get("/runs", params={"limit": 2}).json()) == 2
    # limit must be within bounds
    assert c.get("/runs", params={"limit": 0}).status_code == 422
    assert c.get("/runs", params={"limit": 99999}).status_code == 422


# --- resume / cancel -------------------------------------------------------


def test_cancel_queued_run():
    c = _client()
    rid = c.post("/runs", json={"graph": "g"}).json()["run_id"]
    r = c.post(f"/runs/{rid}/cancel")
    assert r.status_code == 200 and r.json()["cancelled"] is True
    assert c.get(f"/runs/{rid}").json()["status"] == "cancelled"


def test_cancel_unknown_returns_false():
    c = _client()
    r = c.post("/runs/nope/cancel")
    assert r.status_code == 200 and r.json()["cancelled"] is False


def test_resume_requeues_run():
    c = _client()
    rid = c.post("/runs", json={"graph": "g"}).json()["run_id"]
    r = c.post(f"/runs/{rid}/resume", json={"value": "approved"})
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "queued"
    assert body["resume_value"] == "approved"


def test_resume_missing_run_404():
    c = _client()
    r = c.post("/runs/nope/resume", json={"value": 1})
    assert r.status_code == 404


# --- monitoring ------------------------------------------------------------


def test_stats_counts_by_status():
    c = _client()
    c.post("/runs", json={"graph": "g"})
    c.post("/runs", json={"graph": "g"})
    stats = c.get("/stats").json()
    assert stats["total"] == 2
    assert stats["by_status"]["queued"] == 2


def test_healthz_plain():
    c = _client()
    assert c.get("/healthz").json() == {"status": "ok"}


def test_healthz_with_pool_health():
    c = _client(pool_health=lambda: PoolHealth(workers=4, alive=4, busy=1))
    body = c.get("/healthz").json()
    assert body["status"] == "ok"
    assert body["pool"]["workers"] == 4
    assert body["pool"]["idle"] == 3
    assert body["pool"]["healthy"] is True


# --- surface guarantees ----------------------------------------------------


def test_worker_protocol_is_not_exposed():
    # claim / heartbeat / complete mutate run state and must not be reachable
    # over HTTP — workers talk to the queue directly, not through this API.
    c = _client()
    paths = set(c.get("/openapi.json").json()["paths"])
    for leaked in ("/claim", "/runs/x/claim", "/runs/x/heartbeat", "/runs/x/complete"):
        assert leaked not in paths
    # the intended surface is present
    assert "/runs" in paths and "/stats" in paths and "/healthz" in paths


def test_openapi_served():
    c = _client()
    assert c.get("/openapi.json").status_code == 200
