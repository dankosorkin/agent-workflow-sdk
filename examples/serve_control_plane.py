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

"""Drive the control plane over HTTP.

Run: python examples/serve_control_plane.py
Needs the server extra: pip install 'agent-workflow-sdk[server]'

``create_app(queue)`` returns a FastAPI ASGI app that maps 1:1 onto the run
queue: POST /runs enqueues, GET /runs[/{id}] observes, resume/cancel manage,
/stats and /healthz monitor. The app adds no behavior of its own — every route
forwards to one queue method.

To keep this offline and self-contained, it exercises the app through FastAPI's
TestClient (no socket, no running server). In a real deployment you would skip
the client and serve the module-level ``app`` with an ASGI server instead:

    uvicorn examples.serve_control_plane:app --port 8000

and a worker (usually a separate process) would drain the same queue. Here the
queue is a MemoryRunQueue, so there is nothing to actually execute the run — we
only demonstrate the HTTP surface and the run lifecycle up to "queued".
"""

from __future__ import annotations

from agentflow import MemoryRunQueue, PoolHealth
from agentflow.server import create_app

# A module-level ASGI app, so `uvicorn examples.serve_control_plane:app` works.
# pool_health is optional; here it is a static stand-in for a WorkerPool's
# health, which /healthz surfaces as a readiness signal.
queue = MemoryRunQueue()
app = create_app(queue, pool_health=lambda: PoolHealth(workers=4, alive=4, busy=1))


def main() -> None:
    try:
        from fastapi.testclient import TestClient
    except ModuleNotFoundError:
        raise SystemExit(
            "this example needs the server extra: pip install 'agent-workflow-sdk[server]'"
        ) from None

    client = TestClient(app)

    print("POST /runs")
    created = client.post("/runs", json={"graph": "summarize", "input": {"text": "..."}})
    run = created.json()
    print(f"  -> {created.status_code} {run['status']} id={run['run_id']}")

    print("GET /runs/{id}")
    got = client.get(f"/runs/{run['run_id']}").json()
    print(f"  -> status={got['status']} graph={got['graph']}")

    print("GET /runs?graph=summarize")
    listed = client.get("/runs", params={"graph": "summarize"}).json()
    print(f"  -> {len(listed)} run(s)")

    print("GET /stats")
    stats = client.get("/stats").json()
    print(f"  -> total={stats['total']} by_status={stats['by_status']}")

    print("GET /healthz")
    print(f"  -> {client.get('/healthz').json()}")

    print("POST /runs/{id}/cancel")
    rid = run["run_id"]
    cancelled = client.post(f"/runs/{rid}/cancel").json()
    print(f"  -> cancelled={cancelled['cancelled']}")
    now = client.get(f"/runs/{rid}").json()["status"]
    print(f"  -> status now: {now}")

    print("\ninteractive OpenAPI docs would be at /docs; schema at /openapi.json")


if __name__ == "__main__":
    main()
