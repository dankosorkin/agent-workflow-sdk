"""Static UI serving and the per-resource SSE event streams."""

from __future__ import annotations

import json
import time

from agentflow.server import workflow_app
from agentflow.server.workflow_app import create_workflow_app
from agentflow.workflow.service import WorkflowService
from fastapi.testclient import TestClient
from test_design import BRIEF, NO_ISSUES, Designer, good_plan_obj, j
from wf_helpers import FakeRunner, make_inventory

UI_DIR = workflow_app.Path(workflow_app.__file__).resolve().parents[2] / "ui-mockup-demo"


def build(tmp_path, designer, runner):
    service = WorkflowService(
        tmp_path, inventory_source=make_inventory, designer=designer, runner=runner
    )
    return service, TestClient(create_workflow_app(service, ui_dir=UI_DIR))


def good_runner():
    return FakeRunner(
        {
            "collect": ['{"logs": [{"id": 1}]}'],
            "analyze": ['{"findings": [{"title": "spike"}]}'],
        }
    )


def wait_for(client, url, field, wanted, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        body = client.get(url).json()
        if body[field] in wanted:
            return body
        time.sleep(0.02)
    raise AssertionError(f"{url}: {field} never became one of {wanted}; last {body}")


def design_done(client, request="find anomalies"):
    created = client.post("/designs", json={"request": request})
    assert created.status_code == 201
    return wait_for(client, f"/designs/{created.json()['id']}", "status", {"done", "failed"})


def _frames(client, url, timeout=5.0):
    """Collect `data:` frames from an SSE stream, bounded by a wall-clock timeout."""
    frames: list[dict] = []
    end = time.time() + timeout
    with client.stream("GET", url) as response:
        assert response.status_code == 200, response
        assert response.headers["content-type"].startswith("text/event-stream")
        for line in response.iter_lines():
            if time.time() > end:
                raise AssertionError(f"{url}: stream did not close within {timeout}s")
            if line.startswith("data:"):
                frames.append(json.loads(line[len("data:") :].strip()))
    return frames


def test_the_ui_index_is_served(tmp_path):
    _, client = build(tmp_path, Designer(orchestrator=[], creator=[], verifier=[]), good_runner())
    with client:
        response = client.get("/ui/index.html")
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/html")


def test_the_root_redirects_to_the_ui(tmp_path):
    _, client = build(tmp_path, Designer(orchestrator=[], creator=[], verifier=[]), good_runner())
    with client:
        response = client.get("/", follow_redirects=False)
        assert response.status_code in (307, 308)
        assert response.headers["location"] == "/ui/index.html"

        followed = client.get("/", follow_redirects=True)
        assert followed.status_code == 200
        assert followed.headers["content-type"].startswith("text/html")


def test_an_unknown_id_yields_404_before_streaming(tmp_path):
    _, client = build(tmp_path, Designer(orchestrator=[], creator=[], verifier=[]), good_runner())
    with client:
        assert client.get("/designs/nope/events").status_code == 404
        assert client.get("/runs/nope/events").status_code == 404


def test_design_events_stream_to_a_terminal_status(tmp_path):
    d = Designer(orchestrator=[BRIEF], creator=[j(good_plan_obj())], verifier=[NO_ISSUES])
    _, client = build(tmp_path, d, good_runner())
    with client:
        created = client.post("/designs", json={"request": "find anomalies"}).json()
        frames = _frames(client, f"/designs/{created['id']}/events")
        assert frames and isinstance(frames[-1], dict)
        assert frames[-1]["status"] in {"done", "failed", "interrupted"}


def test_run_events_stream_to_completion(tmp_path):
    d = Designer(orchestrator=[BRIEF], creator=[j(good_plan_obj())], verifier=[NO_ISSUES])
    _, client = build(tmp_path, d, good_runner())
    with client:
        plan_id = design_done(client)["plan_id"]
        client.post(f"/plans/{plan_id}/approve", json={"by": "daniel"})
        run = client.post(f"/plans/{plan_id}/runs", json={"params": {"window": "24h"}}).json()
        frames = _frames(client, f"/runs/{run['id']}/events")
        assert frames and frames[-1]["status"] == "completed", frames[-1] if frames else frames
