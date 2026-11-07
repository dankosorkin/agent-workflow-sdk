"""The local HTTP service, end to end with scripted designers and agents."""

from __future__ import annotations

import json
import time

import pytest
from agentflow.server.workflow_app import create_workflow_app
from agentflow.workflow.service import WorkflowService
from fastapi.testclient import TestClient
from test_design import BRIEF, NO_ISSUES, QUESTIONS, Designer, good_plan_obj, j
from wf_helpers import GOOD_COLLECT, FakeRunner, make_inventory

ASK = '{"needs_clarification": {"question": "Which env?", "options": ["staging", "prod"]}}'


def build(tmp_path, designer, runner):
    service = WorkflowService(
        tmp_path, inventory_source=make_inventory, designer=designer, runner=runner
    )
    return service, TestClient(create_workflow_app(service))


def wait_for(client, url, field, wanted, timeout=30.0):
    end = time.time() + timeout
    while time.time() < end:
        body = client.get(url).json()
        if body[field] in wanted:
            return body
        time.sleep(0.02)
    raise AssertionError(f"{url}: {field} never became one of {wanted}; last {body}")


def good_runner():
    return FakeRunner(
        {
            "collect": ['{"logs": [{"id": 1}]}'],
            "analyze": ['{"findings": [{"title": "spike"}]}'],
        }
    )


def design_done(client, designer_request="find anomalies"):
    created = client.post("/designs", json={"request": designer_request})
    assert created.status_code == 201
    return wait_for(client, f"/designs/{created.json()['id']}", "status", {"done", "failed"})


def test_design_approve_run_to_completion(tmp_path):
    d = Designer(orchestrator=[BRIEF], creator=[j(good_plan_obj())], verifier=[NO_ISSUES])
    service, client = build(tmp_path, d, good_runner())
    with client:
        design = design_done(client)
        assert design["status"] == "done" and design["plan_id"]
        plan_id = design["plan_id"]

        # not approved yet: the run is refused, with the reason
        run = client.post(f"/plans/{plan_id}/runs", json={"params": {"window": "24h"}}).json()
        refused = wait_for(client, f"/runs/{run['id']}", "status", {"refused"})
        assert [i["code"] for i in refused["issues"]] == ["not_approved"]

        assert client.get(f"/plans/{plan_id}/issues").json() == []
        approved = client.post(f"/plans/{plan_id}/approve", json={"by": "daniel"}).json()
        assert approved["plan"]["approval"]["by"] == "daniel"

        run = client.post(f"/plans/{plan_id}/runs", json={"params": {"window": "24h"}}).json()
        done = wait_for(client, f"/runs/{run['id']}", "status", {"completed", "failed", "error"})
        assert done["status"] == "completed", done
        assert done["outcome"]["final_outputs"][0]["value"] == [{"title": "spike"}]
        assert {r["id"] for r in client.get("/runs").json()} >= {run["id"]}


def test_the_orchestrator_asks_through_the_api(tmp_path):
    d = Designer(
        orchestrator=[QUESTIONS, BRIEF], creator=[j(good_plan_obj())], verifier=[NO_ISSUES]
    )
    _, client = build(tmp_path, d, good_runner())
    with client:
        created = client.post("/designs", json={"request": "x"}).json()
        url = f"/designs/{created['id']}"
        asking = wait_for(client, url, "status", {"asking"})
        assert asking["question"]["questions"] == ["Which log files?", "How far back?"]

        assert client.post(url + "/answer", json={"answer": "app.log, 24h"}).status_code == 200
        # a second answer to the same question is a conflict, not a silent overwrite
        assert client.post(url + "/answer", json={"answer": "again"}).status_code == 409
        done = wait_for(client, url, "status", {"done", "failed"})
        assert done["status"] == "done"
        assert done["dialogue"][0]["answer"] == "app.log, 24h"


def test_a_run_waits_for_a_step_question_and_continues(tmp_path):
    plan = good_plan_obj()
    plan["graph"]["steps"][1]["may_ask"] = {"max_questions": 1}
    d = Designer(orchestrator=[BRIEF], creator=[j(plan)], verifier=[NO_ISSUES])
    runner = FakeRunner(
        {"collect": [GOOD_COLLECT], "analyze": [ASK, '{"findings": [{"title": "t"}]}']}
    )
    _, client = build(tmp_path, d, runner)
    with client:
        plan_id = design_done(client)["plan_id"]
        client.post(f"/plans/{plan_id}/approve", json={"by": "daniel"})
        run = client.post(f"/plans/{plan_id}/runs", json={"params": {"window": "24h"}}).json()
        url = f"/runs/{run['id']}"
        waiting = wait_for(client, url, "status", {"waiting", "completed", "failed", "error"})
        assert waiting["status"] == "waiting", waiting
        assert waiting["outcome"]["question"]["question"] == "Which env?"

        assert client.post(url + "/resume").status_code == 409  # waiting needs an answer
        assert client.post(url + "/answer", json={"answer": "prod"}).status_code == 200
        done = wait_for(client, url, "status", {"completed", "failed", "error"})
        assert done["status"] == "completed", done


def test_errors_are_plain_http_errors(tmp_path):
    d = Designer(orchestrator=[BRIEF], creator=[], verifier=[])
    _, client = build(tmp_path, d, good_runner())
    with client:
        assert client.get("/designs/nope").status_code == 404
        assert client.get("/plans/nope").status_code == 404
        assert client.post("/runs/nope/answer", json={"answer": "x"}).status_code == 404
        assert client.post("/designs", json={"request": "  "}).status_code == 422
        assert client.post("/plans/nope/approve", json={"by": "x"}).status_code == 404
        assert client.get("/inventory").json()["agents"].keys() == {"collector", "analyst"}


def test_unapprovable_plans_are_refused_with_the_issues(tmp_path):
    bad = good_plan_obj()
    bad["graph"]["steps"][0]["checks"] = [{"kind": "count", "target": "ghost", "min": 1}]
    d = Designer(orchestrator=[BRIEF], creator=[j(bad)] * 3, verifier=[])
    _, client = build(tmp_path, d, good_runner())
    with client:
        design = design_done(client)
        assert design["result"]["status"] == "not_executable"
        plan_id = design["plan_id"]
        codes = [i["code"] for i in client.get(f"/plans/{plan_id}/issues").json()]
        assert "bad_check" in codes
        response = client.post(f"/plans/{plan_id}/approve", json={"by": "daniel"})
        assert response.status_code == 422


def test_a_restart_marks_unfinished_work_interrupted_and_a_run_can_resume(tmp_path):
    d = Designer(orchestrator=[QUESTIONS], creator=[], verifier=[])
    service, client = build(tmp_path, d, good_runner())
    with client:
        created = client.post("/designs", json={"request": "x"}).json()
        wait_for(client, f"/designs/{created['id']}", "status", {"asking"})

    # a new process over the same data directory
    d2 = Designer(orchestrator=[], creator=[], verifier=[])
    again, client2 = build(tmp_path, d2, good_runner())
    with client2:
        design = client2.get(f"/designs/{created['id']}").json()
        assert design["status"] == "interrupted" and "restarted" in design["error"]


def test_records_survive_a_restart(tmp_path):
    d = Designer(orchestrator=[BRIEF], creator=[j(good_plan_obj())], verifier=[NO_ISSUES])
    _, client = build(tmp_path, d, good_runner())
    with client:
        plan_id = design_done(client)["plan_id"]
    _, client2 = build(tmp_path, Designer(orchestrator=[], creator=[], verifier=[]), good_runner())
    with client2:
        assert client2.get(f"/plans/{plan_id}").json()["plan"]["graph"]["steps"]
        assert json.dumps(client2.get("/designs").json())


@pytest.mark.parametrize("path", ["/designs", "/plans", "/runs"])
def test_lists_start_empty(tmp_path, path):
    _, client = build(tmp_path, Designer(orchestrator=[], creator=[], verifier=[]), good_runner())
    with client:
        assert client.get(path).json() == []
