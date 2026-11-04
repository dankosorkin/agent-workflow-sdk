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


MOCK_LEFTOVERS = (
    "a72f19c",
    "Java version audit",
    "Dependency update",
    "fictional mock",
    "nothing is connected",
    "Find all services in ~/Development",
)


def test_the_ui_carries_no_leftover_mock_data():
    for path in sorted(UI_DIR.glob("*.html")):
        text = path.read_text()
        found = [m for m in MOCK_LEFTOVERS if m in text]
        assert not found, f"{path.name} still contains mock data: {found}"


def test_the_create_screen_sends_only_what_the_server_reads():
    # A control that is sent but ignored is decorative (the old rounds/mode toggles were).
    text = (UI_DIR / "Create.dc.html").read_text()
    assert "max_rounds" not in text and "max_design_rounds" not in text
    # what the form sends is exactly what POST /designs reads
    assert "{ request, system_backend: this.state.system }" in text
    # the executing backend is chosen at launch, not at design time
    assert "extChoices" not in text and "pickBackend" not in text
    main = (UI_DIR / "Main.dc.html").read_text()
    assert (
        "backend: this.state.backend, extensions: this._selection(), mode: this.state.mode" in main
    )


def test_the_create_screen_can_resume_a_design_from_the_url():
    # A design left waiting for an answer must be reachable again after leaving the tab.
    text = (UI_DIR / "Create.dc.html").read_text()
    assert ".get('design')" in text and "AF.get('/designs')" in text


def test_the_plans_screen_has_an_empty_state_instead_of_a_dead_approve_button():
    text = (UI_DIR / "Main.dc.html").read_text()
    assert "There is no plan to approve yet" in text and "{{hasPlan}}" in text


def test_a_design_and_a_run_keep_an_activity_log(tmp_path):
    d = Designer(orchestrator=[BRIEF], creator=[j(good_plan_obj())], verifier=[NO_ISSUES])
    _, client = build(tmp_path, d, good_runner())
    with client:
        design = design_done(client)
        events = client.get(f"/designs/{design['id']}").json()["events"]
        stages = {(e["role"], e["state"]) for e in events if e["kind"] == "stage"}
        assert {
            ("orchestrator", "started"),
            ("creator", "finished"),
            ("verifier", "finished"),
        } <= stages
        assert events[0]["kind"] == "design" and events[0]["state"] == "started"
        assert events[-1]["kind"] == "design" and events[-1]["state"] == "done"

        plan_id = design["plan_id"]
        client.post(f"/plans/{plan_id}/approve", json={"by": "daniel"})
        run = client.post(f"/plans/{plan_id}/runs", json={"params": {"window": "24h"}}).json()
        wait_for(client, f"/runs/{run['id']}", "status", {"completed"})
        log = client.get(f"/runs/{run['id']}").json()["events"]
        steps = {(e["step"], e["state"]) for e in log if e["kind"] == "step"}
        assert {("collect", "finished"), ("analyze", "finished")} <= steps
        assert log[-1]["kind"] == "run" and log[-1]["state"] == "completed"


def test_the_activity_log_is_observation_only(tmp_path):
    # The plain record is unchanged by the log: the log lives apart from the record.
    d = Designer(orchestrator=[BRIEF], creator=[j(good_plan_obj())], verifier=[NO_ISSUES])
    service, client = build(tmp_path, d, good_runner())
    with client:
        design = design_done(client)
        assert "events" not in service.get_design(design["id"])
        assert service.events(design["id"])


def test_a_stream_sends_a_heartbeat_while_nothing_changes(tmp_path, monkeypatch):
    import asyncio

    class Slow:
        """A designer that is still thinking: the record does not change for a while."""

        async def __call__(self, role, prompt):
            await asyncio.sleep(2.5)
            return BRIEF

    _, client = build(tmp_path, Slow(), good_runner())
    with client:
        created = client.post("/designs", json={"request": "x"}).json()
        seen = []
        end = time.time() + 6
        with client.stream("GET", f"/designs/{created['id']}/events") as response:
            for line in response.iter_lines():
                seen.append(line)
                if line.startswith("event: tick") or time.time() > end:
                    break
        assert any(line.startswith("event: tick") for line in seen), seen


class AgentBackend:
    """A backend that behaves like a real agent: text, a tool call, a permission, usage."""

    def __init__(self, answer: str) -> None:
        self.answer = answer

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def prompt(self, prompt):
        from agentflow.events import (
            PermissionRequest,
            TextChunk,
            ToolCall,
            ToolResult,
            TurnEnd,
            Usage,
        )

        yield TextChunk(text="thin")
        yield TextChunk(text="king...")
        yield ToolCall(id="t1", name="read_file", args={"path": "a.log"})
        yield ToolResult(id="t1", status="ok", content="10 lines")
        yield PermissionRequest(id="p1", tool="write_file", options=())
        yield Usage(input_tokens=5, output_tokens=7, total_tokens=12, cost_usd=0.001)
        yield TurnEnd(text=self.answer, stop_reason="end_turn")


def _agent_service(tmp_path):
    from agentflow.workflow.runner import collect_text

    answers = {
        "collect": '{"logs": [{"id": 1}]}',
        "analyze": '{"findings": [{"title": "spike"}]}',
        "orchestrator": BRIEF,
        "creator": j(good_plan_obj()),
        "verifier": NO_ISSUES,
    }

    async def designer(role, prompt):
        return await collect_text(AgentBackend(answers[role]), prompt)

    async def runner(step, entry, prompt):
        return await collect_text(AgentBackend(answers[step["id"]]), prompt)

    service = WorkflowService(
        tmp_path, inventory_source=make_inventory, designer=designer, runner=runner
    )
    return service, TestClient(create_workflow_app(service, ui_dir=UI_DIR))


def test_every_agent_event_of_a_run_is_logged_under_its_step(tmp_path):
    service, client = _agent_service(tmp_path)
    with client:
        plan_id = design_done(client)["plan_id"]
        client.post(f"/plans/{plan_id}/approve", json={"by": "daniel"})
        run = client.post(f"/plans/{plan_id}/runs", json={"params": {"window": "24h"}}).json()
        wait_for(client, f"/runs/{run['id']}", "status", {"completed"})

        entries = service.agent_events(run["id"])
        types = {e["type"] for e in entries}
        assert {"text", "tool_call", "tool_result", "permission", "usage", "turn_end"} <= types
        assert {e["step"] for e in entries} == {"collect", "analyze"}
        assert all(e["backend"] for e in entries)
        # Consecutive chunks are joined into one entry; nothing is cut.
        assert [e["text"] for e in entries if e["type"] == "text"][0] == "thinking..."
        call = next(e for e in entries if e["type"] == "tool_call")
        assert call["name"] == "read_file" and call["args"] == {"path": "a.log"}
        assert [e["seq"] for e in entries] == sorted(e["seq"] for e in entries)


def test_every_agent_event_of_a_design_is_logged_under_its_role(tmp_path):
    service, client = _agent_service(tmp_path)
    with client:
        design = design_done(client)
        roles = {e["role"] for e in service.agent_events(design["id"])}
        assert roles == {"orchestrator", "creator", "verifier"}


def test_the_agent_stream_replays_and_ends_with_the_run(tmp_path):
    service, client = _agent_service(tmp_path)
    with client:
        plan_id = design_done(client)["plan_id"]
        client.post(f"/plans/{plan_id}/approve", json={"by": "daniel"})
        run = client.post(f"/plans/{plan_id}/runs", json={"params": {"window": "24h"}}).json()
        wait_for(client, f"/runs/{run['id']}", "status", {"completed"})

        frames = _frames(client, f"/runs/{run['id']}/agent-stream")
        assert [f["seq"] for f in frames] == [e["seq"] for e in service.agent_events(run["id"])]
        after = frames[2]["seq"]
        assert [
            f["seq"] for f in _frames(client, f"/runs/{run['id']}/agent-stream?after={after}")
        ] == [f["seq"] for f in frames if f["seq"] > after]
        assert client.get("/runs/nope/agent-stream").status_code == 404


async def test_an_observer_that_raises_never_breaks_the_agent_call():
    from agentflow.workflow.runner import collect_text, observing

    def broken(ev):
        raise RuntimeError("observer bug")

    with observing(broken):
        assert await collect_text(AgentBackend("answer"), "p") == "answer"
    assert await collect_text(AgentBackend("answer"), "p") == "answer"  # no observer outside
