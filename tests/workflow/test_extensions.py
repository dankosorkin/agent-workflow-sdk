"""Discovering what a backend can load, and proving that it really does."""

from __future__ import annotations

import time

import pytest
from agentflow.server.workflow_app import create_workflow_app
from agentflow.workflow import extensions
from agentflow.workflow.extensions import (
    discover,
    fingerprint,
    parse_front_matter,
    probe_backend,
)
from agentflow.workflow.service import WorkflowService
from fastapi.testclient import TestClient
from test_design import Designer
from test_workflow_ui import UI_DIR
from wf_helpers import FakeRunner, make_inventory


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_front_matter_gives_scalars_and_the_body():
    meta, body = parse_front_matter('---\nname: x\ndescription: "a b"\ntools:\n  - r\n---\n# Hi\n')
    assert meta == {"name": "x", "description": "a b"} and body.strip() == "# Hi"
    assert parse_front_matter("# no front matter") == ({}, "# no front matter")
    assert parse_front_matter("---\nname: never closed\n") == ({}, "---\nname: never closed\n")


def test_front_matter_understands_block_scalars():
    text = "---\nname: mongo\ndescription: >-\n  Connect to Mongo\n  and run queries.\nmodel: x\n---\nbody"
    meta, body = parse_front_matter(text)
    assert meta == {
        "name": "mongo",
        "description": "Connect to Mongo and run queries.",
        "model": "x",
    }
    assert body == "body"
    literal, _ = parse_front_matter("---\ndescription: |\n  line one\n  line two\n---\n")
    assert literal["description"] == "line one\nline two"


@pytest.fixture
def machine(tmp_path):
    home, project = tmp_path / "home", tmp_path / "project"
    write(home / ".kiro/steering/java.md", "---\ninclusion: manual\n---\n# Java rules\nbody")
    write(home / ".kiro/steering/old.md.bak", "ignored")
    write(
        home / ".kiro/skills/api-design/SKILL.md",
        "---\nname: api-design\ndescription: Design APIs\n---\nx",
    )
    write(project / ".kiro/steering/java.md", "---\ninclusion: always\n---\n# Project Java rules")
    write(project / "AGENTS.md", "# project instructions")
    write(home / ".claude/CLAUDE.md", "# my claude rules")
    write(home / ".codex/AGENTS.md", "Act as an executor.")
    return home, project


def test_discover_lists_names_and_descriptions_never_content(machine):
    home, project = machine
    found = discover("kiro", project, home=home)
    assert [e.name for e in found["skills"]] == ["api-design"]
    assert found["skills"][0].description == "Design APIs"
    steering = {e.name: e for e in found["steering"]}
    assert set(steering) == {"java"}  # the .bak file is skipped
    # a project file overrides the user's file of the same name
    assert steering["java"].scope == "project" and steering["java"].inclusion == "always"
    assert found["instructions"][0].name == "AGENTS.md (project)"
    assert not any(hasattr(e, "content") for e in found["steering"])


def test_each_backend_looks_where_it_really_looks(machine):
    home, project = machine
    assert [e.name for e in discover("claude_code", project, home=home)["instructions"]] == [
        "CLAUDE.md (user)"
    ]
    codex = discover("codex", project, home=home)
    assert {e.name for e in codex["instructions"]} == {"AGENTS.md (user)", "AGENTS.md (project)"}
    assert discover("codex", project, home=home)["steering"] == []
    with pytest.raises(KeyError):
        discover("nope", project, home=home)


def test_a_fingerprint_changes_when_a_file_does(machine):
    home, project = machine
    before = fingerprint(discover("kiro", project, home=home)["steering"])
    assert before == fingerprint(discover("kiro", project, home=home)["steering"])
    write(project / ".kiro/steering/java.md", "---\ninclusion: always\n---\n# Changed")
    assert fingerprint(discover("kiro", project, home=home)["steering"]) != before


def test_available_backends_reports_missing_executables(monkeypatch):
    monkeypatch.setattr(extensions.shutil, "which", lambda b: "/bin/true" if b == "codex" else None)
    monkeypatch.setattr(extensions, "_run_version", lambda path: "v1")
    by_name = {b["backend"]: b for b in extensions.available_backends()}
    assert by_name["codex"]["available"] and by_name["codex"]["version"] == "v1"
    assert not by_name["kiro"]["available"] and by_name["kiro"]["path"] is None


def obeying(tmp):
    """A fake agent that does what a good backend does: apply the files it finds in its cwd."""

    async def run(backend, cwd, prompt):
        text = " ".join(p.read_text() for p in cwd.rglob("*") if p.is_file())
        if "canary phrase" in prompt:
            return next(w for w in text.replace(".", " ").split() if w.startswith("PHRASE-"))
        return "ready " + next(w for w in text.replace(".", " ").split() if w.startswith("CANARY-"))

    return run


async def deaf(backend, cwd, prompt):
    return "ready"


async def test_a_backend_that_applies_the_files_passes_the_probe(tmp_path):
    results = {r.kind: r for r in await probe_backend("kiro", runner=obeying(tmp_path))}
    assert results["steering"].status == "ok" and results["skills"].status == "ok"
    assert results["instructions"].status == "not_probed"


async def test_a_backend_that_ignores_the_files_is_reported_as_such(tmp_path):
    results = {r.kind: r for r in await probe_backend("claude_code", runner=deaf)}
    assert (
        results["instructions"].status == "missing"
        and "not applied" in results["instructions"].detail
    )
    assert results["skills"].status == "missing"


async def test_codex_has_no_project_skills_so_they_are_not_probed():
    results = {r.kind: r for r in await probe_backend("codex", runner=obeying(None))}
    assert results["instructions"].status == "ok" and results["skills"].status == "not_probed"


async def test_a_failing_agent_is_an_error_not_a_crash():
    async def broken(backend, cwd, prompt):
        raise RuntimeError("cli not logged in")

    results = await probe_backend("codex", runner=broken)
    assert any(r.status == "error" and "cli not logged in" in r.detail for r in results)


async def test_the_probe_cleans_up_its_temporary_directory():
    seen = []

    async def spy(backend, cwd, prompt):
        seen.append(cwd)
        return ""

    await probe_backend("claude_code", runner=spy)
    assert seen and not seen[0].exists()


def make_client(tmp_path, workspace, runner):
    service = WorkflowService(
        tmp_path / "data",
        inventory_source=make_inventory,
        designer=Designer(orchestrator=[], creator=[], verifier=[]),
        runner=FakeRunner({}),
    )
    app = create_workflow_app(service, ui_dir=UI_DIR, workspace=workspace, probe_runner=runner)
    return TestClient(app)


def test_the_backends_endpoints(tmp_path, monkeypatch):
    project = tmp_path / "project"
    write(project / ".kiro/steering/rules.md", "---\ninclusion: manual\n---\n# Rules")
    monkeypatch.setattr(extensions.Path, "home", staticmethod(lambda: tmp_path / "nohome"))
    client = make_client(tmp_path, project, obeying(tmp_path))
    with client:
        names = {b["backend"] for b in client.get("/backends").json()}
        assert names == {"kiro", "claude_code", "codex"}

        body = client.get("/backends/kiro/extensions").json()
        assert body["workspace"] == str(project.resolve())
        assert [e["name"] for e in body["extensions"]["steering"]] == ["rules"]
        assert client.get("/backends/nope/extensions").status_code == 404

        assert client.get("/backends/kiro/probe").json()["status"] == "never"
        started = client.post("/backends/kiro/probe")
        assert started.status_code == 202
        end = time.time() + 5
        state = client.get("/backends/kiro/probe").json()
        while state["status"] == "running" and time.time() < end:
            time.sleep(0.02)
            state = client.get("/backends/kiro/probe").json()
        assert state["status"] == "done"
        assert {r["kind"]: r["status"] for r in state["results"]}["steering"] == "ok"
        assert client.post("/backends/nope/probe").status_code == 404


async def test_kiro_steps_may_load_skills_but_nothing_else_by_default():
    from agentflow.events import PermissionRequest
    from agentflow.workflow.runner import make_backend

    entry = {"backend": "kiro", "agent": "vibe"}

    def decides(step, tool):
        policy = make_backend(entry, step).permission
        return policy.decide(PermissionRequest(id="1", tool=tool))

    ro = {"id": "s", "permission": "read_only"}
    assert type(await decides(ro, "disclose_context")).__name__ == "Allow"
    assert type(await decides(ro, "write")).__name__ != "Allow"
    al = {"id": "s", "permission": "allowlist", "tools": ["read"]}
    assert type(await decides(al, "read")).__name__ == "Allow"
    assert type(await decides(al, "disclose_context")).__name__ == "Allow"
    assert type(await decides(al, "write")).__name__ != "Allow"
