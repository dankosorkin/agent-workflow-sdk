"""Roles, the per-backend inventory, strict and track, and the per-run worktree."""

from __future__ import annotations

import subprocess

import pytest
from agentflow.workflow import inventory_digest, validate_inventory
from agentflow.workflow import runner as runner_mod
from agentflow.workflow.environment import (
    BUILTIN_ROLES,
    Environment,
    build_inventory,
    drift,
    load_roles,
    resolve_mode,
)
from agentflow.workflow.service import WorkflowService
from test_design import BRIEF, NO_ISSUES, Designer, good_plan_obj, j
from wf_helpers import make_inventory


def write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def git(cwd, *args):
    return subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@t", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


@pytest.fixture
def project(tmp_path):
    root = tmp_path / "project"
    root.mkdir()
    git(root, "init", "-q")
    write(root / "README.md", "hello")
    write(root / ".kiro/steering/tracked.md", "---\ninclusion: manual\n---\n# Tracked rule")
    git(root, "add", "-A")
    git(root, "commit", "-q", "-m", "init")
    # not committed: a worktree would not have it
    write(root / ".kiro/steering/draft.md", "---\ninclusion: manual\n---\n# Draft rule")
    return root


def test_the_builtin_roles_and_their_layers(tmp_path):
    assert {"researcher", "analyst", "writer", "coder", "reviewer"} <= set(BUILTIN_ROLES)
    home, proj = tmp_path / "home", tmp_path / "proj"
    write(
        proj / ".agentflow/roles/researcher.md",
        "---\ndescription: mine\npermissions: read_only\n---\nBe terse.",
    )
    write(proj / ".agentflow/roles/broken.md", "---\npermissions: root\n---\nx")
    write(proj / ".agentflow/roles/empty.md", "---\ndescription: none\n---\n")
    roles = load_roles(proj, home=home)
    assert roles["researcher"].source == "project" and roles["researcher"].preamble == "Be terse."
    assert roles["analyst"].source == "builtin"
    assert "broken" not in roles and "empty" not in roles  # an unusable file never half-applies


def test_the_inventory_for_a_backend_is_valid_and_uses_the_generic_agent(project, tmp_path):
    inv = build_inventory("kiro", project, home=tmp_path / "nohome")
    assert validate_inventory(inv) == []
    assert inv["agents"]["researcher"]["backends"] == [{"backend": "kiro", "agent": "vibe"}]
    assert inv["agents"]["coder"]["permissions"] == ["read_only", "allowlist", "full"]
    assert inv["agents"]["analyst"]["preamble"]
    assert inv["context"] == {"backend": "kiro", "extensions": []}
    with pytest.raises(ValueError):
        build_inventory("nope", project)


def test_selected_extensions_are_part_of_the_digest(project, tmp_path):
    home = tmp_path / "nohome"
    pick = [{"kind": "steering", "name": "tracked"}]
    inv = build_inventory("kiro", project, selection=pick, home=home)
    assert [e["name"] for e in inv["context"]["extensions"]] == ["tracked"]
    before = inventory_digest(inv)
    write(project / ".kiro/steering/tracked.md", "---\ninclusion: manual\n---\n# Changed")
    after_inv = build_inventory("kiro", project, selection=pick, home=home)
    assert inventory_digest(after_inv) != before
    assert drift(inv, after_inv) == ["steering:tracked"]
    # a file that is not selected does not matter
    write(project / ".kiro/steering/draft.md", "# edited, but not selected")
    assert inventory_digest(
        build_inventory("kiro", project, selection=pick, home=home)
    ) == inventory_digest(after_inv)
    with pytest.raises(ValueError):
        build_inventory("kiro", project, selection=[{"kind": "skills", "name": "ghost"}], home=home)


def test_a_role_edit_changes_the_digest_too(project, tmp_path):
    home = tmp_path / "nohome"
    before = build_inventory("codex", project, home=home)
    write(
        project / ".agentflow/roles/analyst.md",
        "---\ndescription: d\npermissions: read_only\n---\nNew text.",
    )
    after = build_inventory("codex", project, home=home)
    assert inventory_digest(before) != inventory_digest(after) and drift(before, after) == [
        "role:analyst"
    ]


def test_auto_is_strict_when_a_step_writes():
    read_only = good_plan_obj()
    assert resolve_mode("auto", read_only) == "track"
    writes = good_plan_obj()
    writes["graph"]["steps"][0]["permission"] = "allowlist"
    assert resolve_mode("auto", writes) == "strict"
    effects = good_plan_obj()
    effects["graph"]["steps"][1]["side_effects"] = True
    assert resolve_mode("auto", effects) == "strict"
    assert (
        resolve_mode("track", writes) == "track" and resolve_mode("strict", read_only) == "strict"
    )


def test_a_run_gets_its_own_worktree_with_uncommitted_extensions(project, tmp_path):
    env = Environment(project, tmp_path / "data")
    ws = env.prepare_workspace("run-1", "kiro")
    assert ws["kind"] == "worktree" and ws["branch"] == "agentflow/run-1"
    path = tmp_path / "data" / "worktrees" / "run-1"
    assert (path / "README.md").read_text() == "hello"
    # the draft steering was never committed, but the agent in the worktree still sees it
    assert (path / ".kiro/steering/draft.md").exists()
    assert "agentflow/run-1" in git(project, "branch", "--list")
    assert git(project, "status", "--porcelain").count("README") == 0  # the checkout is untouched
    assert ws["path"] == str(path.resolve()) or ws["path"].endswith("run-1")


def test_a_project_that_is_not_a_repository_is_used_in_place(tmp_path):
    plain = tmp_path / "plain"
    plain.mkdir()
    ws = Environment(plain, tmp_path / "data").prepare_workspace("run-2", "codex")
    assert ws["kind"] == "directory" and ws["path"] == str(plain.resolve())
    assert "not a git repository" in ws["note"]


def plan_for_roles():
    plan = good_plan_obj()
    plan["graph"]["steps"][0]["agent"] = "researcher"
    plan["graph"]["steps"][1]["agent"] = "analyst"
    return plan


def make_service(tmp_path, project):
    seen = {"cwd": [], "prompts": []}

    async def runner(step, entry, prompt):
        seen["cwd"].append(runner_mod._workspace.get())
        seen["prompts"].append(prompt)
        return {
            "collect": '{"logs": [{"id": 1}]}',
            "analyze": '{"findings": [{"title": "spike"}]}',
        }[step["id"]]

    designer = Designer(orchestrator=[BRIEF], creator=[j(plan_for_roles())], verifier=[NO_ISSUES])
    env = Environment(
        project,
        tmp_path / "data",
        home=tmp_path / "nohome",
        designer_factory=lambda b, cwd: (designer, False),
    )
    service = WorkflowService(
        tmp_path / "data",
        inventory_source=make_inventory,
        designer=designer,
        runner=runner,
        environment=env,
    )
    return service, seen, designer


async def settle(service, kind, doc_id, done, timeout=5.0):
    import asyncio

    end = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < end:
        rec = getattr(service, f"get_{kind}")(doc_id)
        if rec["status"] in done:
            return rec
        await asyncio.sleep(0.02)
    raise AssertionError(rec)


async def designed_and_approved(service, **kw):
    d = service.start_design("find anomalies", backend="kiro", **kw)
    done = await settle(service, "design", d["id"], {"done", "failed"})
    assert done["status"] == "done", done
    service.approve(done["plan_id"], "daniel")
    return done["plan_id"]


async def test_a_plan_runs_in_its_worktree_with_the_role_preamble(project, tmp_path):
    service, seen, designer = make_service(tmp_path, project)
    plan_id = await designed_and_approved(service)
    record = service.get_plan(plan_id)
    assert record["context"] == {"backend": "kiro", "selection": [], "mode": "auto"}
    assert "analyst" in record["inventory"]["agents"]

    run = service.start_run(plan_id, {"window": "24h"})
    done = await settle(
        service, "run", run["id"], {"completed", "failed", "partial", "error", "refused"}
    )
    assert done["status"] == "completed", done
    assert done["workspace"]["kind"] == "worktree"
    assert all(str(c).endswith(run["id"]) for c in seen["cwd"]) and seen["cwd"]
    assert seen["prompts"][0].startswith(BUILTIN_ROLES["researcher"].preamble)
    assert "Collect the raw log records." in seen["prompts"][0]


async def test_strict_revokes_the_approval_when_a_selected_file_changes(project, tmp_path):
    service, _, _ = make_service(tmp_path, project)
    pick = [{"kind": "steering", "name": "tracked"}]
    plan_id = await designed_and_approved(service, extensions=pick, mode="strict")
    write(
        project / ".kiro/steering/tracked.md",
        "---\ninclusion: manual\n---\n# Changed after approval",
    )
    run = service.start_run(plan_id, {"window": "24h"})
    done = await settle(
        service, "run", run["id"], {"completed", "refused", "error", "failed", "partial"}
    )
    assert done["status"] == "refused"
    assert "inventory_changed" in {i["code"] for i in done["issues"]}


async def test_track_goes_on_and_reports_the_change(project, tmp_path):
    service, _, _ = make_service(tmp_path, project)
    pick = [{"kind": "steering", "name": "tracked"}]
    plan_id = await designed_and_approved(service, extensions=pick, mode="track")
    write(
        project / ".kiro/steering/tracked.md",
        "---\ninclusion: manual\n---\n# Changed after approval",
    )
    run = service.start_run(plan_id, {"window": "24h"})
    done = await settle(
        service, "run", run["id"], {"completed", "refused", "error", "failed", "partial"}
    )
    assert done["status"] == "completed"
    assert done["warnings"] and "steering:tracked" in done["warnings"][0]


async def test_choosing_a_backend_needs_an_environment_and_known_files(tmp_path, project):
    plain = WorkflowService(
        tmp_path / "d2",
        inventory_source=make_inventory,
        designer=Designer(orchestrator=[], creator=[], verifier=[]),
        runner=lambda *a: None,
    )
    with pytest.raises(ValueError, match="environment"):
        plain.start_design("x", backend="kiro")
    service, _, _ = make_service(tmp_path, project)
    with pytest.raises(ValueError, match="unknown"):
        service.start_design("x", backend="kiro", extensions=[{"kind": "skills", "name": "ghost"}])
    with pytest.raises(ValueError, match="mode"):
        service.start_design("x", backend="kiro", mode="loose")


async def test_the_http_form_carries_backend_extensions_and_mode(project, tmp_path):
    from agentflow.server.workflow_app import create_workflow_app
    from fastapi.testclient import TestClient

    service, _, _ = make_service(tmp_path, project)
    with TestClient(create_workflow_app(service, workspace=project)) as client:
        assert client.get("/environment").json()["enabled"] is True
        body = {
            "request": "find anomalies",
            "backend": "kiro",
            "extensions": [{"kind": "steering", "name": "tracked"}],
            "mode": "strict",
        }
        created = client.post("/designs", json=body)
        assert created.status_code == 201
        design = created.json()
        assert design["backend"] == "kiro" and design["mode"] == "strict"
        assert [e["name"] for e in design["inventory"]["context"]["extensions"]] == ["tracked"]
        bad = client.post(
            "/designs", json={**body, "extensions": [{"kind": "skills", "name": "ghost"}]}
        )
        assert bad.status_code == 422
        assert client.post("/designs", json={"request": "x", "backend": "nope"}).status_code == 422
