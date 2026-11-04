"""Roles, the per-backend inventory, strict and track, and the per-run worktree."""

from __future__ import annotations

import subprocess

import pytest
from agentflow.workflow import inventory_digest, validate_inventory
from agentflow.workflow import runner as runner_mod
from agentflow.workflow.environment import (
    BUILTIN_ROLES,
    Environment,
    bind_inventory,
    build_catalog,
    build_inventory,
    drift,
    load_roles,
    resolve_mode,
)
from agentflow.workflow.runner import PlanRefused
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


def test_the_catalog_is_neutral_and_valid(project, tmp_path):
    catalog = build_catalog(project, home=tmp_path / "nohome")
    assert catalog["neutral"] is True and validate_inventory(catalog) == []
    researcher = catalog["agents"]["researcher"]
    assert {b["backend"] for b in researcher["backends"]} == {"kiro", "claude_code", "codex"}
    assert catalog["agents"]["coder"]["permissions"] == ["read_only", "allowlist", "full"]
    assert "context" not in catalog


def test_binding_serves_every_role_by_one_backend_only(project, tmp_path):
    inv = build_inventory("kiro", project, home=tmp_path / "nohome")
    assert validate_inventory(inv) == []
    assert inv["agents"]["researcher"]["backends"] == [{"backend": "kiro", "agent": "vibe"}]
    assert inv["context"] == {"backend": "kiro", "extensions": []}
    with pytest.raises(ValueError):
        build_inventory("nope", project)


def test_an_approval_does_not_depend_on_the_backend_or_the_files(project, tmp_path):
    home = tmp_path / "nohome"
    catalog = build_catalog(project, home=home)
    digest = inventory_digest(catalog)
    pick = [{"kind": "steering", "name": "tracked"}]
    for backend in ("kiro", "claude_code", "codex"):
        bound = bind_inventory(
            catalog, backend, project, selection=pick if backend == "kiro" else [], home=home
        )
        assert inventory_digest(bound) == digest
    write(project / ".kiro/steering/tracked.md", "---\ninclusion: manual\n---\n# Changed")
    assert (
        inventory_digest(bind_inventory(catalog, "kiro", project, selection=pick, home=home))
        == digest
    )


def test_a_missing_extension_is_named(project, tmp_path):
    home = tmp_path / "nohome"
    with pytest.raises(ValueError, match="skills 'ghost'"):
        build_inventory("kiro", project, selection=[{"kind": "skills", "name": "ghost"}], home=home)


def test_drift_names_the_files_that_changed(project, tmp_path):
    home = tmp_path / "nohome"
    pick = [{"kind": "steering", "name": "tracked"}]
    before = build_inventory("kiro", project, selection=pick, home=home)["context"]["extensions"]
    assert drift(before, before) == []
    write(project / ".kiro/steering/tracked.md", "---\ninclusion: manual\n---\n# Changed")
    after = build_inventory("kiro", project, selection=pick, home=home)["context"]["extensions"]
    assert drift(before, after) == ["steering:tracked"]
    assert drift(before, []) == ["steering:tracked"] and drift([], before) == ["steering:tracked"]


def test_a_role_edit_changes_the_catalog_digest(project, tmp_path):
    home = tmp_path / "nohome"
    before = inventory_digest(build_catalog(project, home=home))
    write(
        project / ".agentflow/roles/analyst.md",
        "---\ndescription: d\npermissions: read_only\n---\nNew text.",
    )
    assert inventory_digest(build_catalog(project, home=home)) != before


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


def make_service(tmp_path, project, plan=None, on_step=None):
    seen = {"cwd": [], "prompts": [], "backends": []}

    async def runner(step, entry, prompt):
        seen["cwd"].append(runner_mod._workspace.get())
        seen["prompts"].append(prompt)
        seen["backends"].append(entry["backend"])
        if on_step:
            on_step(step["id"])
        return {
            "collect": '{"logs": [{"id": 1}]}',
            "analyze": '{"findings": [{"title": "spike"}]}',
        }[step["id"]]

    designer = Designer(
        orchestrator=[BRIEF], creator=[j(plan or plan_for_roles())], verifier=[NO_ISSUES]
    )
    env = Environment(
        project,
        tmp_path / "data",
        home=tmp_path / "nohome",
        system_backend="codex",
        designer_factory=lambda backend, sandbox: designer,
    )
    service = WorkflowService(
        tmp_path / "data",
        inventory_source=make_inventory,
        designer=designer,
        runner=runner,
        environment=env,
    )
    return service, seen


async def settle(service, kind, doc_id, done, timeout=5.0):
    import asyncio

    end = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < end:
        rec = getattr(service, f"get_{kind}")(doc_id)
        if rec["status"] in done:
            return rec
        await asyncio.sleep(0.02)
    raise AssertionError(rec)


FINISHED = {"completed", "refused", "error", "failed", "partial", "held"}


async def designed_and_approved(service):
    d = service.start_design("find anomalies")
    done = await settle(service, "design", d["id"], {"done", "failed"})
    assert done["status"] == "done", done
    service.approve(done["plan_id"], "daniel")
    return done["plan_id"], done


async def test_a_plan_is_designed_without_a_backend(project, tmp_path):
    service, _ = make_service(tmp_path, project)
    plan_id, design = await designed_and_approved(service)
    record = service.get_plan(plan_id)
    assert design["system_backend"] == "codex"
    assert record["neutral"] is True and "context" not in record and "inventory" not in record
    assert not any("backend" in s for s in record["plan"]["graph"]["steps"])
    assert service.environment_info()["system_backend"] == "codex"


async def test_the_same_approved_plan_runs_on_two_backends(project, tmp_path):
    service, seen = make_service(tmp_path, project)
    plan_id, _ = await designed_and_approved(service)
    on_kiro = service.start_run(plan_id, {"window": "24h"}, backend="kiro")
    first = await settle(service, "run", on_kiro["id"], FINISHED)
    on_codex = service.start_run(plan_id, {"window": "24h"}, backend="codex")
    second = await settle(service, "run", on_codex["id"], FINISHED)
    assert first["status"] == "completed" and second["status"] == "completed"
    assert set(seen["backends"][:2]) == {"kiro"} and set(seen["backends"][2:]) == {"codex"}
    assert first["binding"]["backend"] == "kiro" and second["binding"]["backend"] == "codex"


async def test_a_run_uses_its_worktree_and_the_role_preamble(project, tmp_path):
    service, seen = make_service(tmp_path, project)
    plan_id, _ = await designed_and_approved(service)
    run = service.start_run(plan_id, {"window": "24h"}, backend="kiro")
    done = await settle(service, "run", run["id"], FINISHED)
    assert done["status"] == "completed", done
    assert done["workspace"]["kind"] == "worktree"
    assert all(str(c).endswith(run["id"]) for c in seen["cwd"]) and seen["cwd"]
    assert seen["prompts"][0].startswith(BUILTIN_ROLES["researcher"].preamble)
    assert "Collect the raw log records." in seen["prompts"][0]


async def test_launch_needs_a_backend_and_refuses_one_that_cannot_run_the_plan(project, tmp_path):
    writes = plan_for_roles()
    writes["graph"]["steps"][0].update(agent="writer", permission="allowlist", tools=["write"])
    service, _ = make_service(tmp_path, project, plan=writes)
    plan_id, _ = await designed_and_approved(service)
    with pytest.raises(ValueError, match="choose the backend"):
        service.start_run(plan_id, {"window": "24h"})
    with pytest.raises(PlanRefused) as refused:  # codex cannot restrict to a tool list
        service.start_run(plan_id, {"window": "24h"}, backend="codex")
    assert "backend_cannot_enforce" in {
        i.code for i in refused.value.issues
    } or "no_executable_backend" in {i.code for i in refused.value.issues}
    run = service.start_run(plan_id, {"window": "24h"}, backend="kiro")
    assert (await settle(service, "run", run["id"], FINISHED))["status"] == "completed"


async def test_the_preview_says_what_a_launch_would_bind(project, tmp_path):
    service, _ = make_service(tmp_path, project)
    plan_id, _ = await designed_and_approved(service)
    ok = service.binding_preview(plan_id, "kiro", [{"kind": "steering", "name": "tracked"}], "auto")
    assert ok["resolved_mode"] == "track" and [e["name"] for e in ok["extensions"]] == ["tracked"]
    missing = service.binding_preview(plan_id, "kiro", [{"kind": "skills", "name": "ghost"}])
    assert "extension_missing" in {i["code"] for i in missing["issues"]}
    assert "unknown_backend" in {
        i["code"] for i in service.binding_preview(plan_id, "nope")["issues"]
    }


async def test_strict_holds_the_run_when_a_relied_on_file_changes_and_resume_accepts(
    project, tmp_path
):
    pick = [{"kind": "steering", "name": "tracked"}]
    edited = {"done": False}

    def edit_once(step_id):
        if step_id == "collect" and not edited["done"]:
            edited["done"] = True
            write(
                project / ".kiro/steering/tracked.md",
                "---\ninclusion: manual\n---\n# Changed mid-run",
            )

    service, _ = make_service(tmp_path, project, on_step=edit_once)
    plan_id, _ = await designed_and_approved(service)
    run = service.start_run(
        plan_id, {"window": "24h"}, backend="kiro", extensions=pick, mode="strict"
    )
    held = await settle(service, "run", run["id"], FINISHED)
    assert held["status"] == "held" and "context_changed" in {i["code"] for i in held["issues"]}
    assert "steering:tracked" in held["issues"][0]["message"]
    service.resume_run(run["id"])
    done = await settle(
        service, "run", run["id"], {"completed", "error", "failed", "partial", "refused"}
    )
    assert done["status"] == "completed"


async def test_track_goes_on_and_reports_the_change(project, tmp_path):
    pick = [{"kind": "steering", "name": "tracked"}]
    edited = {"done": False}

    def edit_once(step_id):
        if step_id == "collect" and not edited["done"]:
            edited["done"] = True
            write(
                project / ".kiro/steering/tracked.md",
                "---\ninclusion: manual\n---\n# Changed mid-run",
            )

    service, _ = make_service(tmp_path, project, on_step=edit_once)
    plan_id, _ = await designed_and_approved(service)
    run = service.start_run(
        plan_id, {"window": "24h"}, backend="kiro", extensions=pick, mode="track"
    )
    done = await settle(service, "run", run["id"], FINISHED)
    assert done["status"] == "completed"
    assert done["warnings"] and "steering:tracked" in done["warnings"][0]


async def test_an_older_plan_still_runs_without_a_binding(tmp_path, project):
    service = WorkflowService(
        tmp_path / "legacy",
        inventory_source=make_inventory,
        designer=Designer(orchestrator=[BRIEF], creator=[j(good_plan_obj())], verifier=[NO_ISSUES]),
        runner=lambda *a: None,
    )
    d = service.start_design("x")
    assert d["system_backend"] is None
    with pytest.raises(ValueError, match="environment"):
        service.start_design("x", system_backend="kiro")


async def test_the_http_layer_carries_system_backend_launch_binding_and_preview(project, tmp_path):
    from agentflow.server.workflow_app import create_workflow_app
    from fastapi.testclient import TestClient

    service, _ = make_service(tmp_path, project)
    plan_id, _ = await designed_and_approved(service)
    with TestClient(create_workflow_app(service, workspace=project)) as client:
        env = client.get("/environment").json()
        assert env["enabled"] is True and env["system_backend"] == "codex"
        preview = client.get(
            f"/plans/{plan_id}/binding", params={"backend": "kiro", "ext": ["steering:tracked"]}
        )
        assert preview.status_code == 200 and preview.json()["resolved_mode"] == "track"
        assert client.get(
            f"/plans/{plan_id}/binding", params={"backend": "kiro", "ext": ["skills:ghost"]}
        ).json()["issues"]
        no_backend = client.post(f"/plans/{plan_id}/runs", json={"params": {"window": "24h"}})
        assert no_backend.status_code == 422
        body = {
            "params": {"window": "24h"},
            "backend": "kiro",
            "extensions": [{"kind": "steering", "name": "tracked"}],
            "mode": "strict",
        }
        started = client.post(f"/plans/{plan_id}/runs", json=body)
        assert started.status_code == 201 and started.json()["binding"]["backend"] == "kiro"
        bad = client.post(f"/plans/{plan_id}/runs", json={**body, "backend": "nope"})
        assert bad.status_code == 422
