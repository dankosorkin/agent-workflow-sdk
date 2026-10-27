from __future__ import annotations

import json

import pytest
from agentflow.backends import DenyAll
from agentflow.workflow import claude_designer, design_plan
from agentflow.workflow import design as design_module
from agentflow.workflow.checks import CHECKS
from agentflow.workflow.design_prompts import (
    ROLES,
    creator_prompt,
    creator_task,
    role_system,
    verifier_prompt,
    verifier_task,
)
from agentflow.workflow.roles import (
    agent_name,
    claude_inline_agents,
    install_agents,
    main,
    packaged_dir,
    render_agents,
)
from test_design import BRIEF, NO_ISSUES, Designer, good_plan_obj, j


def test_the_packaged_files_are_exactly_what_the_prompts_render():
    """One source of truth: edit design_prompts.py, run `python -m agentflow.workflow generate`."""
    for backend, files in render_agents().items():
        for name, text in files.items():
            shipped = packaged_dir() / backend / name
            assert shipped.exists(), (
                f"{shipped} is missing; run: python -m agentflow.workflow generate"
            )
            assert shipped.read_text(encoding="utf-8") == text, f"{shipped} is stale; regenerate"
    shipped_names = {p.name for p in packaged_dir().rglob("*") if p.is_file()}
    rendered = {n for files in render_agents().values() for n in files}
    assert shipped_names == rendered  # nothing extra lingers


@pytest.mark.parametrize("role", ROLES)
def test_claude_files_have_valid_frontmatter(role):
    text = render_agents()["claude"][f"{agent_name(role)}.md"]
    assert text.startswith("---\n")
    front, _, body = text[4:].partition("\n---\n")
    fields = dict(line.split(": ", 1) for line in front.splitlines() if not line.startswith("#"))
    assert fields["name"] == agent_name(role)
    assert set(fields) == {"name", "description", "disallowedTools", "permissionMode"}
    # an unquoted YAML value must not contain ': ' or start with an indicator character
    assert ": " not in fields["description"] and fields["description"][0].isalpha()
    assert "Write" in fields["disallowedTools"] and "Bash" in fields["disallowedTools"]
    assert body.strip() == role_system(role).strip()


@pytest.mark.parametrize("role", ROLES)
def test_kiro_files_are_valid_json_with_the_documented_fields(role):
    data = json.loads(render_agents()["kiro"][f"{agent_name(role)}.json"])
    assert data["name"] == agent_name(role)
    assert data["prompt"] == role_system(role)
    assert data["tools"] == [] and data["description"]


def test_role_instructions_are_static_and_the_creator_lists_the_real_checks():
    creator = role_system("creator")
    assert all(kind in creator for kind in CHECKS)
    for role in ROLES:
        text = role_system(role)
        assert text == role_system(role)  # deterministic
        headers = {line for line in text.splitlines() if line.startswith("### ")}
        assert not headers & {"### request", "### brief", "### plan"}  # no data block in a role


def test_inline_agents_json_for_claude():
    agents = json.loads(claude_inline_agents())
    assert set(agents) == {agent_name(r) for r in ROLES}
    assert all(set(a) == {"description", "prompt"} for a in agents.values())
    assert agents["wf-verifier"]["prompt"] == role_system("verifier")


# ---- install ------------------------------------------------------------------------------


def test_install_creates_then_reports_unchanged(tmp_path):
    first = install_agents("claude", tmp_path)
    assert {a for _, a in first} == {"created"} and len(list(tmp_path.iterdir())) == 3
    assert {a for _, a in install_agents("claude", tmp_path)} == {"unchanged"}


def test_install_never_overwrites_a_different_file_without_force(tmp_path):
    install_agents("kiro", tmp_path)
    edited = tmp_path / "wf-creator.json"
    edited.write_text('{"mine": true}', encoding="utf-8")
    actions = dict(install_agents("kiro", tmp_path))
    assert actions[edited] == "skipped" and edited.read_text() == '{"mine": true}'
    assert dict(install_agents("kiro", tmp_path, force=True))[edited] == "overwritten"
    assert json.loads(edited.read_text())["name"] == "wf-creator"


def test_dry_run_writes_nothing(tmp_path):
    target = tmp_path / "agents"
    assert {a for _, a in install_agents("claude", target, dry_run=True)} == {"created"}
    assert not target.exists()


def test_unknown_backend_and_the_cli_requires_an_explicit_target(tmp_path, capsys):
    with pytest.raises(ValueError):
        install_agents("codex", tmp_path)
    with pytest.raises(SystemExit):  # there is no default install location
        main(["install", "claude"])
    assert main(["install", "claude", "--to", str(tmp_path / "a")]) == 0
    assert "created" in capsys.readouterr().out
    assert main(["show", "claude", "wf-creator"]) == 0
    assert main(["show", "claude", "nope"]) == 1


# ---- the two ways of carrying a role ------------------------------------------------------------


def test_named_agent_prompts_carry_only_the_data():
    brief = json.loads(BRIEF)
    inv = {"agents": {"a": {"description": "d", "permissions": ["read_only"]}}}
    full = creator_prompt(brief, inv, feedback=["- [x] fix"])
    task = creator_task(brief, inv, feedback=["- [x] fix"])
    assert full.endswith(task) and "You are wf-creator" in full and "You are wf-creator" not in task
    plan = good_plan_obj()
    assert verifier_prompt(brief, inv, plan).endswith(verifier_task(brief, inv, plan))


async def test_design_plan_with_named_agents_sends_data_only(inventory):
    d = Designer(orchestrator=[BRIEF], creator=[j(good_plan_obj())], verifier=[NO_ISSUES])
    result = await design_plan("find anomalies", inventory, d, named_agents=True)
    assert result.status == "converged"
    for role in ROLES:
        assert f"You are wf-{role}" not in d.prompts(role)[0]
    assert "### request" in d.prompts("orchestrator")[0]
    assert "### brief" in d.prompts("creator")[0] and "### plan" in d.prompts("verifier")[0]


async def test_design_plan_without_named_agents_sends_the_full_instructions(inventory):
    d = Designer(orchestrator=[BRIEF], creator=[j(good_plan_obj())], verifier=[NO_ISSUES])
    await design_plan("find anomalies", inventory, d)
    for role in ROLES:
        assert f"You are wf-{role}" in d.prompts(role)[0]


async def test_claude_designer_selects_the_named_agent(monkeypatch, tmp_path):
    seen = []

    async def fake_collect(backend, prompt):
        seen.append(backend)
        return "ok"

    monkeypatch.setattr(design_module, "collect_text", fake_collect)
    inline = claude_designer(tmp_path)
    await inline("creator", "p")
    backend = seen[0]
    assert backend.agent == "wf-creator" and isinstance(backend.permission, DenyAll)
    assert "--agents" in backend.extra_args  # nothing installed, nothing written

    installed = claude_designer(tmp_path, installed=True)
    await installed("verifier", "p")
    assert seen[1].agent == "wf-verifier" and "--agents" not in seen[1].extra_args
