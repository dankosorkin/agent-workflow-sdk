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

"""Spike: a backend capability catalog, a step permission model, and plan checks.

Design notes §6.2. The catalog is a table in code. A table is only worth having
if it is TRUE, so this spike does not just declare it: every claim that can be
checked against the real engine is checked here against the real backend classes
(``build_command``, ``parse_line``, the permission policies, the HTTP adapters
over a mock transport, a stand-in ``kiro-cli`` process). If the engine changes,
this spike fails and the catalog must be updated — a drift guard.

Four things are shown:

  1. The catalog, and the drift guard that keeps it honest.
  2. A step's ``permission`` ({"access": "read_only" | "allowlist" | "full"}) maps
     onto the engine's real policies, and the real backends turn those into the
     flags the catalog claims. The one trap: a ``ToolAllowlist`` on Codex does NOT
     fail — it silently becomes ``-s read-only`` (stricter than asked). That is
     why the validator must REJECT it instead of trusting the backend.
  3. Plan validation against the catalog, with a machine-readable code per rule.
  4. Step isolation: one backend instance per step. A shared ``CodexBackend``
     resumes the previous step's session (context leaks); a second concurrent turn
     on one ``KiroBackend`` raises. Both are measured.

HONEST SCOPE:
  * "Real backend" for the CLI agents means the real Python classes talking to a
    stand-in executable on PATH. It does not prove what the genuine ``codex`` or
    ``claude`` binaries do with the flags. (This was checked separately against the
    real agents; see design notes §6.2. Notably ``AllowAll`` on Claude Code maps to
    ``--permission-mode acceptEdits`` (with no flag headless writes are REFUSED).
    The assertion below only pins what
    the backend class puts on the command line, not what the CLI then permits.)
  * Skills and MCP are asserted only as "the SDK has no way to pass them"
    (Kiro: ``mcpServers: []`` is hard-coded and is checked on the wire; the
    others have no parameter). Codex MCP support through ``extra_args`` is not
    investigated.
  * The catalog is per backend KIND. Model- or version-specific differences are
    out of scope.

Run it directly:

    python spikes/capabilities_spike.py
"""

from __future__ import annotations

import asyncio
import json
import os
import stat
import sys
import tempfile
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from agentflow import Message, PermissionRequest
from agentflow.backends import AllowAll, DenyAll, ToolAllowlist
from agentflow.backends.anthropic import AnthropicBackend
from agentflow.backends.claude_code import ClaudeCodeBackend
from agentflow.backends.cli_exec import TurnAccumulator
from agentflow.backends.codex import CodexBackend
from agentflow.backends.kiro import KiroBackend
from agentflow.backends.ollama import OllamaBackend
from agentflow.backends.openai import OpenAIBackend
from agentflow.errors import BackendError
from agentflow.events import Allow, Deny, ToolCall, ToolResult

# ---------------------------------------------------------------------------
# 1. The catalog
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Capabilities:
    enforcement: str  # "runtime" | "launch_flags" | "sandbox_only" | "construction"
    allowlist: bool  # can restrict to a list of named tools
    agent_by_name: bool  # a named agent definition can be selected
    mcp: bool  # the SDK can pass MCP servers
    skills: bool  # the SDK can pass skills
    tool_results: str  # "yes" | "partial" | "no" — does it emit ToolResult events
    usage: bool  # reports token Usage
    parallel_on_instance: bool  # several concurrent turns on one instance are allowed


CATALOG: dict[str, Capabilities] = {
    "kiro": Capabilities("runtime", True, True, False, False, "yes", False, False),
    "claude_code": Capabilities("launch_flags", True, True, False, False, "no", False, True),
    "codex": Capabilities("sandbox_only", False, False, False, False, "partial", True, True),
    "ollama": Capabilities("construction", True, False, False, False, "no", True, True),
    "openai": Capabilities("construction", True, False, False, False, "no", False, True),
    "anthropic": Capabilities("construction", True, False, False, False, "no", False, True),
}
# For the HTTP backends the graph (tool_loop), not the backend, runs tools and
# feeds results back as messages, so "no ToolResult from the backend" is expected.


def print_catalog() -> None:
    cols = [
        "enforcement",
        "allowlist",
        "by_name",
        "mcp",
        "skills",
        "tool_results",
        "usage",
        "parallel",
    ]
    print(f"  {'backend':<13}" + "".join(f"{c:<14}" for c in cols))
    for name, c in CATALOG.items():
        row = [
            c.enforcement,
            c.allowlist,
            c.agent_by_name,
            c.mcp,
            c.skills,
            c.tool_results,
            c.usage,
            c.parallel_on_instance,
        ]
        print(f"  {name:<13}" + "".join(f"{str(v):<14}" for v in row))


# ---------------------------------------------------------------------------
# 2. Step permission -> the engine's real policies
# ---------------------------------------------------------------------------


def policy_for(permission: dict[str, Any] | None):
    permission = permission or {"access": "read_only"}  # the strictest is the default
    access = permission.get("access", "read_only")
    if access == "read_only":
        return DenyAll()
    if access == "allowlist":
        return ToolAllowlist(set(permission.get("tools") or []))
    if access == "full":
        return AllowAll()
    raise ValueError(f"unknown access {access!r}")


# ---------------------------------------------------------------------------
# 3. Plan checks against the catalog
# ---------------------------------------------------------------------------


def check_plan(plan: dict[str, Any], backend_of: dict[str, str]) -> list[dict[str, str]]:
    """Return every problem as {severity, code, step, message}. Errors block the run."""
    problems: list[dict[str, str]] = []

    def add(sev: str, code: str, step: str, msg: str) -> None:
        problems.append({"severity": sev, "code": code, "step": step, "message": msg})

    for s in plan["steps"]:
        sid, agent = s["id"], s.get("agent", "default")
        backend = backend_of.get(agent)
        if backend not in CATALOG:
            add("error", "unknown_backend", sid, f"agent {agent!r} maps to no known backend")
            continue
        cap = CATALOG[backend]
        access = (s.get("permission") or {}).get("access", "read_only")
        if access == "allowlist" and not cap.allowlist:
            add(
                "error",
                "backend_cannot_enforce",
                sid,
                f"{backend} cannot restrict to a tool list (it would silently become read-only)",
            )
        if s.get("mcp") and not cap.mcp:
            add("error", "backend_no_mcp", sid, f"the SDK cannot pass MCP servers to {backend}")
        if s.get("skills") and not cap.skills:
            add("error", "backend_no_skills", sid, f"the SDK cannot pass skills to {backend}")
        if agent != "default" and not cap.agent_by_name:
            add(
                "error",
                "backend_no_agent_by_name",
                sid,
                f"{backend} cannot select the named agent {agent!r}",
            )
        if s.get("requires_evidence"):
            if cap.tool_results == "no":
                add("error", "evidence_unavailable", sid, f"{backend} emits no ToolResult events")
            elif cap.tool_results == "partial":
                add("warning", "evidence_partial", sid, f"{backend} reports only some tool results")
        if (s.get("budget") or {}).get("tokens") and not cap.usage:
            add("error", "usage_unavailable", sid, f"{backend} reports no token usage")
    return problems


def plan_with(**step: Any) -> dict[str, Any]:
    return {"steps": [{"id": "s1", "agent": "default", **step}]}


PLAN_CASES: list[tuple[str, dict[str, Any], dict[str, str], str]] = [
    ("agent maps to no backend", plan_with(agent="ghost"), {}, "unknown_backend"),
    (
        "allowlist on Codex",
        plan_with(permission={"access": "allowlist", "tools": ["x"]}),
        {"default": "codex"},
        "backend_cannot_enforce",
    ),
    ("MCP on Kiro", plan_with(mcp=["db"]), {"default": "kiro"}, "backend_no_mcp"),
    (
        "skills on Claude Code",
        plan_with(skills=["review"]),
        {"default": "claude_code"},
        "backend_no_skills",
    ),
    (
        "named agent on Codex",
        plan_with(agent="reviewer"),
        {"reviewer": "codex"},
        "backend_no_agent_by_name",
    ),
    (
        "evidence required from Claude Code",
        plan_with(requires_evidence=True),
        {"default": "claude_code"},
        "evidence_unavailable",
    ),
    (
        "evidence required from Codex (partial)",
        plan_with(requires_evidence=True),
        {"default": "codex"},
        "evidence_partial",
    ),
    (
        "token budget on Kiro",
        plan_with(budget={"tokens": 1000}),
        {"default": "kiro"},
        "usage_unavailable",
    ),
    (
        "token budget on OpenAI",
        plan_with(budget={"tokens": 1000}),
        {"default": "openai"},
        "usage_unavailable",
    ),
]
PLAN_OK: list[tuple[str, dict[str, Any], dict[str, str]]] = [
    ("read-only default on every backend", plan_with(), {"default": "codex"}),
    (
        "allowlist on Kiro",
        plan_with(permission={"access": "allowlist", "tools": ["a"]}),
        {"default": "kiro"},
    ),
    (
        "allowlist on Claude Code",
        plan_with(permission={"access": "allowlist", "tools": ["Read"]}),
        {"default": "claude_code"},
    ),
    ("evidence from Kiro", plan_with(requires_evidence=True), {"default": "kiro"}),
    ("token budget on Codex", plan_with(budget={"tokens": 1000}), {"default": "codex"}),
    ("token budget on Ollama", plan_with(budget={"tokens": 1000}), {"default": "ollama"}),
    ("named agent on Claude Code", plan_with(agent="reviewer"), {"reviewer": "claude_code"}),
]


def test_plan_checks() -> None:
    print(f"  {'plan':<42}{'expected':<28}reported")
    for title, plan, backends, code in PLAN_CASES:
        codes = sorted({p["code"] for p in check_plan(plan, backends)})
        print(f"  {title:<42}{code:<28}{codes}")
        assert code in codes, f"{title}: expected {code}, got {codes}"
    for title, plan, backends in PLAN_OK:
        errors = [p for p in check_plan(plan, backends) if p["severity"] == "error"]
        assert errors == [], f"{title}: {errors}"
    # a warning does not block; an error does
    warn = check_plan(plan_with(requires_evidence=True), {"default": "codex"})
    assert [p["severity"] for p in warn] == ["warning"]
    # every problem is reported at once
    many = check_plan(
        plan_with(mcp=["m"], skills=["k"], requires_evidence=True, budget={"tokens": 5}),
        {"default": "claude_code"},
    )
    assert {p["code"] for p in many} == {
        "backend_no_mcp",
        "backend_no_skills",
        "evidence_unavailable",
        "usage_unavailable",
    }


# ---------------------------------------------------------------------------
# Stand-in executables, so the real backend classes run for real
# ---------------------------------------------------------------------------


def install_fakes() -> Path:
    """Put fake ``codex`` and ``kiro-cli`` on PATH; they log what they were asked."""
    bin_dir = Path(tempfile.mkdtemp(prefix="spike-bin-"))
    os.environ["SPIKE_LOG"] = str(bin_dir / "calls.log")
    os.environ["PATH"] = f"{bin_dir}{os.pathsep}{os.environ['PATH']}"

    def make(name: str, body: str) -> None:
        path = bin_dir / name
        path.write_text(f"#!{sys.executable}\n" + textwrap.dedent(body))
        path.chmod(path.stat().st_mode | stat.S_IEXEC)

    make(
        "codex",
        """
        import json, os, sys
        log = os.environ["SPIKE_LOG"]
        n = sum(1 for line in open(log) if '"codex"' in line) if os.path.exists(log) else 0
        open(log, "a").write(json.dumps({"codex": sys.argv[1:]}) + "\\n")
        print(json.dumps({"type": "thread.started", "thread_id": f"th-{n + 1}"}))
        item = {"type": "agent_message", "text": "ok"}
        print(json.dumps({"type": "item.completed", "item": item}))
        usage = {"input_tokens": 1, "output_tokens": 1}
        print(json.dumps({"type": "turn.completed", "usage": usage}))
    """,
    )
    make(
        "kiro-cli",
        """
        import json, os, sys, time
        def send(o): sys.stdout.write(json.dumps(o) + "\\n"); sys.stdout.flush()
        for line in sys.stdin:
            m = json.loads(line)
            if m.get("method") == "initialize":
                send({"jsonrpc": "2.0", "id": m["id"], "result": {}})
            elif m.get("method") == "session/new":
                line = json.dumps({"session_new": m["params"]}) + "\\n"
                open(os.environ["SPIKE_LOG"], "a").write(line)
                send({"jsonrpc": "2.0", "id": m["id"], "result": {"sessionId": "s"}})
            elif m.get("method") == "session/prompt":
                time.sleep(0.3)
                send({"jsonrpc": "2.0", "id": m["id"], "result": {"stopReason": "end_turn"}})
    """,
    )
    return bin_dir


def read_log() -> list[dict[str, Any]]:
    path = Path(os.environ["SPIKE_LOG"])
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def clear_log() -> None:
    Path(os.environ["SPIKE_LOG"]).unlink(missing_ok=True)


async def drain(backend, text: str) -> list[Any]:
    return [ev async for ev in backend.prompt(text)]


# ---------------------------------------------------------------------------
# The drift guard: the catalog against the real engine
# ---------------------------------------------------------------------------


def flags(cmd: list[str], name: str) -> list[str]:
    """The value(s) following a flag in a command line, or [] if the flag is absent."""
    return [cmd[i + 1] for i, a in enumerate(cmd[:-1]) if a == name]


async def http_usage(backend, body: bytes) -> Any:
    backend._client = httpx.AsyncClient(
        base_url="http://x",
        transport=httpx.MockTransport(lambda r: httpx.Response(200, content=body)),
    )
    events = [ev async for ev in backend.chat([Message("user", "hi")])]
    await backend.close()
    return events[-1].usage


async def test_catalog_matches_engine() -> None:
    perm = {"access": "allowlist", "tools": ["Read", "Grep"]}

    # --- permissions: Codex (sandbox only) -------------------------------------
    def codex_sandbox(permission) -> list[str]:
        cmd = CodexBackend(permission=policy_for(permission)).build_command("p", None)
        return flags(cmd, "-s")

    assert codex_sandbox({"access": "read_only"}) == ["read-only"]
    assert codex_sandbox({"access": "full"}) == ["workspace-write"]
    # The trap: an allowlist does not fail, it silently becomes read-only.
    assert codex_sandbox(perm) == ["read-only"]
    assert CATALOG["codex"].allowlist is False
    assert (
        check_plan(plan_with(agent="default", permission=perm), {"default": "codex"})[0]["code"]
        == "backend_cannot_enforce"
    )

    # --- permissions: Claude Code (launch flags) -------------------------------
    def claude_cmd(permission) -> list[str]:
        return ClaudeCodeBackend(permission=policy_for(permission)).build_command("p", None)

    assert flags(claude_cmd({"access": "read_only"}), "--permission-mode") == ["plan"]
    assert flags(claude_cmd(perm), "--allowed-tools") == ["Grep Read"]
    full = claude_cmd({"access": "full"})
    assert flags(full, "--permission-mode") == ["acceptEdits"] and "--allowed-tools" not in full
    assert "bypassPermissions" not in full and "--allow-dangerously-skip-permissions" not in full
    assert CATALOG["claude_code"].allowlist is True

    # --- permissions: Kiro (decided at run time, per request) ------------------
    async def kiro_decision(permission, tool):
        return await policy_for(permission).decide(PermissionRequest(id="1", tool=tool))

    assert isinstance(await kiro_decision({"access": "read_only"}, "fs_read"), Deny)
    assert isinstance(await kiro_decision(perm, "Read"), Allow)
    assert isinstance(await kiro_decision(perm, "Bash"), Deny)
    assert isinstance(await kiro_decision({"access": "full"}, "Bash"), Allow)

    # --- ToolResult emission ----------------------------------------------------
    claude, codex = ClaudeCodeBackend(permission=DenyAll()), CodexBackend(permission=DenyAll())
    tool_use = {
        "type": "assistant",
        "message": {"content": [{"type": "tool_use", "id": "t1", "name": "Read", "input": {}}]},
    }
    kinds = [type(e) for e in claude.parse_line(tool_use, TurnAccumulator())]
    assert kinds == [ToolCall], kinds  # a ToolCall, never a ToolResult
    shell = {
        "type": "item.completed",
        "item": {"id": "i", "type": "command_execution", "command": "ls", "exit_code": 0},
    }
    patch = {"type": "item.completed", "item": {"id": "j", "type": "file_change"}}
    assert [type(e) for e in codex.parse_line(shell, TurnAccumulator())] == [ToolCall, ToolResult]
    assert [type(e) for e in codex.parse_line(patch, TurnAccumulator())] == [ToolCall]
    assert (CATALOG["claude_code"].tool_results, CATALOG["codex"].tool_results) == ("no", "partial")

    # --- Usage ------------------------------------------------------------------
    acc = TurnAccumulator()
    codex.parse_line(
        {"type": "turn.completed", "usage": {"input_tokens": 3, "output_tokens": 4}}, acc
    )
    assert acc.usage is not None and acc.usage.total_tokens == 7
    acc = TurnAccumulator()
    claude.parse_line(
        {
            "type": "result",
            "subtype": "success",
            "result": "x",
            "usage": {"input_tokens": 3, "output_tokens": 4},
        },
        acc,
    )
    assert acc.usage is None  # Claude Code's adapter does not read usage
    ollama_done = (
        json.dumps(
            {"message": {"content": "x"}, "done": True, "prompt_eval_count": 5, "eval_count": 6}
        )
        + "\n"
    ).encode()
    assert (await http_usage(OllamaBackend("m"), ollama_done)).total_tokens == 11
    openai_sse = (
        b'data: {"choices": [{"delta": {"content": "x"}, "finish_reason": "stop"}]}\n\n'
        b"data: [DONE]\n\n"
    )
    assert await http_usage(OpenAIBackend("m"), openai_sse) is None
    anthropic_sse = (
        b'event: message_delta\ndata: {"type": "message_delta", '
        b'"delta": {"stop_reason": "end_turn"}}\n\n'
    )
    assert await http_usage(AnthropicBackend("m"), anthropic_sse) is None
    assert [
        CATALOG[n].usage for n in ("kiro", "claude_code", "codex", "ollama", "openai", "anthropic")
    ] == [False, False, True, True, False, False]

    # --- MCP on the wire: Kiro sends an empty server list, whatever the plan says ---
    clear_log()
    async with KiroBackend("x", engine="v2", permission=DenyAll()):
        pass
    new = [e["session_new"] for e in read_log() if "session_new" in e]
    assert new and new[0]["mcpServers"] == [], new
    assert CATALOG["kiro"].mcp is False

    print(
        "  catalog verified against: policies, build_command, parse_line, Usage, "
        "mock HTTP transports, a stand-in kiro-cli (MCP list on the wire)"
    )


# ---------------------------------------------------------------------------
# 4. Step isolation: one backend instance per step
# ---------------------------------------------------------------------------


async def test_instance_per_step() -> None:
    def codex_calls() -> list[list[str]]:
        return [e["codex"] for e in read_log() if "codex" in e]

    clear_log()
    shared = CodexBackend(permission=AllowAll())
    await drain(shared, "step one")
    await drain(shared, "step two")  # the "next step" reuses the instance
    second = codex_calls()[1]
    assert "resume" in second and "th-1" in second, second
    tail = second[-3:]
    print(f"  shared CodexBackend, second step argv tail: {tail}  <- resumes step one's session")

    clear_log()
    reused = CodexBackend(permission=AllowAll())  # one instance, but reset between steps
    await drain(reused, "step one")
    await reused.reset_session()
    await drain(reused, "step two")
    second = codex_calls()[1]
    assert "resume" not in second, second
    print(f"  shared instance + reset_session(), second step argv tail: {second[-3:]}  <- fresh")

    clear_log()
    await drain(CodexBackend(permission=AllowAll()), "step one")  # a factory call per step
    await drain(CodexBackend(permission=AllowAll()), "step two")
    second = codex_calls()[1]
    assert "resume" not in second, second
    print(f"  one instance per step, second step argv tail: {second[-3:]}  <- fresh session")

    async with KiroBackend("x", engine="v2", permission=AllowAll()) as one:
        results = await asyncio.gather(drain(one, "a"), drain(one, "b"), return_exceptions=True)
    failures = [r for r in results if isinstance(r, BackendError)]
    assert len(failures) == 1 and "already in progress" in str(failures[0]), results
    print(f"  two concurrent turns on one KiroBackend: {failures[0]}")
    assert CATALOG["kiro"].parallel_on_instance is False


async def main() -> None:
    install_fakes()
    print("=== 1. the catalog ===")
    print_catalog()
    print("\n=== 2. the catalog is true (drift guard against the real engine) ===")
    await test_catalog_matches_engine()
    print("\n=== 3. plan checks against the catalog ===")
    test_plan_checks()
    print("\n=== 4. step isolation: one backend instance per step ===")
    await test_instance_per_step()
    print("\nAll spike assertions passed.")


if __name__ == "__main__":
    asyncio.run(main())
