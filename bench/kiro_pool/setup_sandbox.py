"""Create a hermetic sandbox with custom Kiro agents and (fake or real) MCP servers.

    python bench/kiro_pool/setup_sandbox.py [--profile fake|real] [--sandbox DIR]

Agents live in ``<sandbox>/.kiro/agents`` (Kiro discovers local agents from the
working directory) and set ``includeMcpJson: false`` so your global MCP servers
and accounts are never touched. Nothing is written outside the sandbox.

Profiles:
  fake  local stdlib MCP servers with tunable start-up delay (default, offline)
  real  adds the reference npx servers (memory, sequential-thinking); needs
        network + npm, downloads packages on first run
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROMPT = (
    "You are a terse benchmark agent. Follow the user's instruction literally and never use tools."
)


def fake(name: str, startup_ms: int, tools: int, log: Path) -> dict:
    return {
        "command": sys.executable,
        "args": [
            str(HERE / "fake_mcp.py"),
            "--name",
            name,
            "--tools",
            str(tools),
            "--startup-ms",
            str(startup_ms),
        ],
        "env": {"BENCH_SPAWN_LOG": str(log)},
    }


def build(profile: str, log: Path) -> dict[str, dict[str, dict]]:
    """agent name -> {server name -> server config}. `shared` is used by two agents."""
    shared = fake("shared", 1500, 8, log)
    agents = {
        "bench-light": {"shared": shared},
        "bench-medium": {"shared": shared, "docs": fake("docs", 2500, 20, log)},
        "bench-heavy": {
            "db": fake("db", 4000, 30, log),
            "search": fake("search", 3000, 15, log),
            "git": fake("git", 1000, 12, log),
        },
    }
    if profile == "real":
        npx = lambda pkg: {"command": "npx", "args": ["-y", pkg]}  # noqa: E731
        agents["bench-medium"]["memory"] = npx("@modelcontextprotocol/server-memory")
        agents["bench-heavy"]["thinking"] = npx("@modelcontextprotocol/server-sequential-thinking")
    return agents


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", choices=["fake", "real"], default="fake")
    ap.add_argument("--sandbox", type=Path, default=HERE / ".sandbox")
    args = ap.parse_args()

    sandbox = args.sandbox.resolve()
    agents_dir = sandbox / ".kiro" / "agents"
    agents_dir.mkdir(parents=True, exist_ok=True)
    for old in agents_dir.glob("bench-*.json"):
        old.unlink()  # stale files from an older layout
    log = sandbox / "spawns.log"
    log.write_text("", encoding="utf-8")

    for old in agents_dir.glob("bench-*.md"):
        old.unlink()
    for name, servers in build(args.profile, log).items():
        # Kiro's v3 engine lists only markdown agents as session modes (local
        # JSON agents are invisible to it), so the config is markdown frontmatter.
        # JSON is valid YAML, so the nested structures are written as JSON.
        front = {
            "name": name,
            "description": f"Benchmark agent with {len(servers)} MCP server(s)",
            "mcpServers": servers,
            "tools": [f"@{s}" for s in servers],
            "includeMcpJson": False,
        }
        lines = [f"{k}: {json.dumps(v)}" for k, v in front.items()]
        path = agents_dir / f"{name}.md"
        path.write_text("---\n" + "\n".join(lines) + "\n---\n\n" + PROMPT + "\n", encoding="utf-8")
        print(f"wrote {path}  servers={list(servers)}")
    print(f"sandbox: {sandbox}")


if __name__ == "__main__":
    main()
