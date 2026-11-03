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

"""The environment a plan runs in: which backend, which roles, which files, which directory.

A step is carried out by the *generic* agent of the chosen backend (no named agent files), shaped
by a **role** (a short preamble: researcher, writer, coder...). Everything the user adds to the
backend's own folders (steering, skills, ``AGENTS.md``) is applied by the backend itself; the SDK
only records which of those files the plan relies on, so a change can be noticed:

* **strict**: the files are part of the inventory digest. Edit one after approval and the approval
  is revoked, exactly like an inventory change.
* **track**: the plan keeps the snapshot it was approved with; a changed file is reported as a
  warning and the run goes on.
* **auto** (default): strict if any step writes (``allowlist``/``full``) or has side effects,
  otherwise track.

Each run gets its own git worktree on its own branch, so what the agents write never touches the
user's checkout. A project that is not a git repository is used in place, and the run says so.
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agentflow.workflow.extensions import (
    _SOURCES,
    BACKENDS,
    GENERIC_AGENT,
    discover,
    parse_front_matter,
)

__all__ = [
    "BUILTIN_ROLES",
    "MODES",
    "Environment",
    "Role",
    "build_inventory",
    "load_roles",
    "resolve_mode",
]

MODES = ("auto", "strict", "track")
_PERMISSIONS = ("read_only", "allowlist", "full")


@dataclass(frozen=True)
class Role:
    name: str
    description: str
    permissions: tuple[str, ...]
    preamble: str
    source: str  # builtin | user | project


def _role(name: str, description: str, permissions: tuple[str, ...], preamble: str) -> Role:
    return Role(name, description, permissions, preamble, "builtin")


BUILTIN_ROLES: dict[str, Role] = {
    r.name: r
    for r in (
        _role(
            "researcher",
            "Investigates a question: reads files and, where the backend allows it, searches the "
            "web. Reports facts with their sources. Changes nothing.",
            ("read_only",),
            "You are a researcher. Find out what the task asks and report facts together with "
            "their sources (file paths, URLs). Change nothing. If you cannot verify a claim, say "
            "so instead of guessing.",
        ),
        _role(
            "analyst",
            "Analyses the data or text a step is given and returns structured findings.",
            ("read_only",),
            "You are an analyst. Work only from the material you are given. State what you "
            "found, how sure you are, and what the evidence is.",
        ),
        _role(
            "writer",
            "Produces a document or file from the material it is given.",
            _PERMISSIONS,
            "You are a writer. Produce exactly the document the task describes, in the format "
            "asked. When the task names a file, write it at that path and say where it is.",
        ),
        _role(
            "coder",
            "Reads and changes code in the project (in an isolated worktree).",
            _PERMISSIONS,
            "You are a software engineer. Make the smallest change that does what the task asks, "
            "follow the project's conventions, and run what verifies it. Say what you changed.",
        ),
        _role(
            "reviewer",
            "Reviews a result against its task and answers with a structured verdict.",
            ("read_only",),
            "You are a reviewer. Judge the result against the task, not against your taste. "
            "Report concrete, checkable findings; if there are none, say so.",
        ),
    )
}


def load_roles(project: Path | str, *, home: Path | str | None = None) -> dict[str, Role]:
    """Built-in roles, then ``~/.agentflow/roles/*.md``, then ``<project>/.agentflow/roles/*.md``.

    A role file is Markdown: front matter ``description`` and ``permissions`` (comma separated),
    and the body is the preamble. Same name, later layer wins, so a project can replace a built-in.
    """
    roles = dict(BUILTIN_ROLES)
    home_dir = Path(home).expanduser() if home is not None else Path.home()
    layers = (
        ("user", home_dir / ".agentflow" / "roles"),
        ("project", Path(project) / ".agentflow" / "roles"),
    )
    for source, folder in layers:
        if not folder.is_dir():
            continue
        for path in sorted(folder.glob("*.md")):
            try:
                meta, body = parse_front_matter(path.read_text(encoding="utf-8"))
            except OSError:
                continue
            perms = tuple(p.strip() for p in meta.get("permissions", "read_only").split(","))
            if not body.strip() or any(p not in _PERMISSIONS for p in perms):
                continue  # an unusable file never half-applies
            roles[path.stem] = Role(
                path.stem, meta.get("description", ""), perms, body.strip(), source
            )
    return roles


_EXTENSIONS_NOTE = (
    "The agent loads these itself. Name one in a step prompt when that step should use it "
    "(for example 'use the api-design skill'); nothing is pasted for you."
)


def build_inventory(
    backend: str,
    project: Path | str,
    *,
    selection: list[dict[str, str]] | None = None,
    home: Path | str | None = None,
) -> dict[str, Any]:
    """The inventory for ``backend``: the roles, each served by its generic agent, plus the
    extension files the plan relies on (``selection`` is a list of ``{"kind", "name"}``).

    The result has the usual ``agents`` shape, so validation, backend resolution and approval work
    unchanged. ``context`` and each agent's ``preamble`` are part of the digest on purpose.
    """
    if backend not in BACKENDS:
        raise ValueError(f"unknown backend {backend!r}")
    roles = load_roles(project, home=home)
    agent = GENERIC_AGENT[backend]
    inventory: dict[str, Any] = {
        "agents": {
            name: {
                "description": role.description,
                "permissions": list(role.permissions),
                "backends": [{"backend": backend, "agent": agent}],
                "preamble": role.preamble,
            }
            for name, role in sorted(roles.items())
        }
    }
    found = discover(backend, project, home=home)
    chosen: list[dict[str, Any]] = []
    for want in selection or []:
        match = next(
            (e for e in found.get(want.get("kind", ""), []) if e.name == want.get("name")), None
        )
        if match is None:
            raise ValueError(f"unknown {want.get('kind')} {want.get('name')!r} for {backend}")
        chosen.append(
            {
                "kind": match.kind,
                "name": match.name,
                "description": match.description,
                "scope": match.scope,
                "digest": match.digest,
            }
        )
    inventory["context"] = {"backend": backend, "extensions": chosen}
    return inventory


def resolve_mode(mode: str, plan: dict[str, Any]) -> str:
    """``strict`` or ``track``: ``auto`` is strict when any step writes or has side effects."""
    if mode in ("strict", "track"):
        return mode
    for step in (plan.get("graph") or {}).get("steps") or []:
        if step.get("permission", "read_only") != "read_only" or step.get("side_effects"):
            return "strict"
    return "track"


def drift(snapshot: dict[str, Any], live: dict[str, Any]) -> list[str]:
    """Names of extension files that differ between two inventories (changed, added, gone)."""
    old = {
        (e["kind"], e["name"]): e["digest"]
        for e in (snapshot.get("context") or {}).get("extensions", [])
    }
    new = {
        (e["kind"], e["name"]): e["digest"]
        for e in (live.get("context") or {}).get("extensions", [])
    }
    changed = [
        f"{k[0]}:{k[1]}" for k in sorted(old.keys() | new.keys()) if old.get(k) != new.get(k)
    ]
    roles_old = {n: a.get("preamble") for n, a in (snapshot.get("agents") or {}).items()}
    roles_new = {n: a.get("preamble") for n, a in (live.get("agents") or {}).items()}
    changed += [
        f"role:{n}"
        for n in sorted(roles_old.keys() | roles_new.keys())
        if roles_old.get(n) != roles_new.get(n)
    ]
    return changed


class Environment:
    """Where a service runs plans: the project, the backends it can use, the worktrees it makes."""

    def __init__(
        self,
        project: Path | str,
        data_dir: Path | str,
        *,
        home: Path | str | None = None,
        designer_factory: Callable[[str, Path], tuple[Any, bool]] | None = None,
    ) -> None:
        self.project = Path(project).expanduser().resolve()
        self.data_dir = Path(data_dir).expanduser().resolve()
        self.home = Path(home).expanduser() if home is not None else None
        self._designer_factory = designer_factory

    # -- what the plan is designed from ---------------------------------------------------

    def inventory_for(
        self, backend: str, selection: list[dict[str, str]] | None = None
    ) -> dict[str, Any]:
        return build_inventory(backend, self.project, selection=selection, home=self.home)

    def designer_for(self, backend: str) -> tuple[Any, bool]:
        """``(designer, named_agents)`` running the three design roles on ``backend``."""
        if self._designer_factory is not None:
            return self._designer_factory(backend, self.project)
        from agentflow.workflow.design import backend_designer, claude_designer
        from agentflow.workflow.runner import make_backend

        if backend == "claude_code":
            return claude_designer(self.project), True
        entry = {"backend": backend, "agent": GENERIC_AGENT[backend]}
        return backend_designer(
            lambda role: make_backend(entry, {"permission": "read_only"}, cwd=self.project)
        ), False

    # -- where the plan runs ----------------------------------------------------------------

    def _git_root(self) -> Path | None:
        done = subprocess.run(  # noqa: S603 - argv list, no shell
            ["git", "-C", str(self.project), "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=False,
        )
        return Path(done.stdout.strip()) if done.returncode == 0 and done.stdout.strip() else None

    def prepare_workspace(self, run_id: str, backend: str) -> dict[str, Any]:
        """A fresh worktree on its own branch for one run, with the project's extension files
        copied over (including uncommitted ones). Falls back to the project itself, and says so."""

        def in_place(note: str) -> dict[str, Any]:
            return {"kind": "directory", "path": str(self.project), "branch": None, "note": note}

        root = self._git_root()
        if root is None:
            return in_place("not a git repository: the agents work in the project directory itself")
        rel = self.project.relative_to(root)
        path = self.data_dir / "worktrees" / run_id
        branch = f"agentflow/{run_id}"
        made = subprocess.run(  # noqa: S603 - argv list, no shell
            ["git", "-C", str(root), "worktree", "add", "-b", branch, str(path), "HEAD"],
            capture_output=True,
            text=True,
            check=False,
        )
        if made.returncode != 0:
            return in_place(
                f"could not create a worktree ({made.stderr.strip()[:160]}); using the project"
            )
        base = subprocess.run(  # noqa: S603 - argv list, no shell
            ["git", "-C", str(path), "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
        target = path / rel
        self._overlay_extensions(backend, target)
        return {
            "kind": "worktree",
            "path": str(target),
            "root": str(path),
            "branch": branch,
            "base": base,
            "note": "its own branch; your checkout is untouched",
        }

    def _overlay_extensions(self, backend: str, target: Path) -> None:
        """Copy the project-level extension files into the worktree, so the agent there sees what
        the user sees now (a worktree has only what is committed)."""
        for _kind, scope, base, pattern in _SOURCES[backend]:
            if scope != "project":
                continue
            source_root = self.project / base
            if not source_root.is_dir():
                continue
            for src in source_root.glob(pattern):
                if not src.is_file():
                    continue
                dest = target / base / src.relative_to(source_root)
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(src.read_bytes())
