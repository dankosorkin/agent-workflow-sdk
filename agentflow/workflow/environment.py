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

"""The environment around a plan: system agents, roles, the binding at launch, the worktree.

A plan does not know which provider will run it. It names **roles** (researcher, coder...) and
the permission each step needs; the roles are the neutral vocabulary and live here, in the
engine, together with the three system agents (orchestrator, designer, critic) whose prompts are
in :mod:`agentflow.workflow.design_prompts`. The system agents run on a *system backend* of their
own, in an empty directory and with their full instructions in the prompt, so neither the user's
steering nor the choice of executor changes how plans are designed.

The provider is bound when a run **starts** (:func:`bind_inventory`): the user picks one backend,
every step runs on its generic agent shaped by the role, and the backend applies the user's own
steering, skills and ``AGENTS.md`` / ``CLAUDE.md`` by itself. The files the user chooses to rely on
are recorded with the run, and what happens when one changes is the run's mode:

* **strict**: the run is held (resumable) when a file changes between steps.
* **track**: the run goes on and shows a warning.
* **auto** (default): strict if any step writes (``allowlist``/``full``) or has side effects,
  otherwise track.

An approval covers the plan and the roles it relies on, not a provider: the same approved plan
can run on Kiro today and on Codex tomorrow. Each run gets its own git worktree on its own branch,
so what the agents write never touches the user's checkout; a project that is not a git
repository is used in place, and the run says so.
"""

from __future__ import annotations

import shutil
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
    "bind_inventory",
    "build_catalog",
    "build_inventory",
    "drift",
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


def build_catalog(project: Path | str, *, home: Path | str | None = None) -> dict[str, Any]:
    """The provider-neutral inventory: every role, with the generic agent of each known backend.

    This is what plans are designed, validated and approved against. Its digest covers only what
    the plan depends on (see :func:`~agentflow.workflow.inventory.inventory_digest`).
    """
    roles = load_roles(project, home=home)
    return {
        "neutral": True,
        "agents": {
            name: {
                "description": role.description,
                "permissions": list(role.permissions),
                "backends": [{"backend": b, "agent": GENERIC_AGENT[b]} for b in BACKENDS],
                "preamble": role.preamble,
            }
            for name, role in sorted(roles.items())
        },
    }


def bind_inventory(
    catalog: dict[str, Any],
    backend: str,
    project: Path | str,
    *,
    selection: list[dict[str, str]] | None = None,
    home: Path | str | None = None,
) -> dict[str, Any]:
    """The catalog bound to ONE backend: every role is served by that backend's generic agent
    only (no silent fallback to another provider), plus the extension files the run relies on
    (``selection`` is a list of ``{"kind", "name"}``). The digest equals the catalog's, so an
    approval given on the catalog stays valid for any backend.
    """
    if backend not in BACKENDS:
        raise ValueError(f"unknown backend {backend!r}")
    found = discover(backend, project, home=home)
    chosen: list[dict[str, Any]] = []
    missing: list[str] = []
    for want in selection or []:
        match = next(
            (e for e in found.get(want.get("kind", ""), []) if e.name == want.get("name")), None
        )
        if match is None:
            missing.append(f"{want.get('kind')} {want.get('name')!r}")
            continue
        chosen.append(
            {
                "kind": match.kind,
                "name": match.name,
                "description": match.description,
                "scope": match.scope,
                "digest": match.digest,
            }
        )
    if missing:
        raise ValueError(f"not found for {backend}: {', '.join(missing)}")
    bound = {
        **catalog,
        "agents": {
            name: {**agent, "backends": [{"backend": backend, "agent": GENERIC_AGENT[backend]}]}
            for name, agent in (catalog.get("agents") or {}).items()
        },
        "context": {"backend": backend, "extensions": chosen},
    }
    return bound


def build_inventory(
    backend: str,
    project: Path | str,
    *,
    selection: list[dict[str, str]] | None = None,
    home: Path | str | None = None,
) -> dict[str, Any]:
    """:func:`build_catalog` bound to ``backend`` in one call."""
    return bind_inventory(
        build_catalog(project, home=home), backend, project, selection=selection, home=home
    )


def resolve_mode(mode: str, plan: dict[str, Any]) -> str:
    """``strict`` or ``track``: ``auto`` is strict when any step writes or has side effects."""
    if mode in ("strict", "track"):
        return mode
    for step in (plan.get("graph") or {}).get("steps") or []:
        if step.get("permission", "read_only") != "read_only" or step.get("side_effects"):
            return "strict"
    return "track"


def drift(old: list[dict[str, Any]], new: list[dict[str, Any]]) -> list[str]:
    """``kind:name`` of the extension files that differ between two bindings (changed, added,
    gone). Each argument is a binding's ``context.extensions`` list."""
    before = {(e["kind"], e["name"]): e["digest"] for e in old}
    after = {(e["kind"], e["name"]): e["digest"] for e in new}
    keys = sorted(before.keys() | after.keys())
    return [
        f"{kind}:{name}"
        for kind, name in keys
        if before.get((kind, name)) != after.get((kind, name))
    ]


class Environment:
    """Where a service designs and runs plans: the project, the backends, the system backend, the
    worktrees it makes."""

    #: Which backend the system agents use when none is chosen: the first one installed.
    SYSTEM_PREFERENCE = ("claude_code", "codex", "kiro")

    def __init__(
        self,
        project: Path | str,
        data_dir: Path | str,
        *,
        home: Path | str | None = None,
        system_backend: str | None = None,
        designer_factory: Callable[[str, Path], Any] | None = None,
    ) -> None:
        self.project = Path(project).expanduser().resolve()
        self.data_dir = Path(data_dir).expanduser().resolve()
        self.home = Path(home).expanduser() if home is not None else None
        self.system_backend = system_backend
        self._designer_factory = designer_factory

    # -- the neutral side: what plans are designed from ------------------------------------

    def catalog(self) -> dict[str, Any]:
        return build_catalog(self.project, home=self.home)

    def system_backend_name(self, chosen: str | None = None) -> str:
        """The backend the system agents run on: the one asked for, the configured one, or the
        first installed."""
        name = chosen or self.system_backend
        if name is not None:
            if name not in BACKENDS:
                raise ValueError(f"unknown backend {name!r}")
            return name
        for candidate in self.SYSTEM_PREFERENCE:
            if shutil.which(BACKENDS[candidate]):
                return candidate
        raise ValueError("no agent backend is installed (kiro-cli, claude or codex)")

    def designer_for(self, system_backend: str | None = None) -> Any:
        """The designer running orchestrator, creator and verifier on the system backend.

        They run in an empty directory (the user's project files do not touch them) with their
        whole instructions in the prompt, so no agent file has to be installed anywhere.
        """
        backend = self.system_backend_name(system_backend)
        sandbox = self.data_dir / "system-agents"
        sandbox.mkdir(parents=True, exist_ok=True)
        if self._designer_factory is not None:
            return self._designer_factory(backend, sandbox)
        from agentflow.workflow.design import backend_designer
        from agentflow.workflow.runner import make_backend

        entry = {"backend": backend, "agent": GENERIC_AGENT[backend]}
        return backend_designer(
            lambda role: make_backend(entry, {"permission": "read_only"}, cwd=sandbox)
        )

    # -- the binding: what happens when a run starts ------------------------------------------

    def bind(self, backend: str, selection: list[dict[str, str]] | None = None) -> dict[str, Any]:
        return bind_inventory(
            self.catalog(), backend, self.project, selection=selection, home=self.home
        )

    def preview(
        self,
        plan: dict[str, Any],
        backend: str,
        selection: list[dict[str, str]] | None = None,
        mode: str = "auto",
    ) -> dict[str, Any]:
        """What binding ``plan`` to ``backend`` would mean, without starting anything: whether it
        can run there, and what is missing."""
        from agentflow.workflow.plan import validate_plan
        from agentflow.workflow.runner import unsupported_features

        out: dict[str, Any] = {
            "backend": backend,
            "mode": mode,
            "resolved_mode": resolve_mode(mode, plan),
            "installed": backend in BACKENDS and shutil.which(BACKENDS[backend]) is not None,
            "issues": [],
            "extensions": [],
        }
        if backend not in BACKENDS:
            out["issues"].append(
                {
                    "code": "unknown_backend",
                    "severity": "error",
                    "message": f"unknown backend {backend!r}",
                }
            )
            return out
        if not out["installed"]:
            out["issues"].append(
                {
                    "code": "backend_not_installed",
                    "severity": "error",
                    "message": f"{BACKENDS[backend]} was not found on this machine",
                }
            )
        try:
            bound = self.bind(backend, selection)
        except ValueError as exc:
            out["issues"].append(
                {"code": "extension_missing", "severity": "error", "message": str(exc)}
            )
            return out
        out["extensions"] = bound["context"]["extensions"]
        problems = validate_plan(plan, inventory=bound) + unsupported_features(plan)
        out["issues"] += [i.to_dict() for i in problems]
        return out

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
