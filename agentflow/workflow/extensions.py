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

"""What a user can add to an agent backend, and whether the backend really picks it up.

The SDK does not paste steering files or skills into prompts. The backend is the one that
understands them: put a steering file, a skill or an ``AGENTS.md`` where the backend looks, and
the agent applies it. This module does two things around that:

* :func:`discover` lists, read-only, what exists for a backend: *steering* (rules the agent
  follows), *skills* (capabilities it can load) and *instructions* (``AGENTS.md`` /
  ``CLAUDE.md``). Only names, descriptions and paths, never the content.
* :func:`probe_backend` proves the pickup. It puts canary files with a random token into an
  empty temporary directory, runs the real agent there and checks the token comes back. A
  backend that ignores a file is reported as such, not assumed to work.

Nothing here writes outside the temporary directory a probe creates and then removes.
"""

from __future__ import annotations

import asyncio
import hashlib
import secrets
import shutil
import subprocess
import tempfile
import time
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

__all__ = [
    "BACKENDS",
    "Extension",
    "ProbeResult",
    "available_backends",
    "discover",
    "fingerprint",
    "parse_front_matter",
    "probe_backend",
]

#: backend name -> the executable it needs.
BACKENDS = {"kiro": "kiro-cli", "claude_code": "claude", "codex": "codex"}

#: The agent a backend runs when the plan names none. Kiro's v3 engine needs a mode id: ``vibe``
#: is its built-in general-purpose one (``spec``, ``plan``... are the other built-ins).
GENERIC_AGENT = {"kiro": "vibe", "claude_code": None, "codex": None}

#: Where each backend looks, as ``(kind, scope, base, glob)``. ``base`` is relative to the user's
#: home (``user``) or to the working directory (``project``); a project entry wins over a user
#: entry of the same name, the way the backends themselves resolve them.
_SOURCES: dict[str, list[tuple[str, str, str, str]]] = {
    "kiro": [
        ("steering", "user", ".kiro/steering", "*.md"),
        ("steering", "project", ".kiro/steering", "*.md"),
        ("skills", "user", ".kiro/skills", "*/SKILL.md"),
        ("skills", "project", ".kiro/skills", "*/SKILL.md"),
        ("instructions", "project", ".", "AGENTS.md"),
    ],
    "claude_code": [
        ("instructions", "user", ".claude", "CLAUDE.md"),
        ("instructions", "project", ".", "CLAUDE.md"),
        ("instructions", "project", ".claude", "CLAUDE.md"),
        ("skills", "user", ".claude/skills", "*/SKILL.md"),
        ("skills", "project", ".claude/skills", "*/SKILL.md"),
    ],
    "codex": [
        ("instructions", "user", ".codex", "AGENTS.md"),
        ("instructions", "project", ".", "AGENTS.md"),
        ("skills", "user", ".codex/skills", "*/SKILL.md"),
    ],
}

_IGNORED_SUFFIXES = (".bak", ".example", ".orig", ".tmp")


@dataclass(frozen=True)
class Extension:
    """One file the backend may load. ``digest`` is a short hash of its bytes (for fingerprints)."""

    kind: str  # steering | skills | instructions
    name: str
    description: str
    path: str
    scope: str  # user | project
    inclusion: str | None  # Kiro steering: always | manual | fileMatch ...
    digest: str
    size: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse_front_matter(text: str) -> tuple[dict[str, str], str]:
    """Split ``---`` fenced ``key: value`` lines from the body. Not YAML: scalars only, plus the
    folded/literal block forms (``>``, ``>-``, ``|``, ``|-``) that skill files use for long
    descriptions. A bare ``tools:`` that opens a list is skipped."""
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}, text
    meta: dict[str, str] = {}
    i = 1
    while i < len(lines):
        line = lines[i]
        if line.strip() == "---":
            return meta, "\n".join(lines[i + 1 :])
        key, sep, value = line.partition(":")
        i += 1
        if not sep or not key.strip() or key.startswith((" ", "\t", "-")):
            continue
        value = value.strip()
        if value in (">", ">-", "|", "|-"):
            block: list[str] = []
            while i < len(lines) and lines[i].startswith((" ", "\t")):
                block.append(lines[i].strip())
                i += 1
            joiner = " " if value.startswith(">") else "\n"
            value = joiner.join(part for part in block if part)
        value = value.strip("\"'")
        if value:  # an empty value opens a list
            meta[key.strip()] = value
    return {}, text  # never closed: treat it all as body


def _first_line(body: str) -> str:
    for line in body.splitlines():
        text = line.strip().lstrip("#").strip()
        if text:
            return text[:160]
    return ""


def _read(path: Path) -> tuple[bytes, str] | None:
    try:
        data = path.read_bytes()
    except OSError:
        return None
    return data, data.decode("utf-8", errors="replace")


def discover(
    backend: str, cwd: Path | str = ".", *, home: Path | str | None = None
) -> dict[str, list[Extension]]:
    """The steering, skills and instructions ``backend`` would find from ``cwd``.

    Read-only. A project file overrides a user file of the same kind and name. Backup and
    example files are skipped. An unknown backend raises ``KeyError``.
    """
    sources = _SOURCES[backend]
    home_dir = Path(home).expanduser() if home is not None else Path.home()
    cwd_dir = Path(cwd).expanduser().resolve()
    found: dict[str, dict[str, Extension]] = {"steering": {}, "skills": {}, "instructions": {}}
    for kind, scope, base, pattern in sources:
        root = (home_dir if scope == "user" else cwd_dir) / base
        if not root.is_dir():
            continue
        for path in sorted(root.glob(pattern)):
            name_part = path.parent.name if pattern.endswith("SKILL.md") else path.stem
            if path.name.endswith(_IGNORED_SUFFIXES) or name_part.startswith("."):
                continue
            read = _read(path)
            if read is None:
                continue
            data, text = read
            meta, body = parse_front_matter(text)
            name = meta.get("name") or name_part
            if kind == "instructions":
                name = f"{path.name} ({scope})"
            found[kind][name] = Extension(
                kind=kind,
                name=name,
                description=meta.get("description") or _first_line(body),
                path=str(path),
                scope=scope,
                inclusion=meta.get("inclusion"),
                digest=hashlib.sha256(data).hexdigest()[:16],
                size=len(data),
            )
    return {k: sorted(v.values(), key=lambda e: e.name.lower()) for k, v in found.items()}


def fingerprint(items: list[Extension]) -> str:
    """One hash for a chosen set of files: it changes when any of them is added, removed, edited."""
    h = hashlib.sha256()
    for e in sorted(items, key=lambda e: (e.kind, e.path)):
        h.update(f"{e.kind}\0{e.path}\0{e.digest}\n".encode())
    return h.hexdigest()[:16]


def _run_version(binary: str) -> str | None:
    try:
        done = subprocess.run(  # noqa: S603 - argv list, no shell
            [binary, "--version"], capture_output=True, text=True, timeout=5, check=False
        )
    except (OSError, subprocess.SubprocessError):
        return None
    text = (done.stdout or done.stderr).strip().splitlines()
    return text[0][:80] if text else None


def available_backends() -> list[dict[str, Any]]:
    """Which backends can run on this machine: the executable is found and answers ``--version``."""
    out = []
    for name, binary in BACKENDS.items():
        path = shutil.which(binary)
        out.append(
            {
                "backend": name,
                "binary": binary,
                "path": path,
                "available": path is not None,
                "version": _run_version(path) if path else None,
            }
        )
    return out


# --- the canary probe ---------------------------------------------------------------------

#: ``(backend, cwd, prompt) -> the agent's answer``; replaceable so tests need no real agent.
ProbeRunner = Callable[[str, Path, str], Awaitable[str]]


@dataclass(frozen=True)
class ProbeResult:
    """Does the backend apply a file of this kind that sits where it looks?"""

    backend: str
    kind: str  # steering | skills | instructions
    status: str  # ok | missing | error | not_probed
    detail: str
    seconds: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _canary_files(backend: str, token_rule: str, token_skill: str) -> dict[str, tuple[str, str]]:
    """``kind -> (relative path, content)`` for the files this backend should pick up."""
    rule = f"Whenever you answer, end your answer with the exact token {token_rule}.\n"
    skill = (
        "---\nname: agentflow-canary\n"
        "description: Use when asked for the agentflow canary phrase.\n---\n\n"
        f"# Canary\n\nThe agentflow canary phrase is {token_skill}.\n"
    )
    if backend == "kiro":
        return {
            "steering": (
                ".kiro/steering/agentflow-canary.md",
                f"---\ninclusion: always\n---\n\n{rule}",
            ),
            "skills": (".kiro/skills/agentflow-canary/SKILL.md", skill),
        }
    if backend == "claude_code":
        return {
            "instructions": ("CLAUDE.md", rule),
            "skills": (".claude/skills/agentflow-canary/SKILL.md", skill),
        }
    return {"instructions": ("AGENTS.md", rule)}  # codex: project skills are not a thing


async def _default_runner(backend: str, cwd: Path, prompt: str) -> str:
    from agentflow.workflow.runner import collect_text, make_backend

    entry = {"backend": backend, "agent": GENERIC_AGENT[backend]}
    return await collect_text(make_backend(entry, {"permission": "read_only"}, cwd=cwd), prompt)


async def probe_backend(
    backend: str, *, runner: ProbeRunner | None = None, timeout: float = 180.0
) -> list[ProbeResult]:
    """Run the real agent in an empty temp directory with canary files and see what it applies.

    The prompt must not contradict the file: a prompt that dictates the whole answer wins over a
    rule, and the probe would blame the backend. One short turn per kind of file. A backend that
    cannot take a kind of file at all (Codex has no project skills) is reported ``not_probed``.
    Costs a few tokens; run it on demand.
    """

    if backend not in BACKENDS:
        raise KeyError(backend)
    run = runner or _default_runner
    token_rule, token_skill = f"CANARY-{secrets.token_hex(4)}", f"PHRASE-{secrets.token_hex(4)}"
    files = _canary_files(backend, token_rule, token_skill)
    results: list[ProbeResult] = []
    tmp = Path(tempfile.mkdtemp(prefix="agentflow-probe-"))
    try:
        for rel, content in files.values():
            target = tmp / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")

        async def ask(kind: str, prompt: str, token: str) -> None:
            t0 = time.monotonic()
            try:
                answer = await asyncio.wait_for(run(backend, tmp, prompt), timeout)
            except Exception as exc:
                status, detail = "error", f"{type(exc).__name__}: {exc}"[:300]
            else:
                hit = token in answer
                status = "ok" if hit else "missing"
                detail = (
                    "the agent applied the file"
                    if hit
                    else f"not applied; it said: {answer[:200]!r}"
                )
            results.append(
                ProbeResult(backend, kind, status, detail, round(time.monotonic() - t0, 1))
            )

        rule_kind = "steering" if "steering" in files else "instructions"
        await ask(rule_kind, "Say hello in one short sentence.", token_rule)
        if "skills" in files:
            await ask(
                "skills",
                "What is the agentflow canary phrase? Answer with the phrase only.",
                token_skill,
            )
        else:
            results.append(
                ProbeResult(backend, "skills", "not_probed", "no project skills here", 0.0)
            )
        if rule_kind == "steering":
            results.append(
                ProbeResult(backend, "instructions", "not_probed", "AGENTS.md not probed", 0.0)
            )
        else:
            results.append(ProbeResult(backend, "steering", "not_probed", "no such concept", 0.0))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    order = {"steering": 0, "instructions": 1, "skills": 2}
    return sorted(results, key=lambda r: order[r.kind])
