# Copyright (C) 2026 Daniel Sorkin
#
# This file is part of agent-workflow-sdk.
#
# agent-workflow-sdk is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation, version 3.
#
# See the LICENSE file for the full license text.

"""Durable file checkpointer: one directory per thread, one JSON per step.

Writes are atomic (temp file + ``os.replace``) so a reader in another process
never sees a partial checkpoint. State must be JSON-serializable; a channel
holding a non-JSON value will raise at ``put`` time, which is the right place
to find out.
"""

from __future__ import annotations

import contextlib
import dataclasses
import json
import os
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path

from agentflow.checkpoint._serde import from_payload, to_payload
from agentflow.checkpoint.base import Checkpoint, ThreadInfo
from agentflow.errors import CheckpointConflict, CheckpointError
from agentflow.redaction import Redactor, redact_none

__all__ = ["FileCheckpointer"]


def _chmod(path: Path, mode: int) -> None:
    """Best-effort permission set; ignored on filesystems that don't support it."""
    with contextlib.suppress(OSError):
        path.chmod(mode)


class FileCheckpointer:
    """Persist checkpoints under ``root/<thread>/<step>.json``.

    Single-writer contract: this checkpointer is safe for one process writing a
    given thread at a time. Individual file writes are atomic (temp + replace),
    so a *reader* never sees a partial file, but there is no cross-process lock
    or revision check — two processes writing the same thread can interleave
    steps and corrupt history. For multi-process or concurrent-runner durability
    use :class:`~agentflow.checkpoint.sqlite.SqliteCheckpointer`, which enforces
    atomic per-(thread, step) revisions in a transaction.

    Files and directories are created owner-only (0o600/0o700) by default so a
    persisted run's state is not world-readable. Pass ``secure_permissions=
    False`` to disable (e.g. on filesystems that don't support it).

    ``redact`` optionally masks sensitive values in the persisted ``state``
    before writing. WARNING: a redacted checkpoint cannot be resumed to the
    exact original state — masked values are lost. Use redaction only for
    archival/debug copies, not for the checkpointer you resume from. Default is
    no redaction.
    """

    def __init__(
        self,
        root: Path | str = ".runs",
        *,
        redact: Redactor | None = None,
        secure_permissions: bool = True,
    ) -> None:
        self.root = Path(root)
        self._redact = redact or redact_none
        self._secure = secure_permissions

    def _thread_dir(self, thread: str) -> Path:
        # Keep thread ids filesystem-safe without inventing a scheme: reject
        # separators rather than silently mangling them.
        if "/" in thread or "\\" in thread or thread in ("", ".", ".."):
            raise CheckpointError(f"unsafe thread id {thread!r}")
        return self.root / thread

    async def put(self, cp: Checkpoint, *, if_revision: int | None = None) -> str:
        directory = self._thread_dir(cp.thread)
        directory.mkdir(parents=True, exist_ok=True)
        if self._secure:
            _chmod(directory, 0o700)
            _chmod(self.root, 0o700)
        path = directory / f"{cp.step:06d}.json"
        current = _read_revision(path)
        if if_revision is not None and current != if_revision:
            raise CheckpointConflict(cp.thread, cp.step, if_revision, current)
        payload = to_payload(cp)
        payload["state"] = self._redact(payload["state"])
        payload["interrupt_payload"] = self._redact(payload["interrupt_payload"])
        envelope = {"revision": current + 1, "payload": payload}
        try:
            data = json.dumps(envelope, ensure_ascii=False, indent=2)
        except TypeError as exc:
            raise CheckpointError(
                f"checkpoint state for thread {cp.thread!r} is not JSON-serializable: {exc}"
            ) from exc
        _atomic_write(path, data)
        if self._secure:
            _chmod(path, 0o600)
        return f"{cp.thread}:{cp.step}"

    async def get(self, thread: str, step: int | None = None) -> Checkpoint | None:
        directory = self._thread_dir(thread)
        if not directory.is_dir():
            return None
        files = sorted(directory.glob("*.json"))
        if not files:
            return None
        if step is None:
            target = files[-1]
        else:
            target = directory / f"{step:06d}.json"
            if not target.exists():
                return None
        return _load(target)

    async def history(self, thread: str) -> AsyncIterator[Checkpoint]:
        directory = self._thread_dir(thread)
        if not directory.is_dir():
            return
        for path in sorted(directory.glob("*.json")):
            yield _load(path)

    async def delete_thread(self, thread: str) -> None:
        directory = self._thread_dir(thread)
        if not directory.is_dir():
            return
        for path in directory.glob("*.json"):
            path.unlink(missing_ok=True)
        with contextlib.suppress(OSError):
            directory.rmdir()

    async def prune(
        self, thread: str, *, before_step: int | None = None, older_than: str | None = None
    ) -> int:
        directory = self._thread_dir(thread)
        if not directory.is_dir():
            return 0
        removed = 0
        for path in sorted(directory.glob("*.json")):
            step = int(path.stem)
            if before_step is not None and step >= before_step:
                continue
            if older_than is not None:
                env = json.loads(path.read_text(encoding="utf-8"))
                ts = env.get("payload", env).get("ts", "")
                if ts >= older_than:
                    continue
            path.unlink(missing_ok=True)
            removed += 1
        return removed

    async def list_threads(self, *, limit: int = 100, offset: int = 0) -> list[ThreadInfo]:
        if not self.root.is_dir():
            return []
        infos: list[ThreadInfo] = []
        for directory in sorted(p for p in self.root.iterdir() if p.is_dir()):
            files = sorted(directory.glob("*.json"))
            if not files:
                continue
            cp = _load(files[-1])
            infos.append(
                ThreadInfo(
                    thread=directory.name,
                    latest_step=cp.step,
                    interrupted=cp.interrupted,
                    done=cp.done,
                    ts=cp.ts,
                )
            )
        return infos[offset : offset + limit]


def _read_revision(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        env = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return 0
    return int(env.get("revision", 0)) if isinstance(env, dict) else 0


def _load(path: Path) -> Checkpoint:
    env = json.loads(path.read_text(encoding="utf-8"))
    # New format: {"revision": N, "payload": {...}}. Legacy: bare payload dict.
    if isinstance(env, dict) and "payload" in env and "revision" in env:
        return dataclasses.replace(from_payload(env["payload"]), revision=int(env["revision"]))
    return from_payload(env)


def _atomic_write(path: Path, data: str) -> None:
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(data)
            handle.write("\n")
        os.replace(tmp, path)
    finally:
        Path(tmp).unlink(missing_ok=True)
