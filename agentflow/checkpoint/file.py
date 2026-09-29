"""Durable file checkpointer: one directory per thread, one JSON per step.

Writes are atomic (temp file + ``os.replace``) so a reader in another process
never sees a partial checkpoint. State must be JSON-serializable; a channel
holding a non-JSON value will raise at ``put`` time, which is the right place
to find out.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, AsyncIterator

from agentflow.checkpoint.base import Checkpoint
from agentflow.errors import CheckpointError
from agentflow.redaction import Redactor, redact_none

__all__ = ["FileCheckpointer"]


def _chmod(path: Path, mode: int) -> None:
    """Best-effort permission set; ignored on filesystems that don't support it."""
    try:
        path.chmod(mode)
    except OSError:
        pass


class FileCheckpointer:
    """Persist checkpoints under ``root/<thread>/<step>.json``.

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
        redact: "Redactor | None" = None,
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

    async def put(self, cp: Checkpoint) -> str:
        directory = self._thread_dir(cp.thread)
        directory.mkdir(parents=True, exist_ok=True)
        if self._secure:
            _chmod(directory, 0o700)
            _chmod(self.root, 0o700)
        path = directory / f"{cp.step:06d}.json"
        payload = _to_payload(cp)
        payload["state"] = self._redact(payload["state"])
        payload["interrupt_payload"] = self._redact(payload["interrupt_payload"])
        try:
            data = json.dumps(payload, ensure_ascii=False, indent=2)
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
        return _from_payload(json.loads(target.read_text(encoding="utf-8")))

    async def history(self, thread: str) -> AsyncIterator[Checkpoint]:
        directory = self._thread_dir(thread)
        if not directory.is_dir():
            return
        for path in sorted(directory.glob("*.json")):
            yield _from_payload(json.loads(path.read_text(encoding="utf-8")))


def _to_payload(cp: Checkpoint) -> dict[str, Any]:
    return {
        "thread": cp.thread,
        "step": cp.step,
        "state": dict(cp.state),
        "next": list(cp.next),
        "parent": cp.parent,
        "ts": cp.ts,
        "interrupted": cp.interrupted,
        "interrupt_node": cp.interrupt_node,
        "interrupt_payload": cp.interrupt_payload,
        "extra": dict(cp.extra),
    }


def _from_payload(d: dict[str, Any]) -> Checkpoint:
    return Checkpoint(
        thread=d["thread"],
        step=d["step"],
        state=d["state"],
        next=tuple(d.get("next", ())),
        parent=d.get("parent"),
        ts=d.get("ts", ""),
        interrupted=d.get("interrupted", False),
        interrupt_node=d.get("interrupt_node"),
        interrupt_payload=d.get("interrupt_payload"),
        extra=d.get("extra", {}),
    )


def _atomic_write(path: Path, data: str) -> None:
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(data)
            handle.write("\n")
        os.replace(tmp, path)
    finally:
        Path(tmp).unlink(missing_ok=True)
