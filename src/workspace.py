"""
Generic workspace helpers.

Small, task-agnostic utilities that Task implementations can reuse to
manage their working directory. No task-specific paths or file names
live here — each Task decides its own layout and calls these helpers.
"""

import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any


# Canonical project locations, shared by all tasks. Individual tasks build
# their own file paths relative to these instead of recomputing the roots.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
WORKSPACE = PROJECT_ROOT / "agent_workspace"
BEST_DIR = PROJECT_ROOT / "best"


def ensure_dirs(*dirs: Path) -> None:
    """Create each directory (and parents) if missing."""
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)


def clean_files(*files: Path) -> None:
    """Delete each file if it exists. Silently ignores missing files."""
    for f in files:
        if f.exists():
            f.unlink()


def copy_into(src: Path, dst: Path) -> None:
    """Copy src to dst, creating dst's parent directory if needed."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def copy_into_atomic(src: Path, dst: Path) -> None:
    """Copy ``src`` into ``dst`` without exposing a partial destination."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{dst.name}.", dir=dst.parent)
    os.close(fd)
    try:
        shutil.copy2(src, temporary)
        Path(temporary).replace(dst)
    finally:
        Path(temporary).unlink(missing_ok=True)


def write_json_atomic(path: Path, data: dict[str, Any]) -> None:
    """Write one JSON object atomically for readers in another process."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        Path(temporary).replace(path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def read_json(path: Path) -> dict[str, Any] | None:
    """
    Read and parse a JSON file.

    Returns None if the file is missing or contains invalid JSON,
    so callers can treat either case uniformly.
    """
    if not path.exists():
        return None
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
        return parsed if isinstance(parsed, dict) else None
    except (json.JSONDecodeError, OSError):
        return None
