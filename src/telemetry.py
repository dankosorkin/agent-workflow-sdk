"""
Telemetry — append-only JSON Lines log, one file per run.

Each entry is a flat JSON object with timestamp, run_id, event name,
and arbitrary data fields. The orchestrator and runner call record()
for structured events and log() for human-readable progress lines.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class Telemetry:
    """Writes one .jsonl file per run to telemetry_dir."""

    def __init__(self, run_id: str, telemetry_dir: Path):
        self.run_id = run_id
        self.path = telemetry_dir / f"{run_id}.jsonl"
        telemetry_dir.mkdir(parents=True, exist_ok=True)

    def record(self, event: str, data: dict[str, Any]) -> None:
        """Append a structured event to the telemetry file."""
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "run_id": self.run_id,
            "event": event,
            **data,
        }
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def log(self, msg: str) -> None:
        """Print to stdout and record as a log event."""
        print(msg, flush=True)
        self.record("log", {"message": msg})
