"""Sensitive-data controls: redaction + owner-only file permissions."""

from __future__ import annotations

import json
import stat
import sys
from typing import Annotated

import pytest
from agentflow import (
    END,
    START,
    Graph,
    JsonlTelemetry,
    RedactKeys,
    State,
    last,
)
from agentflow.checkpoint.base import Checkpoint
from agentflow.checkpoint.file import FileCheckpointer


class S(State):
    api_key: Annotated[str, last]
    note: Annotated[str, last]


# --- RedactKeys unit behavior ---


def test_redact_masks_sensitive_keys():
    r = RedactKeys()
    out = r(
        {"api_key": "sk-secret", "note": "ok", "nested": {"authorization": "Bearer x", "keep": 1}}
    )
    assert out["api_key"] == "***REDACTED***"
    assert out["note"] == "ok"
    assert out["nested"]["authorization"] == "***REDACTED***"
    assert out["nested"]["keep"] == 1


def test_redact_walks_lists_and_does_not_mutate():
    r = RedactKeys()
    src = {"items": [{"token": "abc"}, {"x": 2}]}
    out = r(src)
    assert out["items"][0]["token"] == "***REDACTED***"
    assert out["items"][1]["x"] == 2
    assert src["items"][0]["token"] == "abc"  # original untouched


def test_redact_case_insensitive_substring():
    r = RedactKeys()
    out = r({"OPENAI_API_KEY": "x", "MY_SECRET_VALUE": "y", "plain": "z"})
    assert out["OPENAI_API_KEY"] == "***REDACTED***"
    assert out["MY_SECRET_VALUE"] == "***REDACTED***"
    assert out["plain"] == "z"


# --- telemetry redaction end-to-end ---


async def test_telemetry_redacts_backend_event_payload(tmp_path):
    tel = JsonlTelemetry(tmp_path, redact=RedactKeys())

    g = Graph(S)

    async def leak(state, ctx):
        # A tool-call-like event whose args carry a secret.
        from agentflow.events import ToolCall

        ctx.emit(ToolCall(id="1", name="login", args={"api_key": "sk-LEAK"}))
        return {"note": "done"}

    g.add_node("leak", leak)
    g.add_edge(START, "leak")
    g.add_edge("leak", END)
    await g.compile(hooks=tel).invoke({"note": ""}, thread="t")

    import asyncio

    await asyncio.sleep(0.05)  # let fire-and-forget event hook flush

    text = (tmp_path / "t.jsonl").read_text()
    assert "sk-LEAK" not in text
    assert "***REDACTED***" in text


# --- checkpoint permissions + redaction ---


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX permissions")
async def test_checkpoint_files_are_owner_only(tmp_path):
    cp = FileCheckpointer(tmp_path / "runs")
    await cp.put(Checkpoint(thread="t", step=1, state={"note": "x"}, next=("a",)))
    f = next((tmp_path / "runs" / "t").glob("*.json"))
    mode = stat.S_IMODE(f.stat().st_mode)
    assert mode == 0o600
    dir_mode = stat.S_IMODE((tmp_path / "runs" / "t").stat().st_mode)
    assert dir_mode == 0o700


async def test_checkpoint_redaction_masks_state(tmp_path):
    cp = FileCheckpointer(tmp_path / "runs", redact=RedactKeys())
    await cp.put(
        Checkpoint(thread="t", step=1, state={"api_key": "sk-SECRET", "note": "keep"}, next=())
    )
    f = next((tmp_path / "runs" / "t").glob("*.json"))
    data = json.loads(f.read_text())
    assert data["state"]["api_key"] == "***REDACTED***"
    assert data["state"]["note"] == "keep"


async def test_checkpoint_no_redaction_by_default(tmp_path):
    cp = FileCheckpointer(tmp_path / "runs")
    await cp.put(Checkpoint(thread="t", step=1, state={"api_key": "sk-KEPT"}, next=()))
    f = next((tmp_path / "runs" / "t").glob("*.json"))
    # Default: resumable fidelity — nothing masked.
    assert "sk-KEPT" in f.read_text()
