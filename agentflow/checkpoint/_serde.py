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

"""Shared checkpoint (de)serialization used by the file/sqlite/redis backends.

Keeping one implementation avoids drift between the persistent checkpointers —
they all store the same JSON-able payload shape for a :class:`Checkpoint`.
"""

from __future__ import annotations

from typing import Any

from agentflow.checkpoint.base import Checkpoint

__all__ = ["to_payload", "from_payload"]


def to_payload(cp: Checkpoint) -> dict[str, Any]:
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


def from_payload(d: dict[str, Any]) -> Checkpoint:
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
