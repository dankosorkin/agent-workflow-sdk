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

"""Checkpointing: durable run state, resume, and time-travel."""

from typing import TYPE_CHECKING

from agentflow.checkpoint.base import Checkpoint, Checkpointer, ThreadInfo
from agentflow.checkpoint.file import FileCheckpointer
from agentflow.checkpoint.memory import MemoryCheckpointer
from agentflow.checkpoint.sqlite import SqliteCheckpointer

if TYPE_CHECKING:  # for type checkers only; the runtime import is lazy below
    from agentflow.checkpoint.postgres import PostgresCheckpointer
    from agentflow.checkpoint.redis import RedisCheckpointer

__all__ = [
    "Checkpoint",
    "Checkpointer",
    "ThreadInfo",
    "MemoryCheckpointer",
    "FileCheckpointer",
    "SqliteCheckpointer",
    "RedisCheckpointer",
    "PostgresCheckpointer",
]


def __getattr__(name: str):
    # These need optional extras (`redis` / `postgres`); import lazily so
    # `import agentflow.checkpoint` works without them installed.
    if name == "RedisCheckpointer":
        from agentflow.checkpoint.redis import RedisCheckpointer

        return RedisCheckpointer
    if name == "PostgresCheckpointer":
        from agentflow.checkpoint.postgres import PostgresCheckpointer

        return PostgresCheckpointer
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
