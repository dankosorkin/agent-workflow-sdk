"""Checkpointing: durable run state, resume, and time-travel."""

from typing import TYPE_CHECKING

from agentflow.checkpoint.base import Checkpoint, Checkpointer
from agentflow.checkpoint.file import FileCheckpointer
from agentflow.checkpoint.memory import MemoryCheckpointer
from agentflow.checkpoint.sqlite import SqliteCheckpointer

if TYPE_CHECKING:  # for type checkers only; the runtime import is lazy below
    from agentflow.checkpoint.redis import RedisCheckpointer

__all__ = [
    "Checkpoint",
    "Checkpointer",
    "MemoryCheckpointer",
    "FileCheckpointer",
    "SqliteCheckpointer",
    "RedisCheckpointer",
]


def __getattr__(name: str):
    # RedisCheckpointer needs the optional `redis` extra; import it lazily so
    # `import agentflow.checkpoint` works without redis installed.
    if name == "RedisCheckpointer":
        from agentflow.checkpoint.redis import RedisCheckpointer

        return RedisCheckpointer
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
