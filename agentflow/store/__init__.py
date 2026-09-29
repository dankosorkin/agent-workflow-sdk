"""Store: durable, cross-thread application memory (distinct from checkpoints).

A checkpointer persists one thread's execution state for resume; a store holds
durable key-value data shared across threads (profiles, facts, long-term
memory). See :mod:`agentflow.store.base` for the protocol.
"""

from typing import TYPE_CHECKING

from agentflow.store.base import Item, Store
from agentflow.store.memory import MemoryStore

if TYPE_CHECKING:  # for type checkers only; the runtime import is lazy below
    from agentflow.store.postgres import PostgresStore

__all__ = ["Item", "Store", "MemoryStore", "PostgresStore"]


def __getattr__(name: str):
    # PostgresStore needs the optional `postgres` extra; import it lazily so
    # `import agentflow.store` works without asyncpg installed.
    if name == "PostgresStore":
        from agentflow.store.postgres import PostgresStore

        return PostgresStore
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
