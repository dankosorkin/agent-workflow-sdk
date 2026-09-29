"""State model: channels, reducers, and update merging.

A workflow's state is a set of named *channels*. Each channel has a
*reducer*: a pure function ``(current, update) -> new`` that folds a node's
partial update into the current value. Reducers are what make concurrent
node updates in a single super-step merge deterministically instead of
racing on last-writer-wins.

Schemas are declared as a ``TypedDict`` subclass of :class:`State`, with each
field optionally annotated with its reducer::

    from typing import Annotated
    from agentflow.state import State, append, add, last

    class ChatState(State):
        messages: Annotated[list, append]   # concatenate
        tokens: Annotated[int, add]         # sum
        best: Annotated[float, last]        # overwrite (the default)

A field without an explicit reducer uses :func:`last` (last-value-wins).

This module is pure data: no async, no I/O, no engine imports.
"""

from __future__ import annotations

import typing
from typing import Annotated, Any, Callable, Mapping, TypedDict, get_args, get_origin, get_type_hints

__all__ = [
    "State",
    "Reducer",
    "Channel",
    "last",
    "append",
    "add",
    "merge",
    "union",
    "channels_from_schema",
    "apply_update",
    "apply_updates",
    "RESERVED_PREFIX",
    "RESERVED_NAMES",
]

RESERVED_PREFIX = "__"

#: Channel names the runtime injects into state and manages itself. User
#: schemas and node updates may not use these.
RESERVED_NAMES = frozenset({"__step", "__next"})

# A reducer takes the current channel value (or None if unset) and an incoming
# update, and returns the new channel value.
Reducer = Callable[[Any, Any], Any]


# ---------------------------------------------------------------------------
# Base schema marker
# ---------------------------------------------------------------------------

class State(TypedDict, total=False):
    """Base class for a workflow state schema.

    Subclass with ``Annotated[type, reducer]`` fields. Being ``total=False``
    means every channel is optional at the type level — nodes emit partial
    updates, and a channel simply has no value until first written.
    """


# ---------------------------------------------------------------------------
# Built-in reducers
# ---------------------------------------------------------------------------

def last(current: Any, update: Any) -> Any:
    """Last-value-wins. The default reducer."""
    return update


def append(current: Any, update: Any) -> list:
    """Concatenate list updates onto a list channel.

    ``current`` of ``None`` is treated as an empty list. A non-list ``update``
    is appended as a single element; a list ``update`` is extended in order.
    """
    base = list(current) if current is not None else []
    if isinstance(update, (list, tuple)):
        base.extend(update)
    else:
        base.append(update)
    return base


def add(current: Any, update: Any) -> Any:
    """Numeric/additive reducer using ``+``. ``None`` current starts at 0."""
    if current is None:
        return update
    return current + update


def merge(current: Any, update: Any) -> dict:
    """Shallow dict merge; keys in ``update`` win. ``None`` current is ``{}``."""
    base = dict(current) if current is not None else {}
    if update:
        base.update(update)
    return base


def union(current: Any, update: Any) -> set:
    """Set union. ``None`` current is the empty set."""
    base = set(current) if current is not None else set()
    base.update(update if isinstance(update, (set, frozenset, list, tuple)) else {update})
    return base


# ---------------------------------------------------------------------------
# Channel resolution from an annotated schema
# ---------------------------------------------------------------------------

class Channel(typing.NamedTuple):
    """A resolved channel: its name and the reducer that folds its updates."""
    name: str
    reducer: Reducer


def _extract_reducer(annotation: Any) -> Reducer:
    """Pull the reducer out of an ``Annotated[type, reducer]`` hint.

    A bare type (no ``Annotated`` metadata, or metadata without a callable)
    resolves to :func:`last`. The first callable in the metadata is used as
    the reducer, matching the LangGraph convention.
    """
    if get_origin(annotation) is Annotated:
        for meta in get_args(annotation)[1:]:
            if callable(meta):
                return meta
    return last


def channels_from_schema(schema: type) -> dict[str, Channel]:
    """Resolve a :class:`State` subclass into its channels.

    Reads the class's type hints (including ``Annotated`` metadata) and maps
    each field to a :class:`Channel`. Raises :class:`ValueError` if a field
    uses the reserved ``__`` prefix.
    """
    hints = get_type_hints(schema, include_extras=True)
    result: dict[str, Channel] = {}
    for name, annotation in hints.items():
        # A leading-dunder field written directly in a class body is
        # name-mangled by Python (``__step`` -> ``_ClassName__step``), so it
        # can never collide here; but a name reaching us with the reserved
        # prefix (e.g. via functional TypedDict syntax) is rejected outright.
        if name.startswith(RESERVED_PREFIX) or name in RESERVED_NAMES:
            raise ValueError(
                f"channel {name!r} is reserved for the engine "
                f"(no {RESERVED_PREFIX!r} prefix, and not one of {sorted(RESERVED_NAMES)})"
            )
        result[name] = Channel(name=name, reducer=_extract_reducer(annotation))
    return result


# ---------------------------------------------------------------------------
# Update application
# ---------------------------------------------------------------------------

def apply_update(
    state: Mapping[str, Any],
    update: Mapping[str, Any] | None,
    channels: Mapping[str, Channel],
) -> dict[str, Any]:
    """Fold a single node's partial ``update`` into ``state`` via reducers.

    Returns a new dict; ``state`` is not mutated. An update key with no
    declared channel is rejected — nodes may only write declared channels.
    A ``None`` or empty update is a no-op that returns a shallow copy.
    """
    result = dict(state)
    if not update:
        return result
    for key, value in update.items():
        if key.startswith(RESERVED_PREFIX) or key in RESERVED_NAMES:
            raise KeyError(f"node may not write reserved channel {key!r}")
        channel = channels.get(key)
        if channel is None:
            raise KeyError(
                f"node wrote undeclared channel {key!r}; "
                f"declared channels: {sorted(channels)}"
            )
        result[key] = channel.reducer(result.get(key), value)
    return result


def apply_updates(
    state: Mapping[str, Any],
    updates: list[Mapping[str, Any] | None],
    channels: Mapping[str, Channel],
) -> dict[str, Any]:
    """Fold several node updates (one super-step) into ``state`` in order.

    Updates are applied in the given order — the runtime passes them in a
    deterministic node order so that non-commutative reducers (``append``,
    ``last``) are well-defined. Commutative reducers (``add``, ``union``) are
    order-independent regardless.
    """
    result = dict(state)
    for update in updates:
        result = apply_update(result, update, channels)
    return result
