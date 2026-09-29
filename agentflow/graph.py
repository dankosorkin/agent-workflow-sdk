"""Graph builder: nodes, edges, conditional routing, and compilation.

A :class:`Graph` is a mutable builder. You register nodes and edges, then
call :meth:`Graph.compile` to validate the structure and get an immutable,
async-runnable :class:`~agentflow.compiled.CompiledGraph`.

Execution semantics (super-steps, fan-out, reducers) live in
:mod:`agentflow.runtime`; this module is only structure + validation.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Mapping, Sequence, Union

from agentflow.errors import CompilationError
from agentflow.state import Channel, channels_from_schema

__all__ = ["START", "END", "Graph", "Node", "Router"]


# ---------------------------------------------------------------------------
# Sentinels for the virtual entry/exit nodes
# ---------------------------------------------------------------------------

class _Sentinel:
    def __init__(self, name: str) -> None:
        self._name = name

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return self._name

    def __reduce__(self):  # keep identity stable across pickling
        return (_resolve_sentinel, (self._name,))


START = _Sentinel("START")
END = _Sentinel("END")


def _resolve_sentinel(name: str) -> _Sentinel:
    return {"START": START, "END": END}[name]


# A node is an async callable (state, ctx) -> partial update (or None).
# ``ctx`` is agentflow.runtime.Context, imported lazily to avoid a cycle.
Node = Callable[[Mapping[str, Any], Any], Awaitable[Mapping[str, Any] | None]]

# A router maps the current state to the next target key(s). Sync or async.
Target = Union[str, "_Sentinel"]
Router = Callable[[Mapping[str, Any]], Any]


class _ConditionalEdge:
    """A router plus its key->target mapping, attached to a source node."""

    __slots__ = ("router", "mapping")

    def __init__(self, router: Router, mapping: Mapping[str, Target]):
        self.router = router
        self.mapping = dict(mapping)


class Graph:
    """Mutable builder for a workflow graph over a :class:`State` schema."""

    def __init__(self, schema: type):
        self.schema = schema
        self.channels: dict[str, Channel] = channels_from_schema(schema)
        self.nodes: dict[str, Node] = {}
        # static edges: src -> list of targets (str or END)
        self._edges: dict[Target, list[Target]] = {}
        # conditional edges: src -> _ConditionalEdge
        self._branches: dict[str, _ConditionalEdge] = {}

    # ------------------------------------------------------------------
    # Building
    # ------------------------------------------------------------------

    def add_node(self, name: str, fn: Node) -> "Graph":
        if not isinstance(name, str) or not name:
            raise CompilationError("node name must be a non-empty string")
        if name in self.nodes:
            raise CompilationError(f"duplicate node {name!r}")
        if name in ("START", "END"):
            raise CompilationError(f"{name!r} is a reserved node name")
        self.nodes[name] = fn
        return self

    def add_edge(self, src: Target, dst: Target) -> "Graph":
        """Add an unconditional edge ``src -> dst``.

        ``src`` may be :data:`START` or a node name; ``dst`` may be a node name
        or :data:`END`.
        """
        self._edges.setdefault(src, []).append(dst)
        return self

    def add_conditional_edges(
        self,
        src: str,
        router: Router,
        mapping: Mapping[str, Target],
    ) -> "Graph":
        """Route out of ``src`` by a ``router(state) -> key`` into ``mapping``.

        The router may return a single key or a list of keys (fan-out). Each
        key must be present in ``mapping``; each mapped target must be a known
        node or :data:`END`. Both checked at :meth:`compile` time.
        """
        if src in self._branches:
            raise CompilationError(f"node {src!r} already has conditional edges")
        self._branches[src] = _ConditionalEdge(router, mapping)
        return self

    # ------------------------------------------------------------------
    # Compilation
    # ------------------------------------------------------------------

    def compile(self, *, checkpointer: Any = None, step_limit: int = 100):
        """Validate the graph and return a :class:`CompiledGraph`.

        Imports :mod:`agentflow.compiled` lazily so the builder module has no
        import cycle with the runtime.
        """
        self._validate()
        from agentflow.compiled import CompiledGraph

        return CompiledGraph(
            schema=self.schema,
            channels=dict(self.channels),
            nodes=dict(self.nodes),
            edges={k: list(v) for k, v in self._edges.items()},
            branches=dict(self._branches),
            checkpointer=checkpointer,
            step_limit=step_limit,
        )

    def _validate(self) -> None:
        known: set[Target] = set(self.nodes) | {END}

        # Every static edge must connect known endpoints; START may be a src.
        for src, targets in self._edges.items():
            if src is not START and src not in self.nodes:
                raise CompilationError(f"edge from unknown node {src!r}")
            for dst in targets:
                if dst not in known:
                    raise CompilationError(f"edge to unknown target {dst!r}")

        # Conditional edges: source must be a node; targets must be known.
        for src, branch in self._branches.items():
            if src not in self.nodes:
                raise CompilationError(f"conditional edges from unknown node {src!r}")
            for key, dst in branch.mapping.items():
                if dst not in known:
                    raise CompilationError(
                        f"conditional edge {src!r}[{key!r}] -> unknown target {dst!r}"
                    )

        # There must be at least one entry edge from START.
        start_targets = self._edges.get(START, [])
        if not start_targets:
            raise CompilationError("graph has no edge from START")
        for dst in start_targets:
            if dst not in self.nodes:
                raise CompilationError(f"START -> unknown node {dst!r}")

        # Every node must be reachable from START.
        self._check_reachability(start_targets)

        # Every node must have an outgoing edge (static or conditional) so it
        # cannot silently dead-end without reaching END.
        for name in self.nodes:
            has_static = name in self._edges and len(self._edges[name]) > 0
            has_branch = name in self._branches
            if not has_static and not has_branch:
                raise CompilationError(
                    f"node {name!r} has no outgoing edge (add an edge to END "
                    f"if it is terminal)"
                )

    def _check_reachability(self, start_targets: Sequence[Target]) -> None:
        adjacency: dict[Target, set[Target]] = {}
        for src, targets in self._edges.items():
            adjacency.setdefault(src, set()).update(targets)
        for src, branch in self._branches.items():
            adjacency.setdefault(src, set()).update(branch.mapping.values())

        seen: set[Target] = set()
        stack: list[Target] = list(start_targets)
        while stack:
            node = stack.pop()
            if node in seen or node is END:
                continue
            seen.add(node)
            stack.extend(adjacency.get(node, ()))

        unreachable = set(self.nodes) - seen
        if unreachable:
            raise CompilationError(
                f"nodes unreachable from START: {sorted(unreachable)}"
            )
