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

    def add_node(self, name: str, fn: "Node | Any") -> "Graph":
        """Register a node.

        ``fn`` is an async ``(state, ctx) -> update`` callable, or a
        :class:`~agentflow.compiled.CompiledGraph` to embed as a subgraph. A
        subgraph runs on an isolated checkpoint sub-thread, receives the parent
        state restricted to the channels it declares, and its final state is
        merged back as this node's update (only keys that are parent channels).
        """
        if not isinstance(name, str) or not name:
            raise CompilationError("node name must be a non-empty string")
        if name in self.nodes:
            raise CompilationError(f"duplicate node {name!r}")
        if name in ("START", "END"):
            raise CompilationError(f"{name!r} is a reserved node name")

        from agentflow.compiled import CompiledGraph

        if isinstance(fn, CompiledGraph):
            self.nodes[name] = _subgraph_node(name, fn, parent_channels=self.channels)
        else:
            self.nodes[name] = fn
        return self

    def add_subgraph(
        self,
        name: str,
        subgraph: Any,
        *,
        input_map: Mapping[str, str] | None = None,
        output_map: Mapping[str, str] | None = None,
    ) -> "Graph":
        """Embed a :class:`~agentflow.compiled.CompiledGraph` with explicit
        channel mapping between parent and child.

        ``input_map`` maps parent channel -> subgraph channel for the values
        fed in; ``output_map`` maps subgraph channel -> parent channel for the
        values merged back. When omitted, channels shared by name pass through.
        Explicit maps let you avoid double-counting accumulator channels
        (``add``/``append``) by routing the subgraph's result into a distinct
        parent channel.
        """
        if name in self.nodes:
            raise CompilationError(f"duplicate node {name!r}")
        self.nodes[name] = _subgraph_node(
            name, subgraph, parent_channels=self.channels,
            input_map=input_map, output_map=output_map,
        )
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

    def compile(self, *, checkpointer: Any = None, step_limit: int = 100, hooks: Any = None,
                max_node_concurrency: int | None = None, isolate_state: str = "fanout"):
        """Validate the graph and return a :class:`CompiledGraph`.

        ``hooks`` is an optional :class:`~agentflow.observability.Hooks` for
        lifecycle callbacks / metrics. Imports :mod:`agentflow.compiled` lazily
        so the builder module has no import cycle with the runtime.
        """
        if isolate_state not in ("fanout", "always", "never"):
            raise CompilationError(
                f"isolate_state must be 'fanout', 'always', or 'never', "
                f"got {isolate_state!r}"
            )
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
            hooks=hooks,
            max_node_concurrency=max_node_concurrency,
            isolate_state=isolate_state,
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


# ---------------------------------------------------------------------------
# Subgraph-as-node
# ---------------------------------------------------------------------------

def _subgraph_node(
    name: str,
    subgraph: Any,
    *,
    parent_channels: Mapping[str, Channel],
    input_map: Mapping[str, str] | None = None,
    output_map: Mapping[str, str] | None = None,
):
    """Wrap a CompiledGraph so it behaves as a single async node.

    - Input: parent state routed into subgraph channels. With ``input_map``
      (parent -> sub), only those are passed; otherwise channels shared by
      name pass through.
    - Execution: on an isolated checkpoint sub-thread derived from the parent
      thread and step, so subgraph checkpoints never collide with the parent's.
    - Output: subgraph final state routed back to parent channels. With
      ``output_map`` (sub -> parent), only those are merged; otherwise channels
      shared by name are merged via the parent's reducers.

    Default (no maps) semantics: channels shared by name flow both ways. For
    accumulator channels (``add``/``append``) this double-counts the value the
    subgraph inherited from the parent — use ``output_map`` to route the result
    into a distinct parent channel when that matters.
    """
    sub_channels = set(subgraph._graph.channels)
    parent_names = set(parent_channels)

    async def run_subgraph(state: Mapping[str, Any], ctx: Any) -> Mapping[str, Any]:
        if input_map is not None:
            sub_input = {
                sub_key: state[p_key]
                for p_key, sub_key in input_map.items()
                if p_key in state
            }
        else:
            sub_input = {k: v for k, v in state.items() if k in sub_channels}

        sub_thread = f"{ctx.thread}::{name}@{ctx.step}"
        final = await subgraph.invoke(sub_input, thread=sub_thread)

        if output_map is not None:
            return {
                p_key: final[sub_key]
                for sub_key, p_key in output_map.items()
                if sub_key in final and p_key in parent_names
            }
        return {k: v for k, v in final.items() if k in parent_names}

    run_subgraph.__name__ = f"subgraph[{name}]"
    return run_subgraph
