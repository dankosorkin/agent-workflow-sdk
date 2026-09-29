"""Exception hierarchy for AgentFlow.

All library exceptions derive from :class:`AgentFlowError` so callers can
catch the whole family with one except clause.
"""

from __future__ import annotations

__all__ = [
    "AgentFlowError",
    "GraphError",
    "CompilationError",
    "NodeError",
    "RunTimeout",
    "BackendError",
    "BackendTransportError",
    "BackendRateLimitError",
    "CheckpointError",
    "InterruptError",
]


class AgentFlowError(Exception):
    """Base class for every AgentFlow exception."""


# ---------------------------------------------------------------------------
# Graph / engine
# ---------------------------------------------------------------------------


class GraphError(AgentFlowError):
    """A problem with graph structure or execution."""


class CompilationError(GraphError):
    """The graph failed validation at ``compile()`` time.

    Raised for unknown edge targets, an unreachable node, a missing entry
    edge from START, a conditional-router mapping to an undefined node, etc.
    """


class NodeError(GraphError):
    """A node raised during execution.

    Wraps the original exception (as ``__cause__``) and names the node so the
    last good checkpoint plus this message pinpoint where a run failed.
    """

    def __init__(self, node: str, message: str):
        self.node = node
        super().__init__(f"node {node!r} failed: {message}")


# ---------------------------------------------------------------------------
# Backends
# ---------------------------------------------------------------------------


class BackendError(AgentFlowError):
    """A recoverable backend/agent error.

    Note this is distinct from the :class:`~agentflow.events.BackendError`
    *event*, which is data a node may branch on. This exception is raised for
    backend misuse (e.g. prompting before ``start()``).
    """


class BackendTransportError(BackendError):
    """A fatal transport failure: the process died, the socket dropped, or
    the wire produced unparseable data. Not recoverable within the turn.

    ``status`` and ``headers`` are populated when the failure came from an HTTP
    response, so callers can branch on them (e.g. distinguish a 429 from a 500).
    """

    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        headers: dict[str, str] | None = None,
        retry_after: float | None = None,
    ) -> None:
        self.status = status
        self.headers = headers or {}
        self.retry_after = retry_after
        super().__init__(message)


class BackendRateLimitError(BackendTransportError):
    """A provider rate limit (HTTP 429) that outlived the retry policy.

    Carries ``retry_after`` (seconds) when the provider supplied it, so a
    caller can back off at the application level."""


# ---------------------------------------------------------------------------
# Checkpointing / HITL
# ---------------------------------------------------------------------------


class CheckpointError(AgentFlowError):
    """A checkpoint could not be written, read, or resumed."""


class RunTimeout(GraphError):
    """A whole-run timeout fired before the graph reached END.

    The last completed super-step's checkpoint is intact (each step is
    checkpointed before the next begins), so the run is resumable from there
    when a checkpointer is configured.
    """

    def __init__(self, thread: str, seconds: float, step: int):
        self.thread = thread
        self.seconds = seconds
        self.step = step
        super().__init__(
            f"run {thread!r} exceeded {seconds}s (reached step {step}); last checkpoint preserved"
        )


class InterruptError(AgentFlowError):
    """Internal control-flow signal that a node requested a human interrupt.

    The runtime catches this to suspend the run and persist an interrupted
    checkpoint; it is not meant to escape to user code. Callers observe the
    interrupt through the run's stream / return value, not this exception.
    """

    def __init__(self, node: str, payload: object):
        self.node = node
        self.payload = payload
        super().__init__(f"interrupt requested by node {node!r}")
