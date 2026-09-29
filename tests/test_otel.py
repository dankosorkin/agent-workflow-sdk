"""OtelHooks: run/node spans emitted via an in-memory OTel exporter."""

from __future__ import annotations

from typing import Annotated

import pytest
from agentflow import END, START, Graph, State, add

# Skip the whole module if the otel extra isn't installed.
otel = pytest.importorskip("opentelemetry")

from agentflow.otel import OtelHooks  # noqa: E402
from opentelemetry.sdk.trace import TracerProvider  # noqa: E402
from opentelemetry.sdk.trace.export import SimpleSpanProcessor  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (  # noqa: E402
    InMemorySpanExporter,
)


class S(State):
    n: Annotated[int, add]


@pytest.fixture
def spans():
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("test")
    yield tracer, exporter


async def test_run_and_node_spans_emitted(spans):
    tracer, exporter = spans

    g = Graph(S)
    g.add_node("a", lambda s, c: {"n": 1})
    g.add_node("b", lambda s, c: {"n": 2})
    g.add_edge(START, "a")
    g.add_edge("a", "b")
    g.add_edge("b", END)
    await g.compile(hooks=OtelHooks(tracer=tracer)).invoke({"n": 0})

    finished = exporter.get_finished_spans()
    names = sorted(s.name for s in finished)
    assert "agentflow.run" in names
    assert "agentflow.node.a" in names
    assert "agentflow.node.b" in names

    run_span = next(s for s in finished if s.name == "agentflow.run")
    assert run_span.attributes["agentflow.completed"] is True
    assert run_span.attributes["agentflow.schema_version"] == "1"


async def test_node_error_recorded_on_span(spans):
    tracer, exporter = spans

    g = Graph(S)

    async def boom(state, ctx):
        raise ValueError("kaboom")

    g.add_node("boom", boom)
    g.add_edge(START, "boom")
    g.add_edge("boom", END)

    from agentflow import NodeError

    with pytest.raises(NodeError):
        await g.compile(hooks=OtelHooks(tracer=tracer)).invoke({"n": 0})

    finished = exporter.get_finished_spans()
    node_span = next(s for s in finished if s.name == "agentflow.node.boom")
    # error status + recorded exception event
    assert node_span.status.status_code.name == "ERROR"
    assert any(e.name == "exception" for e in node_span.events)
    # run span marked not completed
    run_span = next(s for s in finished if s.name == "agentflow.run")
    assert run_span.attributes["agentflow.completed"] is False


async def test_backend_event_added_to_span(spans):
    tracer, exporter = spans
    from agentflow.events import TextChunk

    g = Graph(S)

    async def talk(state, ctx):
        ctx.emit(TextChunk(text="hi"))
        return {"n": 1}

    g.add_node("talk", talk)
    g.add_edge(START, "talk")
    g.add_edge("talk", END)
    await g.compile(hooks=OtelHooks(tracer=tracer)).invoke({"n": 0})

    import asyncio

    await asyncio.sleep(0.05)  # let fire-and-forget on_event flush

    finished = exporter.get_finished_spans()
    # the backend event lands on the node span or the run span
    all_events = [e.name for s in finished for e in s.events]
    assert "agentflow.backend_event" in all_events
