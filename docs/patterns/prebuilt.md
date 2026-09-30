# Prebuilt patterns

The core primitives — nodes, edges, reducers, a compiled graph — are enough to build anything. But a few shapes come up so often that AgentFlow ships them ready-made, in `agentflow.prebuilt`. None are special engine features: each is built entirely from the primitives, so they double as worked examples you can read and adapt.

There are two graph builders (a tool-calling loop and an optimize loop), two node wrappers for resilience, a quality-gate helper, and an idempotency helper.

## tool_loop — the function-calling agent

The classic loop: call the model with the conversation and the available tools; if it asks to call tools, run them, append the results, call the model again; stop when it answers without a tool. Because an [LLM backend](../backends/llm-backends.md) never runs tools itself, the graph owns execution.

```python
from agentflow import Message
from agentflow.backends.ollama import OllamaBackend
from agentflow.prebuilt import Tool, tool_loop

async def get_weather(city: str) -> str:
    return f"{city}: 18C, clear"

tools = [Tool(
    "get_weather", get_weather,
    description="Get the weather for a city",
    schema={"type": "object",
            "properties": {"city": {"type": "string"}},
            "required": ["city"]},
)]

llm = OllamaBackend("llama3.2")
app = tool_loop(llm, tools, max_turns=10)

async with llm:
    out = await app.invoke({"messages": [Message("user", "weather in Paris?")]})
print(out["messages"][-1].content)
```

Under the hood it's a two-node graph — an `agent` node that calls the model and a `tools` node that runs the requested tools — with a conditional edge looping between them.

Always check the terminal `status` before trusting the last message:

| `status` | Meaning |
| --- | --- |
| `"completed"` | The model answered with no tool request — the last message is the final answer |
| `"tool_calls_unresolved"` | `max_turns` was hit with tool calls still pending — the plan is incomplete |

Set `stream_text=True` to forward the model's `TextChunk`s via `ctx.emit`; pass `checkpointer=` to make the loop durable.

## iterate_until_converged — the optimize loop

"Keep improving until it's good enough." You supply one async `work(state)` that performs a turn and returns a `Candidate` (a score plus a payload, and an optional `done` flag); the loop owns best-so-far, the no-improvement streak, and the stop decision.

```python
from agentflow.prebuilt import Candidate, iterate_until_converged

async def work(state):
    draft = await improve(state.get("best_payload"))
    return Candidate(score=await grade(draft), payload=draft)

app = iterate_until_converged(work, perfect_score=100.0, patience=4, max_iterations=20)
out = await app.invoke({})
print(out["stop_reason"], out["best_score"])
```

Stop reasons, in priority order: `done` (worker set `done=True`), `optimal` (reached `perfect_score`), `converged` (no improvement for `patience` rounds), `exhausted` (`max_iterations`). The final state carries `best_score`, `best_payload`, `stop_reason`, `iterations`, and a `history` of scores.

## Quality gates

`add_quality_gate` adds an explicit accept / revise / escalate checkpoint to a graph — the right tool when a gate is one station in a bigger workflow rather than a self-contained loop. It has its own chapter: [quality gates](../durability/quality-gates.md). The short version:

```python
from agentflow.prebuilt import GateResult, GateState, add_quality_gate

async def evaluate(state, ctx) -> GateResult:
    score = await grade(state["artifact"])
    return GateResult(passed=score >= 0.85, score=score)

add_quality_gate(g, "review", evaluate,
                 on_pass="publish", on_fail="revise", on_escalate="ask_human",
                 max_attempts=3)
```

`GateResult` vs `Candidate`: the loop's `Candidate.done` means "the loop may stop"; a gate's `GateResult.passed` means "this meets the bar". Use `iterate_until_converged` for a score to climb, `add_quality_gate` for a three-way checkpoint that can hand off to a human.

## Resilience wrappers

`with_retry` and `with_timeout` wrap a node and return a node, so they compose and drop into `add_node`:

```python
from agentflow.prebuilt import with_retry, with_timeout

flaky = with_retry(with_timeout(call_api, seconds=10), retries=3)
g.add_node("call_api", flaky)
```

- `with_timeout(node, seconds)` fails the node if it runs longer than `seconds`.
- `with_retry(node, retries=2, backoff=0.1, backoff_factor=2.0, max_backoff=30.0, on=(Exception,), jitter=0.0)` retries on failure with capped exponential backoff.

Both never catch an `InterruptError` — a human-in-the-loop pause always propagates cleanly. This differs from [`RetryPolicy`](../backends/retries.md), which guards a single backend HTTP request; `with_retry` guards a whole node.

## Idempotency

`skip_if_done` wraps a node so identical work runs once and is served from a [Store](../durability/store.md) thereafter — see the [long-running loops](long-running-loops.md) chapter.

```python
from agentflow.prebuilt import artifact_key, skip_if_done

node = skip_if_done(expensive, store,
                    namespace=("cache", "expensive"),
                    key_from=lambda s: artifact_key(s["input"]))
```

Next: [long-running loops](long-running-loops.md) for how these compose, or the [tutorial](tutorial-qa-agent.md) for an end-to-end agent.
