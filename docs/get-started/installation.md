# Installation

AgentFlow needs Python 3.11 or newer. The core is dependency-free — installing it pulls in no third-party packages — and each backend or durable store lives behind an optional extra you add only when you need it.

## Install the core

```bash
pip install agent-workflow-sdk
```

The distribution name is `agent-workflow-sdk`; the import root is `agentflow`:

```python
import agentflow
from agentflow import Graph, START, END, State
```

Importing `agentflow` never drags in `httpx`, `redis`, `asyncpg`, `prometheus_client`, or `opentelemetry` — that's the dependency rule the design guarantees. Those arrive only through the extras below.

## Optional extras

Install extras with the bracket syntax, e.g. `pip install "agent-workflow-sdk[ollama,postgres]"`.

| Extra | Enables | Pulls in |
| --- | --- | --- |
| `ollama` | the HTTP LLM backends (Ollama, OpenAI, Anthropic) | `httpx` |
| `postgres` | `PostgresCheckpointer`, `PostgresStore`, `PostgresRunQueue` | `asyncpg` |
| `redis` | `RedisCheckpointer` | `redis` |
| `otel` | `OtelHooks` telemetry | `opentelemetry-*` |
| `prometheus` | `PrometheusHooks` metrics | `prometheus-client` |

The HTTP LLM backends share one client library, so any of them needs the `ollama` extra (it is named for the default local target, but it provides `httpx` for all three).

## Verify

```python
import agentflow
print(agentflow.__version__)
```

Next: the [quickstart](quickstart.md) builds a running graph with no backend at all — just the engine.
