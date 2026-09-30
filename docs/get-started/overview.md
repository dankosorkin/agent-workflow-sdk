# Overview

AgentFlow builds agent workflows the way you'd build any program: out of small pieces that compose. The pieces are a typed state, nodes that transform it, and edges that decide what runs next. The engine runs that graph durably and asynchronously against a backend you choose.

## The three ideas

- A graph is a set of nodes over a shared, typed state. Each node is an async function that reads the state and returns a partial update. Edges — static or conditional — decide which nodes run next, which is how you get branches and loops.
- A backend is how a node talks to a model or an agent. Two families share one event vocabulary: agent backends that run their own tools, and LLM backends that only produce text and tool-call requests. You swap one for another without touching graph code.
- Durability is built in. Configure a checkpointer and every super-step is persisted, so a run resumes after a crash, pauses for a human, and can be inspected step by step.

## What it is not

AgentFlow is a library, not a framework that owns your process, and not a service. It has no fixed agent loop you must fit into — you assemble the loop you need. It ships no HTTP server, no auth, no multi-tenancy; those belong to a service layer you build on top of the control plane.

## Where to go next

- [Installation](installation.md) — install the core and the optional extras.
- [Quickstart](quickstart.md) — write and run a real graph from scratch.
- [Mental model](mental-model.md) — why the engine works the way it does.

Then dive into [Core concepts](../concepts/state.md), which is the heart of the guide.
