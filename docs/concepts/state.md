# State and reducers

State is the shared, typed data a graph operates on. It is a set of named channels, and each channel has a reducer — a pure function that folds a node's partial update into the current value. Reducers are the quiet centerpiece of AgentFlow: they are what make concurrent updates in one super-step merge deterministically instead of racing on last-writer-wins.

## Declaring a schema

A state schema is a `TypedDict` subclass of `State`. Each field is a channel; its annotation carries the reducer.

```python
from typing import Annotated
from agentflow import State, add, append, last

class ChatState(State):
    messages: Annotated[list, append]   # concatenate updates
    tokens: Annotated[int, add]         # sum updates
    best: Annotated[float, last]        # overwrite (the default)
    title: str                          # bare field -> last-wins
```

A field annotated `Annotated[type, reducer]` uses that reducer. A bare field uses `last` (last-value-wins). Because `State` is `total=False`, every channel is optional at the type level — nodes emit partial updates, and a channel simply has no value until first written.

## The built-in reducers

| Reducer | Folds `(current, update)` into | `None` current is |
| --- | --- | --- |
| `last` | the update | — (the default) |
| `append` | `current + update` (a list) | empty list |
| `add` | `current + update` | starts from the update |
| `merge` | `{**current, **update}` | empty dict |
| `union` | `current | update` (a set) | empty set |

`append` extends when the update is a list or tuple, and appends a single element otherwise. Any pure `(current, update) -> new` is a valid reducer — the built-ins are just the common ones. A reducer must be pure and must treat a `None` current as its empty value.

## Nodes return deltas, not absolutes

A node returns a partial update — a dict of channel names to values — and the engine folds each value through that channel's reducer:

```python
async def turn(state, ctx):
    return {"messages": [reply], "tokens": 42}
```

Under the reducers above, `{"messages": [reply]}` appends to the transcript and `{"tokens": 42}` adds 42 to the running count. The node never writes an absolute value or mutates `state`; it describes a change and lets the reducer apply it.

## Why reducers, not assignment

When more than one node runs in the same super-step, several updates land on the state at once. If channels were plain variables, the result would depend on who wrote last. With reducers, the combination is a function you chose: two nodes each appending to `messages` produce both messages in a deterministic order; two nodes each adding to `tokens` produce the sum, regardless of order. Commutative reducers (`add`, `union`) don't depend on order at all; non-commutative ones (`append`, `last`) are applied in the runtime's deterministic node order.

## Rules the engine enforces

- Nodes may only write declared channels. Writing an undeclared channel raises immediately rather than sitting silently in state — a typo fails fast.
- Channel names starting with `__`, and the reserved names `__step` and `__next`, belong to the engine. A schema or update that uses them is rejected.
- Input validation happens up front: `invoke({...})` rejects input keys that are not declared channels before the run starts.

State is pure data — no async, no I/O, no engine imports — so a schema can be understood and tested on its own.

Next: [nodes and edges](nodes-and-edges.md), which turn a state schema into a running graph.
