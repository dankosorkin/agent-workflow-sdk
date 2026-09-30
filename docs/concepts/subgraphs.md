# Subgraphs

A compiled graph can be a node inside another graph. That's a subgraph: it lets you build a complex workflow out of smaller, testable graphs, each with its own state schema and its own checkpoint history.

## Two ways to embed

The simplest way is to pass a `CompiledGraph` straight to `add_node`. Channels shared by name between parent and child pass through automatically.

```python
inner = build_inner().compile()      # its own Graph + compile
g.add_node("inner", inner)           # embed as a node
```

For explicit control over what flows in and out, use `add_subgraph` with channel maps:

```python
g.add_subgraph(
    "inner",
    inner,
    input_map={"parent_doc": "doc"},      # parent channel -> subgraph channel
    output_map={"result": "inner_result"} # subgraph channel -> parent channel
)
```

`input_map` says which parent channels feed which subgraph channels; `output_map` says which subgraph channels merge back into which parent channels. When you omit them, channels shared by name pass through in both directions.

## What happens at run time

When the subgraph node runs, the engine:

1. takes the parent state, restricted to the channels the subgraph declares (or mapped via `input_map`);
2. runs the subgraph to completion on its own isolated checkpoint sub-thread;
3. merges the subgraph's final state back as this node's update — only the keys that are parent channels (or mapped via `output_map`).

So a subgraph is, from the parent's point of view, just a node that returns an update. From its own point of view, it's a full graph with its own super-steps and its own history.

## Why the maps matter

Explicit maps are how you avoid double-counting accumulator channels. If both parent and child have a `messages` channel with an `append` reducer, passing the child's `messages` straight back would append the child's list onto a parent list that may already contain it. Routing the child's result into a distinct parent channel with `output_map` keeps the accumulation clean.

## When to reach for a subgraph

- A reusable sub-workflow you want to test and version on its own.
- A bounded unit you want checkpointed as its own sub-thread.
- A place where the child's state shape genuinely differs from the parent's, and mapping the boundary is clearer than sharing one large schema.

For a loop you simply want to repeat, a conditional edge back to the same node is lighter than a subgraph. Reach for a subgraph when the inner piece is a coherent graph in its own right.

That closes the core concepts. Next: [backends](../backends/overview.md), where a node meets a model or an agent.
