# Mental model

Once you've run a graph, the engine is worth understanding as a whole, because a few deliberate choices explain everything else. This chapter is the "why" behind the primitives.

## A run is a sequence of super-steps

AgentFlow does not walk one node at a time. It advances in super-steps (the bulk-synchronous parallel model). Each super-step:

1. runs the current frontier of nodes — all of them, concurrently, against the same immutable state;
2. folds their partial updates into the state through the channel reducers, in a deterministic order;
3. resolves the outgoing edges to compute the next frontier;
4. checkpoints.

Then the next super-step begins. A single node is just a frontier of one. Fan-out is a frontier of several.

## State is data folded by reducers, not mutated

A node never mutates state. It returns a partial update, and a reducer folds that update into the channel. This is what makes concurrent updates in one super-step well-defined: instead of two nodes racing to write the same field last, their updates are combined by a pure function you chose — sum, concatenate, merge, union, or last-wins. Determinism falls out of that.

## Nodes decide nothing about control flow

A node computes; it does not choose what runs next. Edges do that. A static edge always fires; a conditional edge runs a router over the state and picks a target. Keeping "work" and "routing" separate is the single most important habit in AgentFlow: it is what makes a run's control flow inspectable and a resume predictable. You'll see this rule again in quality gates and long-running loops.

## Super-steps are all-or-nothing

If any node in a super-step raises, the entire step's updates are discarded and no checkpoint is written for it. The last good checkpoint is the previous step. So a resume never sees half a step — either a super-step committed in full, or it didn't happen. This is why a crashed run resumes cleanly.

## Durability is a checkpoint after every step

Because each step is checkpointed before the next begins, the checkpoint is not an afterthought — it's the unit of durability. Resume, time-travel inspection, and human-in-the-loop interrupts are all just "load a checkpoint and continue from its frontier". Configure a checkpointer and you get all three; leave it out and the same graph runs purely in memory.

## Backends are outside the engine

The engine knows nothing about models or agents. A backend is a plain object you construct and call from inside a node. That boundary is why `import agentflow` needs no third-party packages, and why you can swap a local Ollama model for a hosted agent without touching graph code.

With that model in hand, the [core concepts](../concepts/state.md) chapters go deep on each piece, starting with state and reducers.
