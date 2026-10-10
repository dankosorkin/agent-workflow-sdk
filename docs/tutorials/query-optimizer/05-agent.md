# 5. A real agent proposer

This is the chapter the whole applied tutorial builds toward: the proposer stops enumerating and starts reasoning. A local Codex agent reads the schema and query and proposes indexes. The measure stays deterministic SQLite — so swapping the proposer never changes the score.

Runnable file: `examples/tutorial/queryopt/ch05_agent.py`.

- Heuristic (default, offline): `python -m examples.tutorial.queryopt.ch05_agent`
- Local Codex agent: `AGENTFLOW_TUTORIAL_BACKEND=codex python -m examples.tutorial.queryopt.ch05_agent`

## Propose with an agent, verify with code

The single most important idea in this tutorial: the proposer and the verifier are different roles with different trust.

- The proposer suggests indexes. It can be a heuristic, a local Codex agent, or a hosted LLM. It is allowed to be creative, wrong, or non-deterministic.
- The verifier is the SQLite scorer. It is deterministic. It is the ground truth.

An agent proposing `CREATE INDEX ... ON orders(order_date)` is a hint. Whether that index actually lowers the plan cost is decided by `EXPLAIN`, not by the agent. This is what keeps an agent-driven optimizer honest.

## The graph does not change — only the proposer object

`get_proposer()` reads an env flag and returns one of two objects behind the same protocol:

```python
proposer = get_proposer()   # HeuristicProposer, or CodexProposer when the flag is set
```

Nothing else in the graph changes. The `CodexProposer` builds a prompt from the schema and query, runs a local Codex agent, and maps its suggestions onto a vetted set of index moves so a hallucinated column can never reach the scorer:

```python
backend = CodexBackend(permission=AllowAll(), model=self.model)
text = ""
async with backend:
    async for ev in backend.prompt(prompt):
        if isinstance(ev, TurnEnd):
            text = ev.text
# keep only suggestions that match a known-safe move; else fall back to heuristic
```

The agent is an [agent backend](../../backends/agent-backends.md), so it requires an explicit permission policy — `AllowAll()` for this trusted local sandbox. That same mechanism becomes the human-approval gate in chapter 7.

## The honesty guarantee

Run with the heuristic and with Codex and the scores are directly comparable, because the scorer depends only on the candidate and the seeded database — not on who proposed it:

```text
optimizing with HeuristicProposer (the scorer is always real SQLite):
  round 1: proposer HeuristicProposer -> value -15.0 (plan_cost 13.0)
  round 2: proposer HeuristicProposer -> value -8.0 (plan_cost 4.0)
best value -8.0 with 2 indexes
```

This is exactly the "agents locally, LLM in production" story. You develop against a local Codex agent, ship against a hosted LLM, and the deterministic SQLite measure tells you honestly whether the swap helped — a change in the result is a real change, not a change in how the proposer happened to phrase things. Without a backend-independent verifier, you could never tell those apart.

## What you learned

- Split propose (an agent, maybe wrong) from verify (deterministic SQLite truth); the agent explores, the database judges.
- `get_proposer()` swaps a heuristic for a Codex agent with no change to the graph — a backend is just an object called inside a node.
- A deterministic, backend-independent measure is what makes swapping the proposer safe: the score stays comparable.
- Validate an agent's free-text suggestions against a vetted move set before they reach the measure.

In [chapter 6](06-gate.md) the promotion decision becomes an explicit quality gate with an attempt budget.
