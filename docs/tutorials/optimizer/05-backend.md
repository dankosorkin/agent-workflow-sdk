# 5. A real planner backend

So far the proposer enumerates every strategy. That works for four strategies; it does not scale to a real search space. In this chapter a model proposes which few moves to try next — but the verifier stays deterministic code. The optimizer never trusts the model's opinion of a plan; it only trusts `cost_of`.

Runnable file: `examples/tutorial/05_backend.py`.

## The boundary: propose vs verify

This is the single most important idea in the tutorial. Split the loop into two roles:

- The proposer suggests moves. It can be a model, a heuristic, anything. It is allowed to be wrong, creative, or non-deterministic.
- The verifier scores a plan. It is pure, deterministic code. It is the ground truth.

A model proposing `add_index` is a hint. Whether `add_index` actually helps is decided by `cost_of`, not by the model. This is what keeps an LLM-driven optimizer honest: the intelligence explores, the code judges. Every durable, trustworthy agent system draws this line somewhere.

## The proposer is just a small protocol

The graph does not depend on a model. It depends on a tiny interface:

```python
class Proposer(Protocol):
    async def propose(self, problem: dict, incumbent: dict) -> list[str]: ...
```

For a reproducible chapter we use a deterministic stand-in — a heuristic that ranks untried strategies by the problem's factor hints and returns the best two:

```python
class HeuristicProposer:
    async def propose(self, problem, incumbent):
        plan = incumbent["plan"]
        hints = problem["factor_hints"]
        untried = [s for s in hints if s not in plan]
        return sorted(untried, key=lambda s: hints[s])[:2]
```

The `propose` node calls it and turns the suggestions into candidates — but it scores each with `cost_of`, not with anything the proposer said:

```python
async def propose(state, ctx) -> dict:
    incumbent = state["search"]["incumbent"]
    suggestions = await proposer.propose(state["problem"], incumbent)
    plan = incumbent["plan"]
    cands = [{"plan": plan | {s}, "cost": cost_of(plan | {s})}
             for s in suggestions if s not in plan]
    return {"search": {"candidates": cands}}
```

## Swapping in a real model is one object

Because the graph depends only on the `Proposer` protocol, using a real model changes nothing about the graph — you pass a different object. The example ships the Ollama version commented out:

```python
from agentflow.backends.ollama import OllamaBackend
from agentflow.events import Message, TurnEnd

class OllamaProposer:
    def __init__(self, model="llama3.2"):
        self.llm = OllamaBackend(model)
    async def propose(self, problem, incumbent):
        prompt = (f"Strategies: {list(problem['factor_hints'])}. "
                  f"Already applied: {sorted(incumbent['plan'])}. "
                  "Name up to two more to try, comma-separated, no prose.")
        text = ""
        async with self.llm:
            async for ev in self.llm.chat([Message('user', prompt)]):
                if isinstance(ev, TurnEnd):
                    text = ev.text
        names = [n.strip() for n in text.split(',')]
        return [n for n in names if n in problem['factor_hints']][:2]
```

`main(OllamaProposer())` and the optimizer now reasons with a local model. The verifier, the state, the routing, the promotion — all unchanged. This is the backend boundary the framework is built around: the engine knows nothing about models; a backend is a plain object you call from inside a node.

Notice too that the model is given `factor_hints`, not the real cost model. The proposer works from an imperfect view and may suggest a dud; the verifier catches it. That asymmetry — hints to propose, truth to verify — is the point.

## Run it

```text
optimizing with a HeuristicProposer:
  promote {'add_index'} cost 40.0
  promote {'add_index', 'push_filter'} cost 24.0
  promote {'add_index', 'push_filter', 'reorder_stages'} cost 21.6
final plan {'add_index', 'push_filter', 'reorder_stages'} -> cost 21.6
```

## What you learned

- Split propose (may be a model, may be wrong) from verify (deterministic truth). The model proposes; code decides.
- The graph depends on a small protocol, not a backend; swapping a heuristic for a real model is a one-object change.
- A backend is a plain object called inside a node — the engine never imports it.
- Give the proposer an imperfect view and let the verifier catch bad suggestions.

The `verify` node currently promotes silently. In [chapter 6](06-gate.md) we make that decision a real quality gate — accept, revise, or escalate — with an attempt budget, so the promotion boundary becomes an explicit, observable checkpoint.
