# 4. Dual-state with by_key

The search state splits in two, the same way as the mechanics tutorial: fixed facts about the problem apart from the moving state of the search. The split encodes who may write what — and sets up chapter 5, where an agent writes the search but never the problem.

Runnable file: `examples/tutorial/queryopt/ch04_dual_state.py`.

## Problem vs search

```python
class OptState(State):
    problem: Annotated[dict, merge]          # fixed: seed, penalty, query
    search: Annotated[dict, by_key({
        "candidates": append,                # every scored candidate accumulates
        "incumbent": last,                   # the best index set so far, whole
    })]
```

`problem` holds the deterministic facts and barely changes. `search` is everything discovered — the candidates tried and the incumbent (best indexes + value). The two fields of `search` fold differently, which is why it is a [`by_key`](../../concepts/state.md#structured-channels-with-by_key) channel: candidates accumulate, the incumbent is replaced whole.

## Promotion replaces the incumbent whole

```python
updates["search"]["incumbent"] = {"indexes": best_cand.indexes, "value": best_score.value}
```

A new best index set is a new object, written whole with `last` — never a partial patch. Recording each round's candidate summaries into `search.candidates` (via `append`) builds a durable history of what was tried, without disturbing the incumbent.

```text
dual-state: fixed problem vs moving search
  round 1: promote value -15.0
  round 2: promote value -8.0
best value -8.0 with 2 indexes
candidates evaluated across the run: 15
```

## Why the split matters here

It is not just tidiness. In the next chapter the proposer becomes an agent that reads `problem` (the schema and query) and whose suggestions flow into `search` (candidates). The boundary makes it unambiguous: the agent informs the search, it does not get to rewrite the problem definition or the measure. That separation is what keeps an agent-driven optimizer trustworthy.

## What you learned

- `by_key` separates the fixed `problem` from the moving `search`, each field folding its own way.
- The incumbent is replaced whole with `last`; candidates accumulate with `append`.
- The split encodes a trust boundary the agent proposer will respect in chapter 5.

In [chapter 5](05-agent.md) the proposer becomes a real Codex agent — while the SQLite measure stays deterministic.
