# Permissions

An agent backend can run tools — write files, run shell commands, hit the network. Deciding what it may do is a security choice, so AgentFlow makes it explicit: every agent backend requires a `PermissionPolicy`. There is no auto-approve default. This chapter covers the five policies and when to use each.

## Why it is required

If you construct an agent backend without a `permission`, it raises immediately:

```python
KiroBackend("vibe")   # TypeError: an agent backend requires an explicit permission policy
```

That is deliberate. Silently auto-approving an agent's tool use is exactly the kind of default that leads to an agent deleting a file it should not have touched. You state your intent up front.

## The five policies

### AllowAll

Approves every request. Auto-approve — only for a trusted local sandbox.

```python
from agentflow.backends.base import AllowAll
KiroBackend("vibe", permission=AllowAll())
```

Use this when the agent runs in a throwaway container or a directory you do not mind it changing. Never in production against anything you care about.

### DenyAll

Rejects every request. The agent can think and talk but cannot use tools.

```python
from agentflow.backends.base import DenyAll
KiroBackend("vibe", permission=DenyAll())
```

Useful for a read-only "explain this" mode, or as the `fallback` of an allowlist.

### ToolAllowlist

Approves only the named tools; everything else goes to a fallback (default `DenyAll`). This is the pragmatic least-privilege choice: name what the agent may do, refuse the rest.

```python
from agentflow.backends.base import ToolAllowlist, DenyAll
policy = ToolAllowlist({"read_file", "list_directory"}, fallback=DenyAll())
KiroBackend("vibe", permission=policy)
```

Pass `fallback=Interactive()` to ask a human for anything not pre-approved, instead of refusing outright.

### Interactive — durable human approval

Escalates each request to a human via an engine interrupt. It calls `ctx.interrupt(request)`, which suspends the whole run and persists an interrupted checkpoint. The human inspects the request and calls `app.resume(thread, value=...)`; the resumed value becomes the decision.

```python
from agentflow.backends.base import Interactive
KiroBackend("vibe", permission=Interactive())
```

Because it rides on the interrupt mechanism, it is durable and cross-process — the human can take minutes or a restart to answer. It is the right choice for one-shot backends and for approval flows where the decision-maker is elsewhere.

### Callback — inline, same-turn approval

Asks a human (or any resolver) inline, within the same open turn, without interrupting the run. You supply an async function `(PermissionRequest) -> decision`.

```python
import asyncio
from agentflow.backends.base import Callback

async def ask(req):
    answer = await asyncio.to_thread(input, f"allow {req.tool}? [y/N] ")
    return answer

KiroBackend("vibe", permission=Callback(ask))
```

This matters for a persistent-session backend like Kiro: the agent is holding one turn open waiting for the decision, so the answer must go back in that turn. Suspending the run with `Interactive` and abandoning that open turn would deadlock. For Kiro, `Callback` is usually what you want for live human approval.

## Interactive vs Callback

Both put a human in the loop; they differ in how.

| | `Interactive` | `Callback` |
| --- | --- | --- |
| Mechanism | Suspends the run (`ctx.interrupt`) | Asks inline in the open turn |
| Needs `resume()` | Yes | No |
| Durable / cross-process | Yes | No |
| Right for | One-shot backends, remote approvers | Persistent session (Kiro), live local approval |

## What a decision looks like

A policy returns a `PermissionDecision` — an `Allow` or a `Deny`.

```python
from agentflow.events import Allow, Deny

Allow()                       # approve; backend picks the most permissive option
Allow(option_id="allow_once") # approve a specific offered option
Deny(reason="not this time")  # reject
```

For human resolvers, AgentFlow normalizes common answers so you can return a plain string or `None`: `"y"`/`"yes"`/`"allow"`/`"ok"` become `Allow`, `"n"`/`"no"`/`"deny"` become `Deny`, `None`/`False` become `Deny`, and any other string is treated as a specific option id.

## How each backend applies the policy

Only Kiro enforces the policy at run time, per tool call. The one-shot CLI agents translate it once into their own launch controls:

| Backend | Application |
| --- | --- |
| `KiroBackend` | Runtime gate — the policy resolves each ACP permission request |
| `CodexBackend` | Upfront `--sandbox` flag derived from the policy |
| `ClaudeCodeBackend` | Upfront `--allowed-tools` / `--permission-mode` derived from the policy |

For Codex and Claude Code the mapping is best-effort and upfront, not a runtime interception — and an explicit flag you pass always wins over the derived value. If you need genuine per-tool, human-in-the-loop control, use Kiro.

Next: [retries and rate limits](retries.md).
