# Agent backends

An agent backend drives a coding agent that runs its own tools — reading files, editing code, running commands — and asks for permission before it acts. Unlike an LLM backend, it holds a session and does the work itself; your graph observes the stream and answers permission requests. Three ship in the box: `KiroBackend`, `CodexBackend`, and `ClaudeCodeBackend`.

## The lifecycle

Every agent backend has the same shape: `start()`, one or more `prompt(text)` turns, `close()`. Use it as an async context manager so `close()` is guaranteed.

```python
from agentflow.backends.kiro import KiroBackend
from agentflow.backends.base import AllowAll
from agentflow.events import TextChunk, ToolCall, TurnEnd

async with KiroBackend("vibe", permission=AllowAll()) as agent:
    async for ev in agent.prompt("Add a test for the parser."):
        if isinstance(ev, TextChunk):
            print(ev.text, end="")
        elif isinstance(ev, ToolCall):
            print(f"\n[tool: {ev.name}]")
        elif isinstance(ev, TurnEnd):
            ...
```

`prompt(text)` wraps `invoke(TextRequest(text))`. A turn streams `TextChunk`s and `ToolCall`/`ToolResult` events (the agent runs the tools), and ends in exactly one `TurnEnd`. When the agent wants to use a tool that needs authorization, it emits a `PermissionRequest` that your [permission policy](permissions.md) answers.

## permission is required

Every agent backend takes a `permission` policy as a required argument — there is no auto-approve default, because letting an agent run tools unattended is a decision the caller must make on purpose.

```python
from agentflow.backends.base import AllowAll, DenyAll, ToolAllowlist, Interactive

KiroBackend("vibe", permission=AllowAll())               # trusted sandbox only
KiroBackend("vibe", permission=ToolAllowlist({"read_file"}))
```

See [permissions](permissions.md) for the full policy set and how each backend enforces it.

## The three backends

### KiroBackend

Holds a long-lived JSON-RPC (ACP) session with `kiro-cli`. It is the only backend that routes each tool request through your permission policy at runtime, so it's the one to use for fine-grained, live approval.

```python
KiroBackend(
    agent,                    # the agent/mode name, e.g. "vibe"
    model=None,
    engine="v3",
    cwd=".",
    permission=<required>,
    close_grace=0.5,
)
```

On the `v3` engine the agent is selected as a session mode and auth comes from the CLI credential store. Requires `kiro-cli` on PATH.

### CodexBackend

Runs `codex exec --json` once per turn, carrying a `thread_id` between turns to resume the session. Codex has no per-tool runtime gate, so your policy is translated into a sandbox up front.

```python
CodexBackend(
    model=None,
    sandbox=None,             # explicit wins; else derived from the policy
    skip_git_repo_check=True,
    cwd=".",
    permission=<required>,
    timeout=600.0,
    extra_args=None,
)
```

An explicit `sandbox` always wins. Left unset, `AllowAll` maps to `workspace-write` and a deny-all or allowlist policy maps to `read-only` (the safe default). Requires `codex` on PATH.

### ClaudeCodeBackend

Runs `claude -p` once per turn in stream-json mode, resuming via `--resume <session_id>`. Like Codex, permissions are enforced through launch flags, not a runtime gate.

```python
ClaudeCodeBackend(
    model=None,
    agent=None,
    allowed_tools=None,
    disallowed_tools=None,
    permission_mode=None,
    skip_permissions=False,
    cwd=".",
    permission=<required>,
    timeout=600.0,
    extra_args=None,
    use_api_key_env=False,
)
```

Explicit tool flags win; otherwise a `ToolAllowlist` maps to `--allowed-tools` and a deny-all policy maps to the read-only `plan` permission mode. By default it scrubs `ANTHROPIC_*` from the child environment (the CLI uses its own login); set `use_api_key_env=True` to keep them. Requires `claude` on PATH.

## One-shot vs persistent, and what it means for permissions

The split matters. Kiro's persistent session lets it surface each tool request mid-run, so an `Interactive` policy can pause the whole run for a human. The one-shot CLI agents (Codex, Claude Code) can't be gated mid-run — they enforce permissions through the flags above, and the CLI's own controls are authoritative. The `permission` argument is still required for all three; for the CLI agents it's a best-effort translation. The README's capability matrix spells out which backend does what.

Next: [LLM backends](llm-backends.md), or jump to [permissions](permissions.md) and [retries](retries.md).
