# Security Policy

## Reporting a vulnerability

Please report suspected vulnerabilities privately rather than opening a public
issue. Email the maintainers (see repository contact) with:

- a description of the issue and its impact,
- steps to reproduce or a proof of concept,
- affected version(s).

You can expect an acknowledgement within a few business days and a coordinated
disclosure once a fix is available.

## Supported versions

Pre-1.0, only the latest released version receives security fixes.

## Security-relevant design notes

Operators should be aware of these behaviors when running real agents and
persisting real data:

- Authorization is explicit. Agent backends require a `PermissionPolicy` —
  there is no auto-approve default. `AllowAll` should be used only in a trusted
  local sandbox. See the capability matrix in the README for which backends
  route tool requests through the policy (Kiro) versus rely on their own CLI
  sandbox (Codex, Claude Code).
- Persistence is plaintext by default. Checkpoints and JSONL telemetry contain
  prompts, model output, and tool arguments. Use the `redact=` option
  (`RedactKeys`) to mask sensitive keys, and note that files/directories are
  created owner-only (0o600/0o700) where the filesystem supports it.
- Secrets in environment: `.env` is gitignored. The Claude Code backend strips
  inherited `ANTHROPIC_*` variables from its subprocess by default so a key
  meant for another backend is not leaked to the CLI.
- Untrusted content: treat model/tool output and fetched content as untrusted.
  The SDK does not execute model output; your nodes decide what to run.
