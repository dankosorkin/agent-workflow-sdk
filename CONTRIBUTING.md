# Contributing

Thanks for your interest in `agent-workflow-sdk`.

## Development setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e '.[ollama,dev]'
```

## Before you open a PR

Run the same checks CI runs. All must pass:

```bash
ruff check agentflow tests examples
ruff format --check agentflow tests examples
mypy agentflow
pytest -q                 # offline suite (hermetic)
```

- Format with `ruff format` before committing.
- Add tests for new behavior. The suite deliberately tests invariants
  (concurrency, atomicity, timeouts, interrupts), not just happy paths.
- Keep the core (`agentflow/` minus `backends/`) dependency-free. New provider
  libraries go behind an optional extra.
- Update `CHANGELOG.md` under `[Unreleased]`.

## Live backend tests

Backend smoke tests are opt-in and skipped by default. To run them you need the
relevant CLIs/servers and keys (see `.env.example`):

```bash
pytest -m live
```

Each live test self-skips when its backend is absent.

## Architecture

Read `DESIGN.md` first — it records the invariants and contracts your change
must preserve. In short: the dependency direction is strict, state → engine →
backends, and the engine never imports a backend. Please preserve that
boundary.

## Commit and PR conventions

- One logical change per PR; keep diffs reviewable.
- Reference the affected area in the commit subject (e.g. `runtime:`,
  `backends/openai:`).
- Do not commit secrets. `.env` is gitignored; use `.env.example` as the
  template.

## Release process

1. Move `[Unreleased]` entries in `CHANGELOG.md` under a new version heading;
   bump `version` in `pyproject.toml` (SemVer).
2. Ensure CI is green (lint, types, tests, build).
3. Build and verify the artifacts: `python -m build` — confirm the wheel ships
   `agentflow/py.typed` and the LICENSE.
4. Tag `vX.Y.Z` and publish.

Pre-1.0 the public API may change between minor versions; breaking changes are
called out in the changelog.

