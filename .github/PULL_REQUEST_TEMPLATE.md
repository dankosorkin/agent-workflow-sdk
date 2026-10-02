## What and why

What this changes and the problem it solves. Link any related issue.

## How

Key implementation points a reviewer should know. Note any new public API
or any change to a documented contract in `DESIGN.md`.

## Checklist

- [ ] `ruff check agentflow tests examples` passes
- [ ] `ruff format --check agentflow tests examples` passes
- [ ] `mypy agentflow` passes
- [ ] `pytest -q` (offline suite) passes; added/updated tests for the change
- [ ] Docs updated if behavior or public API changed (README / `docs/` / docstrings)
- [ ] `CHANGELOG.md` updated under `[Unreleased]` for a user-facing change
