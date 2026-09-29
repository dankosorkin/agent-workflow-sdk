# TODO

Отложенные задачи по `agentic-workflow-sdk`.

## Сделано

- Live-тест `OpenAIBackend` против настоящего OpenAI — закрыто (прогнан с
  реальным ключом, модель `gpt-4.1-nano`).
- Live-тест `AnthropicBackend` против Messages API — закрыто (org-level ключ +
  `ANTHROPIC_WORKSPACE_ID`, модель `claude-haiku-4-5-20251001`).

## Открыто

- Демо `CompiledGraph.stream()` с проброшенными наружу backend-событиями
  (токены, tool-calls) — прогнать вживую и оформить как пример.
