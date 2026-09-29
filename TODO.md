# TODO

Отложенные задачи по `agentic-workflow-sdk`.

## Сделано

- Live-тест `OpenAIBackend` против настоящего OpenAI — закрыто.
- Live-тест `AnthropicBackend` против Messages API — закрыто.
- Телеметрия: `Hooks` + `RunMetrics` (метрики в памяти), `JsonlTelemetry`
  (durable JSONL, включая backend-события через `ctx.emit`/`on_event`),
  `MultiHooks` (композиция листенеров) — закрыто.

## Открыто

- Evaluation-слой: `Evaluator` протокол, чекеры (exact/JSON-схема/предикат/
  LLM-judge поверх `LLMBackend`), batch-прогон `evaluate(graph, dataset,
  evaluators)` с отчётом. Обсудить дизайн.
- Опционально: OpenTelemetry-экспортер (спаны) как ещё одна реализация
  `Hooks`, поверх той же событийной модели.
