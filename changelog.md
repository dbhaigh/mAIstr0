# Changelog

## 0.0.1 - 2026-10-05

### Added

- Streamed model output from Ollama and vLLM/FastFlowLM through node and
  orchestrator endpoints, including OpenAI-compatible chat completions.
- Temperature and maximum-token forwarding for engines that support those
  generation options.
- Task cancellation through `POST /api/tasks/{id}/cancel`, with dashboard
  controls and a limit of 16 concurrent orchestrator dispatches.
- Combined task results assembled in subtask order.

### Changed

- Reject task submissions when any subtask cannot be assigned to a compatible
  model, rather than silently accepting partial work.
- Return isolated task snapshots to avoid exposing concurrently-mutated task
  records.
- Improve sentence decomposition and generated subtask identifiers.
- Preserve system, user, assistant, and tool roles when handling
  OpenAI-compatible chat requests.
- Use the version declared in `internal/version/version.go` in release builds;
  report semantic version strings consistently and compare legacy numeric
  versions during migration.
- Expand the README with setup, engine, API, and current task-planning details.

### Validation

- `go test ./...`
- `go vet ./...`
- `node --check web/static/app.js`
