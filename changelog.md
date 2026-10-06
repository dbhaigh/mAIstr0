# Changelog

## 0.0.2 - 2026-10-06

### Added

- Native Go Pi agent runtime as the default interactive orchestrator harness,
  with DeepSeek retained as an alternative.
- Runtime harness selector that applies to new conversations while preserving
  the harness for existing conversations.
- Tabbed workspace panels for batch submission, topology, nodes, workload,
  local network, and tasks.
- Dashboard views for the interactive orchestrator, cluster workload, and
  persistent memory and learning.
- Node properties dialog for configuring model engines, default and disabled
  models, network discovery, tray/browser behavior, and memory location.
- Controls to refresh one or all nodes' model registries and visibility for
  discovered LAN peers in the cluster topology.

### Changed

- Refresh node model registries automatically so newly available models are
  reflected without restarting the node.
- Expand the README with dashboard workflows and Windows/Linux build commands.
- Document Pi runtime scope, harness configuration, workspace tabs, and
  versioned release builds.

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
