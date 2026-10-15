# Changelog

## 0.0.6 - 2026-10-14

### Fixed

- Preserve LF line endings for contract fixtures on all platforms, preventing
  Windows checkout conversion from causing false golden-file test failures.
- Update CI checkout, Go setup, and Node setup actions to Node.js 24-based
  releases, removing deprecated Node.js 20 action-runtime warnings.
- Retry automatic task planning with another advertised node/model when the
  selected planning model fails, and return an explicit error if no candidate
  succeeds.
- Allow task-history and memory bootstraps to transfer histories larger than
  64 MiB using protocol-v2 cursor pages and bounded POST batches, without
  advancing peer watermarks before a transfer completes.
- Reject a single sync record that exceeds the per-request limit explicitly.

### Added

- Add a separate transparent inference router for OpenAI, Ollama, and
  Anthropic-compatible paths, with model-aware engine selection and provider
  request/stream forwarding that does not invoke DAG planning.
- Retry task dispatch across up to five distinct node/model pairs with
  transient-failure classification, bounded deadlines/backoff, and durable
  per-attempt status in task history.
- Add stable request/attempt IDs and prompt-free router events reporting node,
  model, engine, reservation time, time to first byte, status, and failure
  class through the API and SSE snapshots.
- Report engine readiness and protocol capabilities per node and exclude
  explicitly unready engines from scheduling.
- Add integration and contract coverage for retry boundaries, cancellation,
  streaming failover safety, request forwarding, attempt persistence, and
  engine readiness.
- Regression coverage for planner model failover after a model-load failure.
- Regression coverage for multi-page transfer, watermark handling, and task
  and memory snapshots larger than 64 MiB.

### Validation

- `go test ./... -count=1 -timeout 5m`
- `go vet ./...`
- `node --test web/app_test.js`
- `.\build\build.ps1` (Windows, Linux, and Ubuntu Linux release binaries)

## 0.0.5 - 2026-10-14

### Added

- Versioned incremental synchronization for memory and task history, with
  per-record origin/sequence metadata, persisted peer watermarks, idempotent
  merges, legacy-record backfill, and full-snapshot fallback for older peers.
- OpenAPI 3.1 and JSON Schema 2020-12 contracts for the orchestrator and node
  APIs, configuration, task/scheduler structures, node status, discovery, and
  memory records.
- Exported HTTP request/response fixtures, generated scheduler scoring
  fixtures, contract drift checks in CI, and a language-neutral porting guide.
- Configuration normalization and validation, shared domain errors and worker
  pool primitives, and persistence interfaces for memory and task history.

### Changed

- Split orchestrator, node-agent, and task-manager responsibilities into
  focused components while preserving existing routes and wire formats.
- Keep legacy bbolt databases and peers compatible; synchronization metadata is
  optional and mixed-version peers continue to use full snapshots.

### Validation

- `go test ./...`
- `go vet ./...`
- `node --test web/app_test.js`
- Windows, Linux, and Ubuntu Linux release builds

## 0.0.4 - 2026-10-13

### Added

- Cross-platform CI for Go tests, vet, dashboard tests, and Windows/Linux builds.
- Process-level integration coverage for secure pairing, authenticated requests,
  streaming inference, and restoring conversations after restart.
- A system-status API and dashboard warning when persistent memory is unavailable.
- LM Studio support and automatic discovery/rescanning of local model engines.
- LAN orchestrator election based on node capability, with stable-ID tie
  breaking and a local worker on the elected orchestrator.
- Synchronization of node memory and task history across joined and peer clusters.
- Versioned incremental memory and task-history synchronization with persisted
  per-peer watermarks and full-snapshot fallback for legacy peers.
- OpenAPI 3.1 and JSON Schema 2020-12 contracts, exported HTTP golden fixtures,
  scheduler-generated assignment fixtures, and a language-neutral porting guide.
- Shared `execute_workflow` support for Pi and DeepSeek, including dependency
  scheduling, output contracts, retries, review gates, and persistent human
  approval.
- Task checkpointing, dependency-aware execution, and capability-based model
  scheduling.
- Windows NPU utilization reporting using DXCore adapter identification and
  GPU Engine counters when available.

### Changed

- Normalize node and orchestrator configuration in one place and validate all
  settings before startup, reporting each invalid value with exit code 2.
- Separate orchestrator and node-agent HTTP handlers from their server lifecycle
  and state-management code.
- Keep DXCore COM object pointers typed to avoid unsafe integer-to-pointer
  conversions in Windows NPU telemetry.
- Add opt-in model-assisted task planning with explicit subtask roles,
  dependencies, expected outputs, and approval controls while preserving
  heuristic decomposition as the default.
- Restore in-progress work after an orchestrator restart as failed with an
  interruption reason instead of presenting it as still running.

### Validation

- `go test ./...`
- `go vet ./...`
- `node --test web/app_test.js`
- Windows and Linux release builds

## 0.0.3 - 2026-10-10

### Added

- Persistent application logging with pairing/TLS diagnostics, daily rotation,
  seven-day retention, and tray-menu access to current and archived logs.
- Unified cluster overview showing current and recent tasks alongside their
  assigned nodes, with assignment selection highlighting the execution node.
- Offline member visibility, last-seen information, and CPU/memory utilization
  history alongside existing GPU telemetry.
- Pi access to the same cluster, model, task, web, math, and memory tools as
  the DeepSeek harness.

### Changed

- Classify pending, running, completed, failed, cancelled, and unknown subtasks
  explicitly; no longer count pending or cancelled work as running.
- Replace inferred worker-to-worker topology edges with explicit
  task/subtask-to-node assignments.
- Keep scheduling APIs restricted to healthy nodes while exposing known
  offline members in dashboard snapshots.
- Limit event snapshots to the 50 most recently updated tasks and avoid
  rebuilding dashboard sections when their displayed data has not changed.
- Refresh the displayed tool catalog when switching agent harnesses.

### Validation

- `go test ./...`
- `node --check web/static/app.js`
- Windows and macOS hardware-package cross-compilation

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
