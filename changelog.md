# Changelog

## 0.0.21 - 2026-10-22

### Added

- Support optional bearer API keys for configured OpenAI-compatible engines,
  report available model size/context metadata, and document the `llamacpp`
  engine name and secret-handling behavior.
- Add an optional per-device CPU/GPU/NPU inventory with accelerator memory,
  supported backends, NUMA nodes, and network-link details while retaining the
  legacy node hardware fields.
- Report verified engine capabilities and model execution devices, match engine
  ordering and GPU/NPU model bonuses to node devices, and only credit NPU
  election strength when a ready model reports running on an NPU.

### Fixed

- Share vLLM-compatible chat request construction across generation, chat, and
  streaming, including chat temperature and max-token options.

## 0.0.20 - 2026-10-22

### Added

- Add contributor and vulnerability-reporting guidance, focused issue and
  pull-request templates, architecture and operational reference docs, and a
  short README linked to those references.
- Publish tagged Windows/Linux amd64 release archives with SHA-256 checksums;
  add a golangci-lint CI job and uploadable Go coverage reports.
- Add per-node/model transparent-router circuit breakers, separate first-byte
  and idle-stream timeouts, opt-in per-client request/concurrency limits, and
  prompt-free router metrics for attempts, failures, queue depth, latency, and
  active streams. Expose breaker states through router request events and SSE.
- Document the peer trust model and harden local dashboard access, pairing
  secret handling, certificate pin changes, inbound request limits, and TLS
  transport requirements.
- Standardize application logs on structured `log/slog` records with configurable
  levels, preserve daily rotation and seven-day retention, and correlate
  orchestrator-to-node dispatches with standard `X-Request-ID` headers.
- Add a prompt-free `--diagnostics` report with redacted configuration,
  bounded engine health probes, read-only database checks, and metadata-only
  recent error summaries.
- Require Go 1.27.3, which includes fixes for standard-library vulnerabilities
  reported by `govulncheck`.

### Fixed

- Expire idle pipeline-stage requests so lost open acknowledgements cannot
  retain node capacity indefinitely; drain requests and retire prior-plan
  stages before automatic replanning to prevent stale assignment conflicts.
- Fix the non-Windows tray build so Linux release binaries compile.

## 0.0.19 - 2026-10-21

### Changed

- Recommend local model-assisted task planning, cache successful normalized
  plans in memory, improve heuristic fan-out and synthesis, add quality/latency
  routing estimates, and support disabled-by-default p95 speculative hedging.
- Document how to verify release artifact versions and clarify that distributed
  pipeline inference remains simulated until production stage backends are added.
- Add request IDs and prompt-free request logging to production HTTP listeners,
  with a 256 MiB request-body cap layered over route-specific limits.

## 0.0.18 - 2026-10-21

### Added

- Add protocol-v2 request-local stage open/prefill/decode/close operations with
  typed binary activation tensors and verified runtime capability reporting.
- Exercise token-by-token stage dispatch and request cleanup with the simulated
  backend; ordinary llama.cpp and vLLM adapters remain non-stage-capable unless
  their runtime explicitly implements the stage protocol.
- Document the protocol-v2 stage contract and clarify that production
  llama.cpp/vLLM partial-layer backends are still pending.

## 0.0.17 - 2026-10-21

### Added

- Add a three-node simulated pipeline integration test covering contiguous
  placement, stage invocation order, and streamed OpenAI-compatible output.
- Add configured OpenAI-compatible adapters for TGI, LocalAI, MLX-LM, SGLang,
  and generic servers, plus a llama.cpp RPC deployment profile for trusted
  multi-host inference.
- Add node-side pipeline-stage loading, streaming forward and unload endpoints,
  with simulated and local HTTP stage-engine adapters.
- Add configured sharded-model placement, atomic multi-node capacity
  reservations, persisted pipeline plans, and OpenAI chat dispatch across stages.
- Show whole versus sharded models, live pipeline stages and transfer/utilization
  telemetry in the Workload view, with a control to recompute placement and load
  stages.
- Document how to configure distributed sharded models and the current PP-only
  runtime requirements and limitations.

## 0.0.16 - 2026-10-19

### Added

- Expose expanded Prometheus metrics at `/metrics` while retaining the
  `/api/metrics` alias, and add `--print-config` for normalized configuration
  output without starting services.
- Provide example systemd user and macOS launchd service definitions.
- Allow node and orchestrator configurations to tune sync page/request limits
  and timeouts while keeping the existing 64 MiB and 10-minute defaults.

### Changed

- Add reverse-proxy and metrics-scraping guidance, including the requirement to
  preserve mutual TLS for cluster traffic.
- Batch bbolt memory snapshot merges by record group and skip writes for
  unchanged dialogue records.

## 0.0.15 - 2026-10-19

### Changed

- Make the per-subtask dispatch deadline configurable, defaulting to 10 minutes
  for slower local model inference.

## 0.0.14 - 2026-10-19

### Added

- Detect unavailable orchestrators with periodic status probes and promote a
  trusted cluster node after four consecutive failures, without blocking node
  scheduling or requiring surviving members to pair again.
- Expose task dependency graphs through `GET /api/tasks/{id}/dependencies`,
  including sorted task nodes, statuses, descriptions, and dependency edges.

### Changed

- Batch periodic memory synchronization and de-duplicate buffered snapshots by
  record ID, re-queuing failed batches for retry.
- Refresh stale hardware telemetry after 30 seconds and mark unavailable GPU
  statistics explicitly so schedulers do not treat missing data as idle.
- Use atomic snapshots for built-in learned-bias and custom-profile reads in
  scheduler scoring paths.

## 0.0.13 - 2026-10-19

### Added

- Automatically provision a fitting model from the bundled Ollama catalog
  when a workflow subtask has no compatible model in the cluster.

### Changed

- Cancel model downloads when the initiating request is cancelled and allow
  longer model-pull operations than inference requests.

## 0.0.12 - 2026-10-18

### Fixed

- Make default automatic startup join an already-running orchestrator instead
  of starting a competing coordinator based on local capability. Wait one full
  discovery lease for late LAN announcements; simultaneous first starts still
  elect a coordinator by capability and stable ID.

### Added

- Add a visible join action for discovered orchestrators in the Local network
  view while retaining the context-menu action.

## 0.0.11 - 2026-10-18

### Added

- Let an unjoined node join a selected LAN-discovered orchestrator from the
  Local network context menu, using the selected coordinator's TLS identity
  and an optional one-use PIN.

## 0.0.10 - 2026-10-18

### Added

- Add an experimental durable consensus foundation with persistent cluster
  identity, mutual-TLS transport primitives, dynamic voter membership, and
  partition/restart tests. Runtime election and mutation paths are not yet
  integrated, so this release does not provide a global no-split-brain
  guarantee.

## 0.0.9 - 2026-10-18

### Fixed

- Ensure task waits do not report terminal completion before the final task
  checkpoint has been persisted.
- Correct coordinator candidate selection after switching orchestrators, and
  stop probing stale node announcements that now belong to another coordinator.
- Keep task-history sync diagnostics in the rolling log file without printing
  routine sync messages to the console.
- Persist successful dispatch attempt metadata and subtask output in one task
  snapshot, and mark in-flight attempts indeterminate when restart recovery
  finds them still running.

### Added

- Show request/attempt IDs, worker placement, status, failure class, HTTP
  status, duration, and error details for every task dispatch attempt.
- Support optional worker-local `Idempotency-Key` result replay for successful
  non-streaming generate requests; this is bounded best-effort deduplication,
  not exactly-once execution.

### Changed

- Preserve an external dispatch deadline as a timeout when it expires during a
  retry rather than reporting it as ordinary cancellation.

### Development

- Run `go test -race ./...` on the Ubuntu CI runner. Windows local race runs
  remain blocked on this host by ThreadSanitizer shadow-memory allocation
  failure (error code 87).

## 0.0.8 - 2026-10-15

### Changed

- Share node/global capacity reservations between workflow dispatch and direct
  inference routing, and expose the same cluster health/capacity view through
  the API and SSE dashboard snapshots.
- Reduce scheduler and history hot-path work with capability/readiness
  prefilters, pure score calculation, in-memory indexes, adaptive sync
  intervals, and opt-in cursor paging; split dashboard code by view and keep
  component lock and context rules documented.

### Validation

- `go test ./... -count=1`
- `go vet ./...`
- `node --test web/app_test.js`
- `.\build\build.ps1` (Windows, Linux, and Ubuntu Linux release binaries)

## 0.0.7 - 2026-10-15

### Added

- Detect and configure llama.cpp OpenAI-compatible servers, including the
  managed-server port, and filter the bundled model catalog by engine.
- Add live hardware, engine inventory, and model details to expandable node
  cards; prefer FastFlowLM on NPU-capable nodes when a model exists on multiple
  local engines.

### Changed

- Keep memory-sync diagnostics in the rolling log file without echoing them to
  the console, and reduce routine console output.
- Document local and paired-client access to the orchestrator API.

### Fixed

- Give memory synchronization a dedicated 10-minute HTTP client instead of
  the peer-control client's 5-second timeout, preventing slow transfers from
  being cut off and reported as unexpected EOF by the other node.
- Avoid detecting ordinary Windows input devices as NPUs.
- Preserve FastFlowLM identity and model names while sharing the OpenAI-
  compatible vLLM implementation.
- Use Explorer rather than `rundll32` when opening links and log files on
  Windows.

### Validation

- `go test ./... -count=1 -timeout 5m`
- `go vet ./...`
- `node --test web/app_test.js`
- `.\build\build.ps1` (Windows, Linux, and Ubuntu Linux release binaries)

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

- Report per-engine model inventory status, check timestamps, and errors, while
  retaining the last successful inventory during transient probe failures.
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
