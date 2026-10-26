# Changelog

## Unreleased

## 0.0.24.4

Released: 2026-10-25.

### Fixes

- Fixed the flaky `TestDynamicVoterMembershipAndLastVoterProtection`
  consensus test: it now removes a follower and tolerates a leader change
  while committing the removal.
- Fixed the flaky `TestPairWithAddressContextCancelsInFlightRequest` test:
  its server now aborts the connection when a request is cancelled, instead
  of sending an implicit empty `200` that could race the cancellation.
- The dual HTTP/HTTPS peer server now reports a real serve failure from either
  listener instead of discarding the second server's result; normal shutdown
  still returns no error.
- The router stream buffer pool stores `*[]byte`, so returning a buffer no
  longer allocates.

### Development

- CI runs golangci-lint v2.15 (v2.13 cannot read Go 1.27 export data) with a
  new `.golangci.yml`. The codebase now reports zero errcheck, staticcheck,
  unused, and ineffassign issues on Linux and Windows.
- Added `internal/closeutil.Discard` for deferred best-effort closes of
  read-only resources. Other unchecked errors are now handled explicitly.
- Removed 43 unused functions.
- The node reverse proxy uses `httputil.ReverseProxy.Rewrite` instead of the
  deprecated `Director` hook. It keeps the same forwarding headers.
- `.goreleaser.yaml` uses `archives.ids` instead of the deprecated
  `archives.builds`.
- CI runs govulncheck v1.9.0. v1.1.4 panics on Go 1.27 syntax.

### Compatibility

- API routes, JSON wire formats, configuration keys, and storage formats are
  unchanged.

## 0.0.24.3

Released: 2026-10-25.

### Development

- Added `build/test-race.ps1` and `build/test-race.sh` to run the race
  detector. Both default to `./...` when given only flags. On Windows, the
  script finds the MinGW compiler and works around DLL shadowing in `PATH`
  (for example, older DLLs in `C:\Program Files\Multipass\bin` that crash
  `cc1.exe` with `0xC0000139`), so native Windows race tests run without
  changing system settings.
- Shell scripts are checked out with LF line endings on every platform.
- Development docs describe the cgo toolchain fix; native Windows race tests
  are no longer documented as unsupported.

### Compatibility

- No API, configuration, storage, or wire-format changes.

## 0.0.24.2

Released: 2026-10-25.

### Performance

- Nodes and orchestrators skip building dashboard snapshots when nobody is
  subscribed to `/api/events`. Shared publishers coalesce bursts away from
  request handlers and suppress byte-identical frames; newly connected
  subscribers still receive current state. SSE payloads and framing are unchanged.
- Reuse named HTTP timeout profiles and connection pools while retaining
  certificate pinning, redirect policies, custom timeouts, and streaming
  cancellation.

### Reliability

- Cancel and join background publishing, health, registration, discovery,
  telemetry, inventory, and adaptive synchronization work before closing shared
  stores. Owned HTTP listeners and active upstream SSE requests stop with their
  service.
- Bound peer task-list reads by the existing configured sync limit, check HTTP
  errors, and cancel in-flight reads when snapshot publishing stops.
- Split supervisor and shard planning/lifecycle responsibilities into focused
  files without changing configuration, API, or storage formats.
- Publish a shard health teardown's failed state and release its operation
  claim together after the persistence attempt, so an immediate replacement
  cannot observe `failed` and then receive a transient model-exists conflict.

### Development

- Isolate agent and orchestrator test fixtures from host engine/hardware
  probing and join background work during cleanup. Preserve existing tests
  while reducing local package runtimes.
- Add weekly/manual snapshot, scheduler, and sync-paging benchmarks with
  repeatable samples and environment metadata.
- Check generated dashboard assets for drift in Linux CI. Normalize generated
  asset line endings so Windows and Linux builds produce identical content
  hashes and gzip/Brotli sidecars.
- Refresh the engineering assessment and document local Ubuntu WSL race
  validation with Go 1.27.3.

## 0.0.24.1

### Versioning

- Add a fourth numeric iteration component for rapid releases:
  `0.0.24.1`, `0.0.24.2`, ..., `0.0.24.10`. Comparisons are numeric;
  `0.0.25` remains newer than every `0.0.24.x` iteration. The prior local
  0.0.25 development build was not a published release; install this build
  manually on any node running that development version, since automatic
  updates intentionally never downgrade.
- Accept four-component versions in peer manifests and schemas; avoid numeric
  overflow when comparing long iteration numbers. Build scripts generate
  platform bundle metadata, and release automation packages four-component
  tags without relying on SemVer-only release tooling.

### Fixed

- Persist a retryable failure on the actual shard group when a membership
  replan cannot place every selected node, instead of leaving the group stuck
  in planning because a node ID shadowed its group ID.
- Advertise ready shard text-model capabilities to the scheduler even when the
  worker reports explicit hardware-only capabilities, so writing, summaries,
  and translation are not incorrectly rejected as having no compatible model.
- Let initial workflow planning use automatic acquisition when the cluster has
  no eligible planning model; keep ordinary writing roles out of hard capability
  requirements in generated plans.
- Keep registered workers online when a combined-role host alternates worker
  and coordinator-candidate LAN announcements under the same ID. Probe the
  trusted worker address instead of treating the role change as an expired
  announcement; retain offline detection and foreign-coordinator exclusions.
- Verify automatically managed shard heads against saved model metadata on
  startup, recovery, health checks, and inference. Restore matching local GGUF
  weights when starting a stopped group whose shared head was reconfigured for
  another model; fail explicitly if the original weights are unavailable.
- Honor `dispatch_timeout_seconds` for inference HTTP calls, including shard
  planning and synthesis, instead of enforcing a separate fixed ten-minute
  client limit. Include shard request-slot waits in the budget and preserve
  caller cancellation. Remove the node-dashboard task-submission proxy's
  competing fixed timeout.
- Consume managed shard generation as SSE and flush node-proxy headers/chunks
  immediately. Log safe progress counts (including reasoning-only output)
  without prompt/answer text, reject truncated/error streams explicitly, and
  explain generation timeout failures with configuration guidance.
- Include ready shard models in automatic task planning, work execution, and
  answer synthesis. Route all stages through the managed head, serialize group
  requests, and avoid reserving already-held member slots a second time.
- Explain unavailable planning models with actionable readiness guidance.
- Allow ten minutes for managed model-head loading instead of terminating large
  distributed GGUF loads after thirty seconds. Keep RPC worker startup short,
  extend managed-start and dashboard shard-proxy deadlines, and retain
  cancellation and failed-start cleanup across restarts.
- Refresh per-device accelerator memory through the installed llama.cpp runtime,
  including Vulkan devices previously reported without free memory. Publish
  installation-time readings before planning and clear failed runtime readings.
- Let the shard wizard select its model-head node and reuse local Ollama/LM
  Studio GGUF weights without requiring an already loaded model. Persist a
  matching automatic head and preserve the explicitly selected head in planning.
- Inspect managed-worker availability before model selection and explain
  unavailable engines and inventory failures in the shard wizard.

### Added

- Version 0.0.24.1: automatically stage newer executables directly from pinned
  paired peers and apply only while the entire local process is idle. Preserve
  startup arguments/configuration, retain the previous executable, and support
  packaged Windows/Linux target bundles. Add `--no-peer-updates`; explicitly
  document the accepted unsigned-peer executable trust risk and deferred
  signing/publisher-authorization work. Existing versions require a one-time
  manual upgrade to gain bootstrap support.
- Add **Edit members** to shard-group cards: add/remove worker nodes, install
  missing RPC runtimes, drain inference, release old placement, refresh device
  telemetry, and reload the same saved model/group with comfortable-memory
  proportional placement. Retain the model head; moving weights to another
  head is not supported by membership editing. Add the protected
  `POST /api/shards/{id}/members` route and matching contracts/fixtures.
- Recover missing task models by loading compatible local GGUF weights on idle
  managed nodes, then automatically downloading a recommended public Hugging
  Face Qwen2.5 Instruct GGUF if necessary. Verify LFS SHA-256, exact size and
  GGUF metadata, persist the cache, and start a managed single-node group before
  retrying assignment. Preserve active groups and operator heads.
- Add protected `download-model` managed-engine APIs and the additive
  `huggingface` local-inventory source. Older nodes must be upgraded to download;
  existing routes, configuration defaults, and persistence formats are unchanged.
- Add dashboard and API start/stop/restart for saved shard groups. Stop drains
  inference and retains configuration; start/restart replan current memory with
  the saved model, pins, and head, retain the group ID, and protect engines owned
  by other groups.
- Extend the shard-group state enum with `stopped` (additive contract migration;
  existing records require no rewriting). Clients must accept this unavailable
  state. Stopped groups remain stopped across recovery and shutdown. Interrupted
  `stopping` records now recover to retained `stopped` groups rather than being
  deleted; reissue DELETE to remove an interrupted deletion.
- Delete failed shard groups from dashboard cards, including node-dashboard
  forwarding, duplicate-click protection, and explicit cleanup errors.
- Document shard chat usage, explicit OpenAI requests, and failed-group recovery.
- Automatically download, SHA-256 verify, install, and persist missing llama.cpp
  RPC workers when selecting nodes in the shard wizard. Support official
  Vulkan archives and custom llama-xdna.cpp release archives on Windows/Linux
  amd64. Check installer-managed workers for updates while idle, defer active
  updates, and preserve operator-configured workers and existing installations.
- Add current-orchestrator-protected worker installation APIs, with matching
  dashboard forwarding, schemas, and fixtures. Local-model discovery and
  idle-only head configuration use the same authenticated control path.
  System drivers and vLLM setup remain operator-managed.

## 0.0.24 - 2026-10-24

### Added

- Let operators Ctrl-click a healthy cluster node to open the shard-group
  setup wizard, select compatible managed engines and models, and create
  memory-aware placements with a 2 GiB per-device safety margin.
- Expose protected model-profile inspection for allow-listed GGUF files and
  cached/local Hugging Face models without returning their filesystem paths.
- Add a dashboard "Refresh view" button that re-fetches only the active tab's
  data. A browser reload now restores the active tab, sub-tab and mAIstr0 chat
  transcript for that browser tab (stored in `sessionStorage`).

### Fixed

- Make mAIstr0 chat replies readable in the light colour scheme, which
  previously showed dark text on a hard-coded black bubble.
- Remove the welcome card when the first chat message appears, so replies
  scroll inside the chat instead of overflowing the page until a reload.
- Answer read-only cluster-status questions directly from live inventory,
  health, task, shard, and version APIs instead of requiring an LLM-generated
  JSON task plan. Report common model names, offline nodes, unavailable
  inventories, and partial API failures explicitly.
- Retry invalid automatic task plans once per selected model, then try another
  eligible model. Validate JSON, graph dependencies, and task contracts before
  accepting/caching a plan; preserve cancellation and terminal error responses.
- Send LAN discovery announcements from every local IPv4 address so
  multi-homed hosts (for example a Hyper-V switch plus a bridge on one subnet)
  stay visible; previously one OS-chosen interface could leave a node's
  announcements unheard and cause a split brain.
- Stop dashboard node lists from flickering, dropping, or duplicating cards.
  The keyed DOM patcher anchored insertions on an element it had just removed
  whenever a card's live telemetry changed, so most live updates threw and
  left the Overview, Nodes, and Local network lists partially rendered.
- Log and rebind the discovery listener after UDP read errors instead of
  silently stopping discovery.
- Make a failover-promoted orchestrator step down and rejoin as a node when a
  trusted orchestrator that still answers as elected outranks it, is the one
  it replaced, or leads more members. A later failover reinstates it.
- Answer cluster-status questions from node dashboards: node agents now proxy
  read-only `GET /api/cluster/health` and `GET /api/shards` to the joined
  orchestrator, and forward the `GET /api/tasks` query string (pagination).
  Nodes without engines are reported as "none configured or detected" instead
  of "legacy node".

## 0.0.23 - 2026-10-24

### Added

- Add fake llama.cpp RPC, llama-server, and vLLM headless process binaries and
  integration coverage for managed group creation, streaming/non-streaming
  router chat, killed workers, degraded/failed teardown, and rebuild.
- Build the S3 supervisor and sharding fake binaries on Linux and Windows CI;
  retain Linux race and govulncheck gates. Generate typed shard fixtures and
  check all shard operations, schemas, and untracked fixture drift.
- Document sharding lifecycle, engine/OS CPU/GPU/NPU support requirements,
  threat boundaries, troubleshooting, and an entirely untested real-machine
  hardware validation checklist.
- Document additive RPC relay/backend member fields in the shard-group schema;
  no route, persistence format, or configuration default changes.
- Protect llama.cpp worker traffic with pinned mutual-TLS relays and require
  trusted private interfaces for distributed vLLM traffic, with an explicit
  insecure-network override and warnings.
- Add the optional `llamacpp-xdna2-npu` managed-engine profile for the
  enhanced XRT-backed llama.cpp build, including explicit XDNA device
  selection, single-sequence operation, CPU fallback, and Q4NX deployment
  guidance.

### Changed

- Split DeepSeek tools and harness execution/routing, and memory-store record,
  performance, and synchronization responsibilities into files below 500
  lines without changing public APIs, behavior, or bbolt encodings.

### Fixed

- Make Linux vLLM supervisor tests use valid model-ID configuration and wait
  for child startup evidence rather than racing process readiness.

## 0.0.22 - 2026-10-22

### Added

- Add an allow-list-backed node supervisor for llama-server, ggml-rpc-server,
  and headless vLLM workers, with paired-orchestrator lifecycle APIs, readiness
  checks, bounded restart backoff, and process-tree shutdown.
- Add typed OpenVINO, Hexagon, and FastFlowLM XDNA 2 NPU profiles with
  vendor-aware engine matching, NPU model eligibility explanations, and
  operator-only RPC validation opt-in.

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
