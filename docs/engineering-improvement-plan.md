# Engineering Improvement Plan

Assessment date: 2026-10-25
Repository: `dbhaigh/mAIstr0`
Scope: Current-state assessment of Phases 0–8 and implementation of the
follow-up publishing, lifecycle, networking, structure, and feedback plan.

See the [phase prompt pack](../improvements.md) for goals and acceptance
criteria. This assessment records current implementation status; it is not a
replacement for review or CI.

## Assessment boundaries

Phase 9 [quality gates](./ci-quality-gates.md) separate consistent fast PR checks
from full qualification, independently validate packages, pin actions and limit
permissions, and enforce approved same-run timing/allocation comparisons.
Required aggregate jobs fail on failed/skipped prerequisites. GitHub `main`
was unprotected at inspection; enabling the documented required-check ruleset
after publication remains a repository-administrator action, not a local pass.

Phase 8 now adds [end-to-end measurement and release acceptance](./end-to-end-benchmarks.md):
54 recorded synthetic direct/single/multi-router results, opt-in real-engine
HTTP tooling, correlated privacy-safe router lifecycle diagnostics, explicit
dashboard availability/freshness, and reusable CI gating both tag formats.
The checked-in measurements are synthetic, not hardware qualification. Native
package replacement tests use the same-source second build, not a historical
release. Local evidence does not establish remote CI or publication success.

- The repository uses Go 1.27.3 and an embedded static dashboard. The dashboard
  has an esbuild package manifest, modular source, generated bundles, and Node tests.
- Repository-wide agent guidance and contributor workflow are in
  [`AGENTS.md`](../AGENTS.md) and [`CONTRIBUTING.md`](../CONTRIBUTING.md).
- The implementation started from clean commit `99b1520` on
  `copilot/ubuntu-race-validation` and is included in release `0.0.24.2`.
  The branch incorporates GitHub main's merge commit `9531014`; local validation
  is recorded below, while remote CI remains an independent gate.
- Go 1.27.3 is installed in the WSL user's home directory for local Linux race
  validation. Native Windows race tests pass through `build\test-race.ps1`,
  which works around compiler DLLs shadowed in `PATH`. Older Windows
  configurations failed with ThreadSanitizer error 87.

## Current architecture

### Entry points and composition

- `cmd/maistr0/main.go` parses CLI flags and configuration, performs startup role selection, then starts a node agent, orchestrator, or both. Automatic election advertises a candidate for a fixed startup window before selecting a visible winner.
- `internal/orchestrator/server.go` constructs the registry, task manager, node client, dispatcher, planner, election and harness components; `Server.Mux` registers the orchestrator API and serves embedded assets. HTTP handlers are split among `api_*`, `compat_*`, and related files.
- `internal/nodeagent/agent.go` wires node state, engine inventory, telemetry, identity, orchestrator link, executor, peer dispatch and dialogue components. `internal/nodeagent/agent_server.go` registers the node API and starts its listeners and background registration/discovery work.
- `internal/background` owns cancellation and joining of service background work.
  `internal/hub.Publisher` serializes/coalesces snapshots away from HTTP handlers;
  `internal/httpx.NewClient` centralizes timeout profiles without replacing
  certificate pinning, redirect policy, or streaming lifetime control.

### Major responsibilities and dependencies

| Area | Main implementation | Tests / evidence |
|---|---|---|
| HTTP APIs and errors | `internal/orchestrator/server.go`, `api_*_handlers.go`, `compat_api.go`, `internal/nodeagent/agent_server.go`, `internal/nodeagent/api_handlers.go`; shared error classification in `internal/apperr` | `internal/orchestrator/api_contracts_test.go`, `workflow_contracts_test.go`, `internal/nodeagent/agent_test.go`, `contracts/contracts_test.go` |
| Discovery, pairing and membership | `internal/discovery/discovery.go`, `internal/peeridentity/identity.go`, `internal/peeridentity/dualserver.go`, `internal/orchestrator/election_component.go`, `internal/nodeagent/orchestrator_link.go`, `internal/cluster/registry.go` | `internal/discovery/discovery_test.go`, `internal/peeridentity/*_test.go`, `internal/cluster/registry_test.go` |
| Inference engines and model inventory | `internal/engine/engine.go` and adapter files; node-local probing and reported status in `internal/nodeagent/engine_component.go` | `internal/engine/*_test.go`, `internal/nodeagent/agent_test.go`, `components_test.go` |
| Routing and scheduling | `internal/inferroute/route.go`, `internal/orchestrator/router_handlers.go`, `node_client.go`, `dispatcher_component.go`, `internal/scheduler/assign.go` | `internal/inferroute/route_test.go`, `internal/orchestrator/router_handlers_test.go`, `internal/scheduler/*_test.go` |
| Worker execution and dispatch | Shared generic `internal/workpool/pool.go`; orchestrator dispatch and node-capacity gates in `internal/orchestrator/dispatcher_component.go` and `node_dispatch_gate.go`; node execution and peer dispatch in `internal/nodeagent/executor_component.go` and `components.go` | `internal/workpool/pool_test.go`, `internal/orchestrator/node_dispatch_gate_test.go`, orchestrator and node-agent tests |
| Task lifecycle and recovery | `internal/taskmgr/lifecycle.go`, `deps.go`, `recovery.go`, `sync.go`, `sync_pages.go`; orchestration in `internal/orchestrator/task_planning.go` | task-manager contract, recovery, sync, and lifecycle tests; orchestrator workflow tests |
| Persistence and synchronization | `internal/taskmgr/boltstore.go` plus its `Store` interface and in-memory store; `internal/memory/memory.go`, `boltstore.go`, `inmemory.go`, `sync.go`, `sync_pages.go`; node and orchestrator sync handlers | legacy bbolt fixtures, memory/task sync tests, restart and watermark tests |
| Configuration and secrets | `internal/config/config.go` normalizes and validates node/orchestrator configuration; `internal/peeridentity` owns certificates, trust pins and pairing PINs | `internal/config/config_test.go`, `internal/peeridentity/*_test.go` |
| Web UI | `web/src/` ES modules bundled to hashed assets under `web/static/`; embedded through `web/` | `web/app_test.js` and `web/server_test.go` |
| Language-neutral contracts | `contracts/openapi.yaml`, `contracts/schemas/`, `contracts/fixtures/`, package-level test fixtures | `contracts/contracts_test.go`; CI runs contract generation and drift checks |

The broad dependency flow is: CLI/configuration → orchestrator and/or node-agent composition roots → cluster registry, scheduler, task manager and engine adapters. The orchestrator communicates with paired nodes through its node client and authenticated HTTP; nodes report status and execute routed work. Memory and task stores provide persistence and peer synchronization. The dashboard consumes the existing APIs and SSE snapshots.

## Baseline and validation

The clean starting commit and the implementation passed local build, vet, Go
tests, and dashboard tests. Release `0.0.24.2` packages the follow-up work.
Confirm the CI matrix after synchronization; local checks do not establish
remote CI success. Do not treat older validation results as evidence for later
changes.

CI in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) regenerates and
checks contract fixtures, runs `go test ./contracts`, `go test ./...`,
`go vet ./...`, `go build ./cmd/maistr0`, and `node --test web/app_test.js` on
Ubuntu and Windows. It also runs `go test -race ./...` on Linux. The exact
commands and known local race limitations are documented in
[Development](./development.md).

Local implementation validation on 2026-10-25:

- `go build ./...` and `go vet ./...`: passed.
- `go test ./... -count=1`: passed on Windows.
- `go test -race ./...`: passed in Ubuntu WSL with Go 1.27.3.
- `node --test web/app_test.js`: 58 tests passed.
- `go generate ./contracts ./internal/scheduler`: no fixture drift.
- `npm --prefix web run build`: byte-for-byte reproducible generated assets.

The new scheduled workflow has been added but has not been run in GitHub Actions
as part of this local implementation.

## Responsibility-based follow-up

The current Phase 7 follow-up moves node execution accounting and runtime model
selection out of shared agent state into their existing owners. It removes the
executor's general-state dependency and inventory's selection-state interface,
unifies cached flag calculation, and adds domain-only characterization and race
coverage. Two reproduced cache-consistency defects are fixed without changing
API/configuration/storage contracts or admission scope.

The [architecture assessment](./architecture.md#node-ownership-boundaries)
documents ownership, retained cohesive harness/shard boundaries, and why no new
package/interface split was warranted. The
[ownership comparison](./concurrency-ownership.md#phase-7-ownership-comparison)
records dependencies, lock invariants, testability, duplication, and repeated
before/after allocation measurements. This follow-up is distinct from the
historical prompt-pack phase numbering below.

## Status of items in `improvements.md`

| Phase | Assessment | Current evidence and next action |
|---|---|---|
| 0 — Stabilize baseline | **Clean starting baseline; implementation review pending** | Started from `99b1520`, not the mixed October 18 worktree. Local Linux race validation is available through WSL; CI remains the release gate. |
| 1 — Characterization and contract tests | **Implemented** | The matrix below maps lifecycle, scheduler, compatibility, configuration, status, and error behavior to existing assertions. SSE framing, payload equivalence, reconnect delivery, and cancellation have additional regression tests. |
| 2 — Config validation and normalization | **Implemented** | `NodeConfig` and `OrchestratorConfig` normalize and validate values, with constructor use and table-driven tests. Preserve existing keys and defaults. |
| 3 — Separate domain errors from HTTP | **Implemented** | `internal/apperr` supplies typed categories and status mapping. Keep domain code independent of HTTP and preserve response bodies. |
| 4 — Unify dispatch/worker pool | **Implemented** | `internal/workpool` serves orchestrator and node-agent work; node-capacity gating remains a separate orchestrator concern. Preserve the tested cancellation and capacity semantics. |
| 5 — Persistence behind interfaces | **Implemented** | Task and memory stores have interfaces, bbolt adapters, in-memory implementations, and recovery/compatibility tests. Preserve on-disk encodings and locking rules. |
| 6 — Split god objects | **Implemented; continuing maintenance** | Supervisor lifecycle/process/health/runtime, shard-group lifecycle/placement/state/network/health, and shard validation/memory/llama/vLLM placement now have focused files. These three original files and their extractions are each below 500 lines. Other files, including routing and pipeline dispatch, remain larger; there is no repository-wide 500-line claim. Lock ownership and signatures are preserved. |
| 7 — Incremental memory sync | **Implemented** | Versioned watermark/cursor sync has a full-snapshot compatibility path. Keep merges idempotent and advance watermarks only after accepted data. |
| 8 — Language-neutral artifacts | **Implemented** | OpenAPI, JSON Schemas, exported HTTP fixtures, scheduler fixtures, and a porting guide are checked by tests and CI drift generation. Maintain them with every intentional contract change. |

### Phase 1 coverage map

| Behavior | Existing assertions |
|---|---|
| Heuristic, explicit subtasks/graph, automatic planning and planner failure | `TestTaskCreateHTTPContracts`, `TestAutoPlanHTTPContracts`, and `TestAutoPlanRetriesWithAnotherModelAfterModelLoadFailure` |
| Dependency ordering/output propagation, reviewer insertion, approval and cancellation | `TestTaskDependencyReviewAndApprovalContracts`, `TestTaskHTTPErrorAndCancelContracts`, and `TestGraphTopologicalOrderContract` |
| Task execution failure, restart recovery, and checkpoint visibility after completion | `TestTaskExecutionFailureHTTPContract`, `TestInterruptedTaskRecoveryHTTPContract`, `TestRestartRecoveryContract`, `TestCompletedTaskCheckpointContract`, and `TestWaitDoesNotReturnBeforeTerminalCheckpointIsPersisted` |
| Deterministic assignment, unassignable tasks, no healthy nodes, and telemetry freshness | `TestAssignmentContractForFixedNodeAndModel`, `TestAssignmentErrorsAndTelemetryContract`, `TestScheduler_TelemetryFreshnessWindow`, and `TestAssignIgnoresStaleAcceleratorTelemetry` |
| OpenAI/Ollama/Anthropic success and stream framing, plus validation error envelopes | `TestCompatibilityAPIContracts`, `TestCompatibilityErrorContracts`, `TestOpenAIStreamingPreservesOptionsAndMessageRoles`, and `TestAnthropicTextBlocksAndRequestValidation` |
| Configuration defaults, overlays, missing files, malformed files, and validation | `TestConfigDefaultsAndOverlaysContract`, `TestConfigMissingFilesKeepDefaults`, `TestConfigMalformedFilesReturnErrors`, and `TestConfigValidationReportsAllViolations` |
| Status and persistence-degradation shape; registered routes and schemas | `TestSystemStatusContract`, `TestSystemStatusReportsUnavailablePersistence`, `TestRegisteredHTTPRoutesAreDocumented`, `TestRepresentativeValuesValidateAgainstJSONSchemas`, and `TestSchemaSetIsComplete` |

## Prioritized follow-up

1. **Review the implementation and CI:** keep all wire, configuration, and storage
   contracts stable; review moved declarations separately from behavior changes.
2. **Measure before optimizing:** use the scheduled benchmark reports and fresh
   test timing profiles. Benchmark synchronous snapshot construction separately
   from queued publish requests so idle fast paths cannot hide serialization cost.
3. **Keep lifecycle ownership explicit:** stop and join background work before
   stores or engines close; pass cancellation into network requests and adaptive
   synchronization waits. Already-running hardware/engine probes with legacy
   interfaces finish under their existing bounds before the owner closes.
4. **Maintain completed phases:** prefer existing shared helpers and semantic
   contract tests over new abstractions or arbitrary file-size splits.

## Follow-up plan implementation

- **A - Publishing:** requests are nonblocking, bursts coalesce, unchanged bytes
  are suppressed, idle publishers skip snapshot construction, and new subscribers
  still receive an unchanged current snapshot. Slow-client delivery failures
  remain retryable. Payloads and `data: JSON\n\n` framing are unchanged. Leader
  change logging runs independently of snapshot serialization.
- **B - Lifecycle and network:** cancellable periodic/adaptive loops and owned
  dual HTTP listeners stop with the service. Sync recovery uses the same joined
  lifecycle. Named HTTP timeout profiles preserve custom transports and existing
  nonzero timeouts; streaming uses request cancellation rather than a total
  timeout. Peer task reads check status, bound bytes using the configured sync
  limit, reject malformed payloads, and propagate cancellation.
- **C - Structure:** the three requested large ownership surfaces are split
  without changing their declarations' behavior or lock ownership. This
  assessment and the prompt pack now describe the actual baseline.
  Faster request handling exposed a terminal-state visibility race during shard
  health teardown: `failed` could be read before its operation claim was released.
  A separate regression fix publishes that state and releases the claim together
  after persistence, while keeping store I/O outside the state lock.
- **D - Feedback:** test timing profiles, snapshot build/request benchmarks, a
  scheduled benchmark workflow, and a documented dashboard bundle audit provide
  reproducible checks. Keep real process lifecycle tests rather than shortening
  their assertions solely to hit a timing target.
  Isolated uncached Windows profiles measured node-agent tests at
  77.391s -> 4.379s (94.3% faster) and orchestrator tests at
  64.247s -> 17.328s (73.0% faster), exceeding the 50% reduction target.
  The stable dashboard aliases are retained as compatibility URLs; CI checks
  generated asset drift and tests the actual hashed deployed script.

## Highest-risk design and correctness questions

- **Coordinator authority during partitions:** startup election and `discovery.IsElectedOrchestrator` choose from currently visible announcements; `cmd/maistr0/main.go` uses a startup candidate window, and `internal/discovery/discovery.go` expires peers by TTL. This is a LAN ranking/election mechanism, not a quorum or consensus protocol. A partition can make independent groups believe their local candidate wins. Before changing it, document the intended split-brain behavior and identify any fencing/authority checks in every state-changing API.
- **Ambiguous dispatch outcomes:** `internal/orchestrator/dispatcher_component.go` retries transient dispatch failures across distinct node/model pairs under a deadline. If a worker completes an inference but the response is lost, a retry can repeat compute. Characterize this boundary and cancellation behavior; do not claim exactly-once execution without an idempotency mechanism that spans the worker and failure boundary.
- **Large ownership surfaces remain:** routing and pipeline dispatch still span
  large files, and `nodeagent.Agent` embeds several stateful components. Further
  refactors need a concrete ownership boundary, not another line-count target.
- **Contract scope:** the route test parses literal `ServeMux` registrations and verifies OpenAPI presence, while separate golden/API tests cover selected behavior. Route enumeration alone does not prove every handler's status, error body or streaming behavior; keep semantic contract assertions alongside it.

## Compatibility constraints and acceptance criteria

- Preserve API routes, methods, status codes, JSON and streaming response shapes unless an explicit phase documents a compatible change.
- Keep config keys, defaults, legacy config loading and supported engine names stable.
- Do not change bbolt bucket names, key encodings, task recovery behavior or sync watermarks without fixture-backed migration/compatibility tests.
- Maintain mTLS certificate pinning, pairing PIN expiry/single-use behavior, and loopback-only local administrative routes.
- For each implementation phase, identify the relevant tests first; run `go test ./...`, `go vet ./...`, and `node --test web/app_test.js` as applicable. Run `go test -race` where supported and report any environment limitation rather than treating it as a pass.
- Keep release/package generation separate from ordinary validation when it would overwrite existing artifacts under `dist/`.

## Focused security follow-up

The locally implemented [security review and hardening](./security-review.md)
records authenticated first-contact enrollment, opt-in peer executable updates,
credential/log privacy, certificate binding, and permission regression coverage.
See its verification results and the
[intentional compatibility migration](./pairing-and-security.md#migration).
These working-tree changes have not been committed or merged.

## Phase dependencies

The existing improvement plan makes Phase 1 depend on Phase 0; Phases 2–5 may proceed independently after Phase 1; Phase 6 depends on Phases 3, 4 and 5; and Phase 7 depends on Phase 5. Phase 8 provides the contract/drift guardrail across phases. Reconfirm these dependencies during maintainer review and do not start a later phase until its prerequisites are reviewed and merged.
