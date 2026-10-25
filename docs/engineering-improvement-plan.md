# Engineering Improvement Plan

Assessment date: 2026-10-18
Repository: `dbhaigh/mAIstr0`
Scope: Current-state assessment of Phases 0–8. This document update is
documentation-only and does not change production behavior.

See the [phase prompt pack](../improvements.md) for goals and acceptance
criteria. This assessment records current implementation status; it is not a
replacement for review or CI.

## Assessment boundaries

- The repository is a Go 1.27 module with an embedded static web dashboard. The dashboard has a Node test file but no separate frontend package manifest.
- Repository-wide agent guidance and contributor workflow are in
  [`AGENTS.md`](../AGENTS.md) and [`CONTRIBUTING.md`](../CONTRIBUTING.md).
- At assessment time, `main` was at `78d00f0`; the worktree was already dirty
  across contracts, orchestrator, node-agent, task-manager, and dashboard
  files. Phase status therefore distinguishes implemented source from work
  still awaiting review and merge.
- Phase 0's clean-baseline requirement is not currently met. The full Linux
  race suite passed in Ubuntu 24.04 WSL with Go 1.27.2; native Windows remains
  blocked by ThreadSanitizer error 87. See [Development](./development.md).

## Current architecture

### Entry points and composition

- `cmd/maistr0/main.go` parses CLI flags and configuration, performs startup role selection, then starts a node agent, orchestrator, or both. Automatic election advertises a candidate for a fixed startup window before selecting a visible winner.
- `internal/orchestrator/server.go` constructs the registry, task manager, node client, dispatcher, planner, election and harness components; `Server.Mux` registers the orchestrator API and serves embedded assets. HTTP handlers are split among `api_*`, `compat_*`, and related files.
- `internal/nodeagent/agent.go` wires node state, engine inventory, telemetry, identity, orchestrator link, executor, peer dispatch and dialogue components. `internal/nodeagent/agent_server.go` registers the node API and starts its listeners and background registration/discovery work.

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
| Web UI | `web/static/index.html`, `app.js`, `style.css`; embedded through `web/` | `web/app_test.js` |
| Language-neutral contracts | `contracts/openapi.yaml`, `contracts/schemas/`, `contracts/fixtures/`, package-level test fixtures | `contracts/contracts_test.go`; CI runs contract generation and drift checks |

The broad dependency flow is: CLI/configuration → orchestrator and/or node-agent composition roots → cluster registry, scheduler, task manager and engine adapters. The orchestrator communicates with paired nodes through its node client and authenticated HTTP; nodes report status and execute routed work. Memory and task stores provide persistence and peer synchronization. The dashboard consumes the existing APIs and SSE snapshots.

## Baseline and validation

The working tree is not a clean-HEAD baseline, so results from earlier
checkpoints must not be interpreted as validation of every current change.
Before closing Phase 0, review and merge or move the in-flight work to a
focused branch, then confirm the CI workflow is green.

CI in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) regenerates and
checks contract fixtures, runs `go test ./contracts`, `go test ./...`,
`go vet ./...`, `go build ./cmd/maistr0`, and `node --test web/app_test.js` on
Ubuntu and Windows. It also runs `go test -race ./...` on Linux. The exact
commands and known local race limitations are documented in
[Development](./development.md).

## Status of items in `improvements.md`

| Phase | Assessment | Current evidence and next action |
|---|---|---|
| 0 — Stabilize baseline | **Partial** | `.gitignore` covers `.flm/*.tmp`; CI has a Linux race job, but a clean reviewed baseline and an unqualified race result are still required. The current working tree is dirty. |
| 1 — Characterization and contract tests | **Coverage complete in the current worktree; review/merge pending** | The matrix below maps the requested lifecycle, scheduler, compatibility, configuration, status, and error behaviors to assertions. Current changes add terminal-checkpoint and compatibility-error fixtures; the former `BUG:` skip is now a failing assertion. |
| 2 — Config validation and normalization | **Implemented** | `NodeConfig` and `OrchestratorConfig` normalize and validate values, with constructor use and table-driven tests. Preserve existing keys and defaults. |
| 3 — Separate domain errors from HTTP | **Implemented** | `internal/apperr` supplies typed categories and status mapping. Keep domain code independent of HTTP and preserve response bodies. |
| 4 — Unify dispatch/worker pool | **Implemented** | `internal/workpool` serves orchestrator and node-agent work; node-capacity gating remains a separate orchestrator concern. Preserve the tested cancellation and capacity semantics. |
| 5 — Persistence behind interfaces | **Implemented** | Task and memory stores have interfaces, bbolt adapters, in-memory implementations, and recovery/compatibility tests. Preserve on-disk encodings and locking rules. |
| 6 — Split god objects | **Partial; review pending** | Task-manager and node-agent responsibilities are split into components, including dispatch-attempt persistence and status reporting. The current non-test files in `internal/taskmgr`, `internal/nodeagent`, and `internal/orchestrator` are all below 500 lines (largest measured: `router_handlers.go`, 472 lines). Review explicit dependency boundaries and lock ownership before declaring the phase complete; no further split is justified by file size alone. |
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

1. **Re-establish the baseline:** review the current mixed worktree, keep unrelated changes intact, and merge or move work to focused branches. Confirm the full CI matrix before marking Phase 0 complete.
2. **Close Phase 1 deliberately:** the coverage audit is complete in the current worktree. Review and merge the new goldens with the test changes; add no duplicate coverage.
3. **Close Phase 6 review:** review the current task-manager and node-agent extractions and lock/dependency ownership. Continue only if that review finds a concrete responsibility boundary.
4. **Maintain completed phases:** treat Phases 2–5, 7, and 8 as maintenance unless an audit finds a specific unmet criterion. Keep tests, contracts, and documentation synchronized with later changes.

## Highest-risk design and correctness questions

- **Coordinator authority during partitions:** startup election and `discovery.IsElectedOrchestrator` choose from currently visible announcements; `cmd/maistr0/main.go` uses a startup candidate window, and `internal/discovery/discovery.go` expires peers by TTL. This is a LAN ranking/election mechanism, not a quorum or consensus protocol. A partition can make independent groups believe their local candidate wins. Before changing it, document the intended split-brain behavior and identify any fencing/authority checks in every state-changing API.
- **Ambiguous dispatch outcomes:** `internal/orchestrator/dispatcher_component.go` retries transient dispatch failures across distinct node/model pairs under a deadline. If a worker completes an inference but the response is lost, a retry can repeat compute. Characterize this boundary and cancellation behavior; do not claim exactly-once execution without an idempotency mechanism that spans the worker and failure boundary.
- **Large ownership surfaces remain:** `internal/nodeagent/api_handlers.go` and `internal/orchestrator/dispatcher_component.go` remain large, and `nodeagent.Agent` still embeds several stateful components. Refactors must preserve lock ownership, callback/context behavior and API contracts; the Windows race-test failure makes independent race validation especially important.
- **Contract scope:** the route test parses literal `ServeMux` registrations and verifies OpenAPI presence, while separate golden/API tests cover selected behavior. Route enumeration alone does not prove every handler's status, error body or streaming behavior; keep semantic contract assertions alongside it.
- **WIP release notes:** the inventory feature is documented in README but not yet in the changelog. Resolve that before committing the existing user-visible change.

## Compatibility constraints and acceptance criteria

- Preserve API routes, methods, status codes, JSON and streaming response shapes unless an explicit phase documents a compatible change.
- Keep config keys, defaults, legacy config loading and supported engine names stable.
- Do not change bbolt bucket names, key encodings, task recovery behavior or sync watermarks without fixture-backed migration/compatibility tests.
- Maintain mTLS certificate pinning, pairing PIN expiry/single-use behavior, and loopback-only local administrative routes.
- For each implementation phase, identify the relevant tests first; run `go test ./...`, `go vet ./...`, and `node --test web/app_test.js` as applicable. Run `go test -race` where supported and report any environment limitation rather than treating it as a pass.
- Keep release/package generation separate from ordinary validation when it would overwrite existing artifacts under `dist/`.

## Phase dependencies

The existing improvement plan makes Phase 1 depend on Phase 0; Phases 2–5 may proceed independently after Phase 1; Phase 6 depends on Phases 3, 4 and 5; and Phase 7 depends on Phase 5. Phase 8 provides the contract/drift guardrail across phases. Reconfirm these dependencies during maintainer review and do not start a later phase until its prerequisites are reviewed and merged.
