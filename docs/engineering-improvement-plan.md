# Engineering Improvement Plan

Assessment date: 2026-10-15
Repository: `dbhaigh/mAIstr0`
Scope: Prompt 0 repository assessment; no production behavior changed in this phase.

## Assessment boundaries

- The repository is a Go 1.27 module with an embedded static web dashboard. The dashboard has a Node test file but no separate frontend package manifest.
- No repository `AGENTS.md`, `copilot-instructions.md`, or `.github/instructions/` files were found.
- The working tree was already dirty when assessed. The assessment preserves those changes and runs checks against that working tree; it does not claim a clean-HEAD baseline.
- The checked-out branch is `main` at `8ae1fcf4d717556f675634efdcef48eb3a4a4e06`.
- `improvements.md` describes an older baseline that refers to uncommitted `dispatch_pool.go` files. Those files are absent now; the shared `internal/workpool` implementation is present. No `.flm/memory.json.*.tmp` files are tracked, and `.gitignore` already contains `.flm/*.tmp`.

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

## Baseline and working-tree state

### Uncommitted work

`git diff --stat` reported 11 modified files, 466 insertions and 69 deletions; there were no untracked files. These changes are the preceding per-node engine-inventory implementation, not changes made for this assessment:

- README and engine-status schema: document and define the new optional inventory status fields.
- `internal/engine/engine.go`, `internal/nodeagent/agent.go`, `api_handlers.go`, `components.go`, `engine_component.go`: distinguish unknown, available, empty and failed inventory checks; expose check timestamps/errors; preserve the last successful inventory on failure; retain temporarily unavailable discovered engines for a bounded grace period.
- `internal/nodeagent/components_test.go`: cover inventory changes, empty results, error retention and temporary engine loss.
- `web/static/app.js`, `style.css`, `web/app_test.js`: render per-engine status and grouped model details and test the visible status states.

The Go, schema and UI changes have focused test coverage and passed the checks below, but they remain unreviewed and uncommitted on `main`. The user-visible change has a README update but no corresponding `changelog.md` entry yet.

### Commands run

| Command | Result |
|---|---|
| `go build ./...` | Pass |
| `go vet ./...` | Pass |
| `go test ./...` | Pass; package results were reported as cached in this assessment run |
| `node --test web/app_test.js` | Pass; 13 tests |
| `go test -race ./...` | Could not build: `runtime/cgo: ... cgo.exe: exit status 2`. `CGO_ENABLED=1`, `CC=gcc`, and GCC 16.1 are present; a focused `go test -race -x ./internal/workpool` produced the same cgo failure without additional compiler diagnostics. No toolchain was installed or changed. A race-enabled run on a supported Linux runner remains outstanding. |

The local toolchain is Go 1.27.0, Windows/amd64, Node 24.15.0. CI currently targets Ubuntu and Windows, uses the Go version in `go.mod`, and sets up Node 22. The CI workflow runs contract generation/drift checks, Go tests, vet, application build and dashboard tests. No standalone lint or static-analysis tool beyond `go vet` is configured. The cross-platform `build/build.ps1` and `build/build.sh` packaging scripts were not run because they overwrite existing `dist/` artifacts; `go build ./...` was run instead.

## Status of items in `improvements.md`

| Phase | Assessment | Current evidence and next action |
|---|---|---|
| 0 — Stabilize baseline | **Not complete** | The tree is dirty on `main`, and the race suite did not build on this Windows toolchain. The tracked-temp-file condition is already satisfied: `.gitignore` ignores `.flm/*.tmp`, and no matching temp files are tracked. Review and commit or move the existing inventory work before beginning another implementation phase; rerun race tests on a supported runner. |
| 1 — Characterization and contract tests | **Substantially implemented; coverage audit remains** | There are task/workflow and compatibility fixtures, config tests, scheduler tests, route/schema drift checks and a contract export/generation workflow. Compare each Prompt 1 scenario against actual assertions—particularly API error/stream response shapes and lifecycle/restart edges—before declaring the phase closed. Add only missing behavioral tests, not duplicate suites. |
| 2 — Config validation and normalization | **Implemented** | `NodeConfig` and `OrchestratorConfig` have `Normalize` and `Validate`; constructors call them, and table-driven tests cover URL, listen address, PIN, concurrency, tray, harness and memory-path cases. Maintain compatibility with existing JSON keys and defaults. |
| 3 — Separate domain errors from HTTP | **Implemented in current source** | `internal/apperr` defines error kinds and status mapping; task planning returns typed errors and orchestrator/node handlers use shared mapping helpers. No `taskCreateError` symbol remains. Preserve existing response body shapes when extending errors. |
| 4 — Unify dispatch/worker pool | **Implemented in current source** | `internal/workpool` is used by orchestrator and node-agent; the orchestrator opts out of submitter-context propagation to preserve its task-context lifecycle. The node dispatch gate remains separate and has focused tests. Keep queue and active-slot snapshot semantics stable. |
| 5 — Persistence behind interfaces | **Implemented in current source** | `taskmgr.Store` and `memory.Store` are defined; bbolt adapters, in-memory stores, legacy database fixtures and restart tests exist. The orchestrator also declares a session-store seam. Preserve bucket/key formats and task persistence lock ordering. |
| 6 — Split god objects | **Partial** | Responsibilities are split into components and `orchestrator/server.go` is 369 lines; task manager logic is split by lifecycle, query, recovery and sync. Remaining files exceed the plan's approximate 500-line target, including `nodeagent/agent.go` (527), `nodeagent/api_handlers.go` (936), `orchestrator/dispatcher_component.go` (584), and `orchestrator/router_handlers.go` (480, near the limit). Continue incrementally, task manager first, then node-agent, then orchestrator, without changing exported behavior. |
| 7 — Incremental memory sync | **Implemented in current source; retain regression coverage** | Memory/task records carry origin and sequence data; protocol v2 has per-peer watermarks and cursor paging with legacy/full-snapshot support. Tests cover idempotency, out-of-order convergence, fallback, watermark persistence and page bounds. Keep watermarks from advancing before data is accepted. |
| 8 — Language-neutral artifacts | **Implemented in current source** | OpenAPI, JSON schemas, request/response fixtures, scheduler fixtures and a porting guide exist. `contracts/contracts_test.go` checks route registration and schema/fixture validity; CI checks generated fixture drift. Update contracts with any future wire changes. |

`improvements.md` itself should be reconciled with this assessment: its baseline note references absent dispatch-pool files and its Phase 0 is a stabilization task, whereas the implementation prompt used for this assessment defines Prompt 0 as a repository/architecture assessment. Keep the distinction explicit rather than treating the two Prompt 0 scopes as interchangeable.

## Prioritized follow-up

1. **Close the current baseline:** review the 11-file engine-inventory change, add its changelog entry, and commit it or move it to a named feature branch. Do not overwrite or silently discard the working-tree changes. Keep subsequent phases off `main` and wait for maintainer review as required by the prompt pack.
2. **Complete Phase 1's gap audit:** map each listed lifecycle, API compatibility, configuration, scheduler and recovery behavior to an existing assertion. Fill only demonstrated gaps with deterministic tests and normalized fixtures.
3. **Run race validation on a supported environment:** add no local dependencies for this assessment. Use a Linux CI runner or another supported cgo toolchain and record the exact result before concurrency-focused refactors.
4. **Continue Phase 6 after prerequisite review:** keep the decomposition in small, separately reviewable changes—task manager, node agent, then orchestrator. Prioritize lock ownership, explicit dependencies and focused tests; avoid file splitting without a clear responsibility boundary.
5. **Treat Phases 2–5, 7 and 8 as maintenance unless a fresh audit finds a specific missing acceptance criterion.** Their current implementations and tests are already present; do not redo them just to match the old plan wording.

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
