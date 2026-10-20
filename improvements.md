# mAIstr0 Improvement Plan

This file is the phase prompt pack and acceptance checklist. For a dated
assessment of implementation and remaining work, see
[docs/engineering-improvement-plan.md](./docs/engineering-improvement-plan.md).
The current status below is a working-tree snapshot, not a claim that all
changes have been reviewed or merged.

## Current implementation update — 2026-10-19

- Phases 2–5 and 7–8 are implemented in the current source. Phase 1
  characterization and contract coverage is complete in the current worktree;
  review and merge its fixtures with the tests.
- Phase 6 is implemented for the stated decomposition and size goals: the
  production Go files in `internal/orchestrator`, `internal/nodeagent`, and
  `internal/taskmgr` are all below 500 lines, with responsibilities owned by
  focused components and lock order documented. Further splits should follow a
  concrete review finding, not the line-count target alone.
- Phase 0 remains open until the current worktree is reviewed and merged. The
  Linux race suite has passed previously; native Windows still reports the
  documented ThreadSanitizer shadow-memory allocation limitation.
- Incremental sync now supports configurable transfer limits and timeouts with
  the established 64 MiB and 10-minute defaults. Metrics, service examples,
  normalized config printing, and dashboard connection status are documented
  and covered by tests; see the README and changelog.
- bbolt snapshot merges now batch experience/fact and dialogue/watermark updates
  into transactions instead of writing each record separately, and unchanged
  dialogue records are not rewritten.

## Ground rules for every phase

- Each phase goes into its own branch or PR, and the build stays green between phases.
- Make **no behavior changes** unless the phase explicitly says so. Keep JSON wire formats, API routes,
  on-disk bbolt bucket names, and config keys compatible.
- Every phase must pass:
  ```powershell
  go build ./...
  go vet ./...
  go test ./...
  node --test web/app_test.js
  ```
- Update `README.md` and `changelog.md` when user-visible behavior or configuration changes.
- Don't change the tray app or dashboard UI unless the phase requires it.

## Phase overview

| # | Phase | Depends on | Risk | Outcome | Current status |
|---|-------|-----------|------|---------|----------------|
| 0 | Stabilize baseline | none | Low | Clean, reviewed, green starting point | **Partial** — race caveats are documented; the current worktree is not clean |
| 1 | Characterization and contract tests | 0 | Low | Behavior locked down before refactoring | **Coverage complete in current worktree** — review and merge the test/golden updates |
| 2 | Config validation and normalization | 1 | Low | One source of truth for config rules | **Implemented** |
| 3 | Separate domain errors from HTTP | 1 | Medium | Domain services don't depend on HTTP status codes | **Implemented** |
| 4 | Unify dispatch/worker pool | 1 | Medium | One tested concurrency primitive | **Implemented** |
| 5 | Persistence behind interfaces | 1 | Medium | bbolt isolated behind repositories | **Implemented** |
| 6 | Split god objects | 3, 4, 5 | High | `Server`, `Agent`, and `Manager` decomposed | **Implemented in the current worktree** — production files in all three target packages are below 500 lines; component locks and task-manager order are documented |
| 7 | Incremental memory sync | 5 | Medium | Sync supports watermarks with full-snapshot fallback | **Implemented** |
| 8 | Language-neutral contracts | 1–7 | Medium | OpenAPI, schemas, fixtures, and porting guide | **Implemented** |

You can run phases 2, 3, 4, and 5 in parallel after Phase 1. Don't start Phase 6 until 3, 4, and 5 are merged.

Phase 8 is a continuing compatibility guardrail, not a one-time final step:
update it whenever a route, schema, or externally visible payload changes.
Phase 0 is the stabilization phase in this prompt pack; it is distinct from
the repository assessment documented in
[docs/engineering-improvement-plan.md](./docs/engineering-improvement-plan.md).

---

## Phase 0: Stabilize baseline

**Goal:** Get a clean, reviewed, committed starting point.

**Acceptance criteria**
- The working tree is clean, or the remaining work in progress is on a clearly named branch.
- The new `internal/orchestrator/dispatch_pool.go`, `internal/orchestrator/dispatch_pool_test.go`, and
  `internal/nodeagent/dispatch_pool.go` are either committed with tests or reverted.
- `go test -race ./...` passes on at least one platform where cgo/race is available, or the gap is documented.
- `git ls-files -- '*.tmp'` is empty, and `.gitignore` ignores `.flm/*.tmp`.

### Prompt 0
```text
You are working in the Go repo at C:\Users\ahaigh\source\repos\mAIstr0.

Goal: stabilize the baseline before a refactoring program starts. Make no behavior changes.

1. Run `git status` and `git diff --stat`. Summarize every uncommitted change by package and say whether it looks
   complete. Pay particular attention to the untracked internal/orchestrator/dispatch_pool.go,
   internal/orchestrator/dispatch_pool_test.go, and internal/nodeagent/dispatch_pool.go.
2. Run `go build ./...`, `go vet ./...`, `go test ./...`, and `node --test web/app_test.js`. Report the results.
3. Try `go test -race ./...`. If the race detector is unavailable (no cgo/gcc), say so. Don't install toolchains
   without asking me.
4. Tracked files `.flm/memory.json.*.tmp` are temp artifacts. Remove them from the index with `git rm --cached`
   and add an ignore rule for `.flm/*.tmp` to .gitignore.
5. Don't commit. Show me a proposed commit grouping, with one logical commit per feature, and wait for approval.

Done when: all checks are green, the temp files are untracked, and you've given me a commit plan.
```

---

## Phase 1: Characterization and contract tests

**Goal:** Lock down current externally visible behavior so Phases 2–7 can prove they preserved it

**Scope**
- Task lifecycle: create (heuristic, explicit subtasks, explicit graph, `plan_mode:"auto"`), dependency wait,
  cancel, approve, `require_review`, failure, and restart recovery. Running tasks become failed and resumable.
- Scheduler: `AssignWithExperience` ordering, `ErrNoHealthyNodes`, `UnassignableSubtasksError`, and telemetry freshness.
- Graph: validation, cycles, missing dependencies, and topological order.
- HTTP API contracts: status codes and JSON shapes for `/api/tasks*`, `/api/status`, `/v1/chat/completions`
  (with and without streaming), `/api/chat`, `/api/generate`, `/api/tags`, and `/v1/messages`.
- Config: defaults and overlay behavior for both config types.

**Acceptance criteria**
- New tests live alongside existing ones. Reuse `internal/orchestrator/api_contracts_test.go` patterns.
- Golden JSON files go under `testdata/contracts/` in each package. Timestamps and IDs are normalized.
- Tests don't depend on real LLM engines. Use the simulated engine and `httptest`.
- No production code changes, except tiny test seams that are justified in the PR description.

### Prompt 1
```text
You are working in the Go repo at C:\Users\ahaigh\source\repos\mAIstr0. All tests currently pass.

Goal: add characterization and contract tests that lock down current behavior. Don't change production behavior.

First read README.md (the API and Development sections), internal/orchestrator/api_contracts_test.go,
internal/orchestrator/task_planning.go, internal/scheduler/{graph.go,assign.go}, internal/taskmgr/taskmgr.go, and
internal/engine/simulated.go.

Then add tests covering:
1. Task lifecycle through the orchestrator HTTP API using httptest and simulated engines:
   - POST /api/tasks with a description only (heuristic), explicit subtasks with depends_on, an explicit graph,
     an invalid plan (cycle or missing dependency, expect 400), and no healthy nodes (expect 503).
   - Dependent subtasks receive prerequisite outputs as input.
   - require_review adds a reviewer subtask; require_approval pauses at awaiting_approval and
     POST /api/tasks/{id}/approve resumes it.
   - POST /api/tasks/{id}/cancel.
   - Restart recovery: tasks that were pending, running, or synthesizing become failed with Resumable=true.
2. Scheduler: deterministic assignment for a fixed node and model fixture, ErrNoHealthyNodes,
   UnassignableSubtasksError listing the right IDs, and stale telemetry (>30s) ignored for utilization penalties.
3. Compatibility APIs: response JSON shape and status for /v1/chat/completions (stream and non-stream),
   /api/chat, /api/generate, /api/tags, and /v1/messages. For streams, assert the SSE/NDJSON framing and the
   terminal chunk.
4. Config: defaults, a partial JSON overlay, a missing file (not an error), and malformed JSON (error).

Store expected JSON as golden files under each package's testdata/contracts/. Normalize volatile fields (IDs,
timestamps, durations) with a small helper and support an -update flag to regenerate them.

Constraints: no real network or engines, no sleeps longer than about 100ms (use channels or polling helpers), and
tests must run in parallel safely with temp dirs for any bbolt files.

Validate with go vet ./... and go test ./... and report coverage deltas from `go test -cover` for orchestrator,
scheduler, taskmgr, and config. If a test exposes an actual bug, don't fix it. Add t.Skip with a clear
"BUG:" message, then list each bug in your summary.
```

---

## Phase 2: Config validation and normalization

**Goal:** Put all config defaulting and validation rules in `internal/config`, and make startup fail fast with
clear messages.

**Acceptance criteria**
- `NodeConfig` and `OrchestratorConfig` each have `Normalize()` for defaults and derived values, plus
  `Validate() error`, which returns a joined error that lists every problem.
- Validation covers concurrency (`<=0` is normalized to the default, and negative values are rejected only if they
  were explicitly set), harness (`pi` or `deepseek`), tray mode (`taskbar` or `hidden`), listen addresses (parseable
  `host:port` or `:port`), URLs (HTTPS for orchestrator and advertise addresses, HTTP or HTTPS for engines), pairing
  PIN format (six digits), and the rule that orchestrator configs need discovery enabled.
- Constructors in `orchestrator` and `nodeagent` call `Normalize`/`Validate` and stop doing ad hoc defaulting.
- `cmd/maistr0/main.go` reports validation errors and exits with a non-zero status.
- Table-driven tests cover every rule.

### Prompt 2
```text
You are working in the Go repo at C:\Users\ahaigh\source\repos\mAIstr0. Phase 1 contract tests exist and pass.

Goal: centralize configuration normalization and validation in internal/config.

1. Read internal/config/config.go, cmd/maistr0/main.go (loadNodeConfig, loadOrchestratorConfig, and the role
   switch), and the constructors NewWithConfig in internal/orchestrator/server.go and
   newAgentWithEngineDetector in internal/nodeagent/agent.go. List every place config values are defaulted or
   checked outside internal/config.
2. Add these methods to both NodeConfig and OrchestratorConfig:
   - Normalize(): apply defaults. MaxConcurrentTasks and MaxConcurrentDispatches <= 0 become the defaults,
     harness is lowercased and trimmed with an empty value becoming "pi", and the default tray mode is applied.
     AdvertiseAddr derivation needs the network, so keep it in nodeagent but call it from a single helper.
   - Validate() error: use errors.Join to report all violations. Rules: harness is in {pi, deepseek}, tray_mode is
     in {taskbar, hidden}, listen_addr parses with net.SplitHostPort, orchestrator_addr and advertise_addr are empty
     or https:// URLs, engines[].name is a supported engine with a valid http(s) url, pairing_pin is empty or exactly
     six digits, and memory_path's parent directory is creatable if set.
3. Replace the ad hoc defaulting in constructors and main.go with Normalize plus Validate. main.go should print
   every validation error and exit with code 2. Keep the existing log.Fatal messages for non-validation startup
   failures.
4. Add table-driven tests in internal/config/config_test.go for every rule, including errors reported together.
5. Update README.md to document the validation rules and add a changelog.md entry.

Constraints: configs that are valid today must keep loading. Don't rename JSON keys. Phase 1 golden tests must
still pass unchanged.

Validate with go vet ./... and go test ./....
```

---

## Phase 3: Separate domain errors from HTTP

**Goal:** Domain and service functions return typed errors, and only HTTP handlers map them to status codes. This
removes `taskCreateError{status:…}` from domain logic.

**Acceptance criteria**
- A small `internal/apperr` package or sentinel errors define the categories: `ErrInvalidInput`, `ErrUnavailable`,
  `ErrUpstream`, `ErrNotFound`, `ErrConflict`, and `ErrInternal`. Wrap them with `%w`.
- `createTask`, `planSubtasks`, and `executeWorkflow` don't return HTTP status codes.
- One `writeError(w, err)` maps categories to 400, 503, 502, 404, 409, and 500.
- Phase 1 golden tests show the same status codes and bodies.

### Prompt 3
```text
You are working in the Go repo at C:\Users\ahaigh\source\repos\mAIstr0. Phase 1 contract tests exist and pass.

Goal: decouple orchestration logic from HTTP status codes without changing API behavior.

1. Read internal/orchestrator/task_planning.go, including createTask, taskCreateError, planSubtasks, and
   executeWorkflow. Also read internal/orchestrator/api_handlers.go, compat_api.go, memory_api.go, and the
   nodeagent api_handlers.go. List every place a domain function chooses an HTTP status.
2. Create internal/apperr with error categories: InvalidInput, NotFound, Conflict, Unavailable,
   Upstream (a bad response or bad plan from a model or node), and Internal. Use a Kind type with
   errors.Is and errors.As support plus constructors like apperr.Invalid(err) that wrap with %w.
3. Change createTask and the other domain functions to return apperr-wrapped errors and remove taskCreateError.
   Keep the current mapping:
   - an invalid request or plan is 400
   - an invalid model-produced plan is 502
   - no healthy nodes or an unassignable subtask is 503
   - a persistence failure is 500
4. Add one writeError(w, err) helper in orchestrator and use it in every handler that currently maps errors by
   hand. Also do this in nodeagent, either with a shared helper in apperr or with one small duplicate helper.
5. Phase 1 golden tests must pass unchanged. Add unit tests for the apperr status mapping.

Constraints: preserve every JSON error body shape exactly. Don't refactor unrelated code.

Validate with go vet ./... and go test ./....
```

---

## Phase 4: Unify the dispatch/worker pool

**Goal:** Replace the two hand-rolled pools in `orchestrator/dispatch_pool.go` and `nodeagent/dispatch_pool.go`
with one generic, well-tested primitive.

**Acceptance criteria**
- `internal/workpool` provides `Pool[J]` with these behaviors:
  - a configurable worker count and queue capacity
  - `Submit(ctx, job)` honors both the job context and pool shutdown
  - per-job contexts are canceled when the pool shuts down
  - `Close()` is idempotent and waits for workers
  - `QueueDepth()` and `Active()` report metrics
- Both call sites use it. `nodeDispatchGate` stays in orchestrator because it's node-capacity logic, but gets its
  own tests.
- Tests cover shutdown during submit, job context cancellation, a full queue with backpressure, metric accuracy,
  and no goroutine leaks (`runtime.NumGoroutine` before and after, or `goleak` if you approve the dependency).
- Tests pass with `-race` where available.

### Prompt 4
```text
You are working in the Go repo at C:\Users\ahaigh\source\repos\mAIstr0. Phase 1 contract tests exist and pass.

Goal: replace the duplicated worker pools with one generic primitive.

1. Read internal/orchestrator/dispatch_pool.go (dispatchPool and nodeDispatchGate),
   internal/orchestrator/dispatch_pool_test.go, internal/nodeagent/dispatch_pool.go (peerDispatchPool), and how
   each one is constructed, submitted to, and closed in server.go and agent.go. Document the semantic differences.
   For example, the nodeagent pool derives a per-job context from the submitter's context, but the orchestrator
   pool doesn't.
2. Create internal/workpool with:
     type Pool[J any] struct{...}
     func New[J any](workers, queueCap int, handle func(ctx context.Context, job J)) *Pool[J]
     func (p *Pool[J]) Submit(ctx context.Context, job J) error // ErrClosed, or ctx.Err()
     func (p *Pool[J]) Close()                                   // idempotent; cancels and waits
     func (p *Pool[J]) QueueDepth() int
     func (p *Pool[J]) Active() int
   Each handled job gets a context canceled by both the submitter's context and pool shutdown. If keeping the old
   orchestrator behavior requires it, make submitter-context propagation an option and explain why in a comment.
3. Migrate both call sites, then delete the old pool types. Keep nodeDispatchGate in orchestrator, give it its own
   file, and add tests for its capacity math (limit, available, reportedActive, and external occupancy).
4. Write thorough tests for workpool: concurrent submit and close, a full queue blocking until a slot opens or the
   context is canceled, metrics, and no goroutine leaks after Close.
5. Run go test -race ./internal/workpool/... ./internal/orchestrator/... ./internal/nodeagent/... if the race
   detector is available.

Constraints: the snapshot fields queue depth and active slots in the SSE cluster snapshot must keep their meaning.
Don't change max_concurrent_dispatches or max_concurrent_tasks semantics.

Validate with go vet ./... and go test ./....
```

---

## Phase 5: Persistence behind interfaces

**Goal:** Isolate bbolt so domain packages depend on repository interfaces, with bbolt adapters and in-memory fakes.

**Acceptance criteria**
- The interfaces are `TaskStore` (in taskmgr), `MemoryStore` (experiences, facts, performance, dialogues,
  snapshot, and merge), and `SessionStore` (agent sessions).
- The existing bbolt code moves into adapters such as `taskmgr/boltstore.go` and `memory/boltstore.go`. The on-disk
  format is unchanged, with the same bucket names and keys.
- In-memory implementations are used in unit tests, so fewer tests need temp dirs.
- A compatibility test opens a database produced by the old code and reads it correctly. A fixture DB is committed
  under `testdata`.

### Prompt 5
```text
You are working in the Go repo at C:\Users\ahaigh\source\repos\mAIstr0. Phase 1 contract tests exist and pass.

Goal: put persistence behind interfaces without changing on-disk formats.

1. Read internal/taskmgr/taskmgr.go and checkpoint.go, internal/memory/*.go, and where the orchestrator persists
   and restores agent sessions (restoreAgentSessions and persistAgentSession in internal/orchestrator/server.go,
   plus memory.AgentSessions). List every bbolt touchpoint with its bucket name and key format.
2. Before changing anything, write a small test helper that builds bbolt fixture DBs with the current code.
   Commit one task DB and one memory DB under testdata/, with tasks in each status, checkpoints, facts,
   experiences, performance rows, dialogues, and agent sessions.
3. Define these interfaces in their owning packages:
   - taskmgr.Store: Load() ([]*Task, error), Save(*Task) error, SaveCheckpoint(...), Close() error,
     and whatever else Manager actually needs. Use the narrowest set possible.
   - memory: split the current Store into an interface and a boltStore implementation. Keep memory.Open
     returning the bolt-backed implementation so callers don't change.
   - agent session persistence: a small SessionStore interface used by orchestrator.
4. Move the bbolt code into adapters with the same bucket names, key formats, and JSON encoding.
   Add in-memory implementations for tests.
5. Add tests that open the committed fixture DBs with the new adapters and assert the full contents round-trip,
   including the restart-recovery rules: running work becomes failed and resumable.
6. Switch pure-logic unit tests to the in-memory stores where that simplifies them.

Constraints: no schema or format changes and no new dependencies. Manager's locking semantics,
including persistGate and per-task persistMu, must be preserved. Note any races you notice instead of
silently changing them.

Validate with go vet ./... and go test ./....
```

---

## Phase 6: Split the god objects

**Goal:** Break `orchestrator.Server` (about 1,444 lines), `nodeagent.Agent` (about 1,520 lines), and
`taskmgr.Manager` (about 987 lines) into cohesive components with explicit dependencies.

**Target decomposition**

- **orchestrator**
  - `harness.go`: harness registry, the per-session harness map, interrupts, and session persistence
  - `dispatcher.go`: workpool, node gates, `executeSubtask`, retry, `recordDispatch`, and dispatch metrics
  - `nodeclient.go`: generate, stream, and capacity HTTP calls to nodes
  - `election.go`: leader and discovery announcements
  - `pairing.go`: identity, trust, and PIN handling, if it isn't already separate
  - `Server` becomes a thin composition root plus route registration
- **nodeagent**
  - `engines.go`: detection, rescanning, prioritization, and the model list
  - `telemetry.go`: hardware histories and snapshot publishing
  - `orchestrator_link.go`: pairing, registration, heartbeat, and the event stream
  - `executor.go`: execution slots and running dispatched subtasks
  - `peers.go`: peer dispatch, most capable and fastest node selection, and proxying
  - `dialogues.go`: dialogue sessions and cancels
- **taskmgr**
  - `lifecycle.go`: create, start, complete, cancel, and approve
  - `deps.go`: `WaitForDependencies` and `InputForSubtask`
  - `sync.go`: `MergeMissing`, `MergeFromReplica`, `ReplaceFromSync`, and `syncTasks`
  - `recovery.go`: load and restart recovery

**Acceptance criteria**
- No file over about 500 lines in these packages, excluding tests.
- Each component has a constructor that takes its dependencies explicitly, with no reach-back into `*Server` or
  `*Agent` except through narrow interfaces.
- Each struct documents what its mutexes protect.
- All Phase 1 golden tests pass unchanged, and no exported API changes unless it's documented.
- Do this as several PRs, one package per PR. Do taskmgr first, then nodeagent, then orchestrator.

### Prompt 6a: taskmgr
```text
You are working in the Go repo at C:\Users\ahaigh\source\repos\mAIstr0. Phases 1–5 are merged. Contract tests,
apperr, workpool, and persistence interfaces all exist.

Goal: decompose internal/taskmgr/taskmgr.go into cohesive files and components with no behavior change.

1. Read the whole package. Produce a responsibility map that lists each method, its group (lifecycle,
   dependency wait and input, replica sync, recovery and load, persistence, or query), and which locks it takes:
   mu, persistGate, and per-task persistMu.
2. Split it into files by responsibility: lifecycle.go, deps.go, sync.go, recovery.go, query.go, and model.go for
   types and Status. If syncTasks is large and replica merging has distinct rules, extract a pure function that
   merges two task slices. Unit-test it directly with table tests.
3. Document the lock hierarchy in a comment on Manager, including the acquisition order, and make sure all code
   follows it.
4. Keep the exported API identical. If a method is unused outside the package, list it, but don't remove it yet.

Validate with go vet ./..., go test ./..., and, where available, go test -race ./internal/taskmgr/....
```

### Prompt 6b: nodeagent
```text
You are working in the Go repo at C:\Users\ahaigh\source\repos\mAIstr0. Phases 1–5 and 6a are merged.

Goal: decompose nodeagent.Agent in internal/nodeagent/agent.go, which is about 1,500 lines, into focused
components with no behavior change.

1. Read agent.go, api_handlers.go, memory_sync.go, peerchat.go, task_sync.go, and dispatch_pool.go if it
   still exists. Group every Agent field and method into one of these areas: engines/models, telemetry,
   orchestrator link (pairing, registration, heartbeat, and the event stream), executor (execution slots and
   active tasks), peers (peer dispatch, mostCapableNode, fastestNode, and proxying), dialogues, and identity.
   Show me the mapping, including which mutex guards which fields.
2. Extract each area into its own struct in its own file. Give each one a constructor that takes explicit
   dependencies. Agent should keep pointers to these components and delegate to them. Cross-component calls
   must go through small interfaces declared where they're used.
3. Keep HTTP handlers in api_handlers.go, but make them call the components.
4. Each mutex must move with the fields it guards. Don't merge or split locks unless you can prove it's safe
   and explain why.
5. Add focused unit tests for each extracted component where coverage was previously indirect, especially
   engine prioritization, capacity reporting, and peer selection.

Constraints: no exported API changes, Phase 1 golden tests pass unchanged, and non-test files in the package stay
under about 500 lines.

Validate with go vet ./... and go test ./....
```

### Prompt 6c: orchestrator
```text
You are working in the Go repo at C:\Users\ahaigh\source\repos\mAIstr0. Phases 1–5, 6a, and 6b are merged.

Goal: decompose orchestrator.Server in internal/orchestrator/server.go into focused components with no behavior
change.

1. Read server.go, api_handlers.go, compat_api.go, memory_api.go, task_planning.go, task_synthesis.go,
   task_history_sync.go, pi_local_provider.go, and the dispatch-gate code. Map every Server field and method to one
   of these areas: harness/session management, dispatcher (pool, node gates, executeSubtask, retryAssignment,
   recordDispatch, and metrics), node client (callNodeGenerate*, streamNodeGenerate, and capacity probes),
   election/discovery, identity/pairing, task planning, synthesis, history sync, and HTTP routing.
2. Extract the harness manager, dispatcher, node client, and election into their own structs and files with
   explicit constructor dependencies. Server becomes the composition root that wires them together and registers
   routes.
3. Task planning (createTask and planSubtasks) should depend on interfaces such as Scheduler, TaskStore,
   Dispatcher, and PlannerModel, not on *Server.
4. Keep the OpenAI-compatible stream writer and responseCapture in compat_api.go or a new streaming.go.

Constraints: no exported API changes. Phase 1 golden tests, including the streaming contracts, must pass
unchanged. Non-test files in the package must stay under about 500 lines.

Validate with go vet ./..., go test ./..., and node --test web/app_test.js.
```

---

## Phase 7: Incremental memory sync

**Goal:** Replace full `Snapshot` and `MergeSnapshot` exchange with incremental sync. Keep the full snapshot as a
fallback and a compatibility path for mixed-version clusters.

**Acceptance criteria**
- Every record type has a monotonic per-origin sequence or `UpdatedAt` watermark.
- A new `SnapshotSince(watermarks)` API returns only newer records, and peers keep a per-peer watermark.
- The protocol is versioned. A node that doesn't support incremental sync gets a full snapshot.
- Merges remain idempotent. Existing fingerprint deduplication is preserved.
- Tests cover idempotency, out-of-order delivery, mixed versions, and watermark persistence across restart.
- README sync docs and the changelog are updated.

### Prompt 7
```text
You are working in the Go repo at C:\Users\ahaigh\source\repos\mAIstr0. Phases 1–6 are merged. Memory persistence
sits behind interfaces.

Goal: add incremental memory and task-history synchronization while keeping full-snapshot sync as a fallback.

1. Read internal/memory/memory.go, including Snapshot, MergeSnapshot, the merge* functions, and fingerprinting;
   internal/nodeagent/memory_sync.go and task_sync.go; internal/orchestrator/task_history_sync.go; and
   memory_api.go. Document the current sync flows: who pulls or pushes, how often, and payload sizes.
2. Design and implement:
   - Per-record origin and sequence metadata. Add it in a backward-compatible way: missing fields mean legacy, and
     legacy records are backfilled on open.
   - Store.SnapshotSince(map[origin]uint64) and a Watermarks() accessor.
   - A per-peer watermark stored in the meta bucket.
   - A sync protocol version in the request or response. Mixed-version peers fall back to the full Snapshot.
3. Keep merges idempotent, keep the existing fact last-writer-wins tie rules, and keep experience fingerprint
   deduplication.
4. Tests: replaying the same batch twice changes nothing; out-of-order batches converge; old-to-new and new-to-old
   peers sync correctly; and watermarks survive restart. Add a benchmark comparing full and incremental payload
   sizes for 10k experiences.
5. Update README.md (the memory sync section) and changelog.md.

Constraints: legacy databases must open without manual migration. Don't change existing JSON field names. Only add
optional fields.

Validate with go vet ./... and go test ./....
```

---

## Phase 8:

**Goal:** Produce language-neutral artifacts

**Acceptance criteria**
- `contracts/` at the repo root holds:
  - an OpenAPI 3.1 spec for every HTTP route on orchestrator and node, generated or hand-written and checked by a
    Go test against the real router
  - JSON Schemas for configs, Task, Subtask, TaskGraph, Assignment, ScoreFactors, NodeStatus, Announcement, and
    memory records
  - Golden request/response fixtures exported from Phase 1, with input, output, and normalization rules
  - Scheduler scoring fixtures: node, model, and subtask inputs with expected assignments and score factors

### Prompt 8
```text
You are working in the Go repo at C:\Users\ahaigh\source\repos\mAIstr0. Phases 1–7 are merged.

Goal: produce language-neutral contracts and a porting guide

1. Create contracts/openapi.yaml (OpenAPI 3.1) covering every route registered by the orchestrator and node
   HTTP servers. Find routes by reading the mux registration code. Add a Go test that enumerates the registered
   routes and fails if any route is missing from the spec.
2. Create contracts/schemas/*.json (JSON Schema 2020-12) for NodeConfig, OrchestratorConfig, Task,
   SubtaskResult, Subtask, TaskGraph, Assignment, ScoreFactors, NodeStatus, discovery.Announcement, and memory
   records (Experience, Fact, Performance, DialogueRecord, and Snapshot). Add a Go test that marshals
   representative values and validates them against the schemas. If a schema library is needed, propose it before
   adding it.
3. Export the Phase 1 golden fixtures into contracts/fixtures/ as JSON. Each case should include the request, the
   expected response, and the normalization rules. Add scheduler fixtures with node, model, and subtask inputs and
   the expected assignments and score factors, generated from the real Go scheduler with a go:generate tool.
4. Add a CI step that runs the contract drift tests.

Validate with go vet ./..., go test ./..., and node --test web/app_test.js.
```

---

## Tips for running these prompts

- Run one prompt per session or PR. Paste the prompt as-is. Each one is self-contained.
- After each phase, run a review with "Review the diff on this branch for behavior changes, race conditions,
  and broken on-disk or wire compatibility."
- If a phase's agent reports "BUG:" skips from Phase 1, triage them before Phase 6. Phase 6 shouldn't fix bugs.
- Keep this plan file updated as phases land.
