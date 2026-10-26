# Concurrency, backpressure, and resource ownership

Phase 5 reviews production asynchronous ownership without replacing the
architecture. Public success payloads, persistence encodings, queue sizes, and
the scope of node execution limits are preserved. Saturated dispatch admission
now reports an explicit error instead of blocking an HTTP submission indefinitely.

## Evidence and reproduction

The standard-library [AST census tool](../build/audit-concurrency.go) indexes
all non-test Go sources, including platform variants. Regenerate the
[machine-readable census](./concurrency-census.json) with:

```powershell
go run .\build\audit-concurrency.go -root . -output .\docs\concurrency-census.json
```

The current census covers 277 files and 2,505 syntactic sites, including 40
direct goroutine launches and 29 timer/wait sites. It includes locks,
channels, contexts, defers, lifecycle calls, and shared asynchronous boundaries.
Sites can overlap: a defer is not proof of cleanup, a named lock is not a
distinct mutex, and a call named `Run` is not necessarily asynchronous.
Reachability and semantics come from the owners and tests below, not counts.
JavaScript timers and shell subprocesses require separate source inspection.

## Shared primitives and admission policy

### Worker pools

[workpool](../internal/workpool/pool.go) owns a fixed number of workers and a
fixed-size channel. `Submit` blocks for queue space, closure, or caller
cancellation; `TrySubmit` rejects immediately with `ErrFull`. Closed pools
return `ErrClosed`. Cancellation is checked at worker entry, including when
the queued submitting context was already canceled. The optional detached
policy deliberately separates accepted work from its submitting request.

Close is concurrent-safe and idempotent: cancel, join workers, then fence
submitters and drain queued references. The jobs channel is not closed while
submitters can access it. Accepted work racing Close can be abandoned;
successfully submitting is not a durable execution guarantee. Active handlers
must cooperate with cancellation. Closing the pool from its own handler is
not supported. Worker panics remain process-fatal: generic recovery would
silently change execution semantics.

The submission read lock does not span handler execution. Shutdown does not
hold the exclusive submission lock while joining workers; this permits a
worker attempting another submission to observe cancellation rather than
deadlocking.

Evidence: [ownership tests](../internal/workpool/ownership_test.go) cover
nonblocking saturation, canceled queued entry, concurrent Close/TrySubmit,
reference draining, and process-fatal panic in an isolated child process.

### Task acceptance and overload

- Orchestrator dispatch: configured dispatch workers; queue is 64 times the
  worker count. Accepted jobs use canonical task contexts and outlive the
  creating HTTP request. Task cancellation or owner shutdown stops them.
- Node peer dispatch: node worker count; queue is 64 times that count. Accepted
  jobs are also task-owned, not handler-owned.
- Request-owned synchronous workflows cancel their task if creation/admission
  fails or their subsequent wait fails.
- The parallel inference tool uses the existing pool with no queued jobs.
  Its worker budget is bounded by assignment count and the sum of advertised
  node concurrency, with at least one slot for a node lacking a positive
  advertisement. Submission waits are caller-cancellable; results preserve
  input order and include errors for unsubmitted inputs.

Creation, peer submission, and resume use `TrySubmit`. Overload or closure
returns HTTP 503 in the existing plain-text error format, with the persisted
task ID. Caller cancellation detected before persistence returns 409 and
creates no record. These checks cannot make cancellation and persistence
atomic; cancellation can race immediately after the check.

**Admission is per subtask, not transactional per workflow.** Some accepted
subtasks may run even when the creating request receives 503. Rejected
subtasks are persisted as failed; accepted work is not silently rolled back.
Inspect the identified task and wait for running work before resuming. Do not
blindly create a second task containing side effects. Dispatch remains
at-least-once, not exactly-once.

The existing scheduler already topologically orders assignments. A
one-worker integration test verifies progress when the submitted plan lists
a dependent before its prerequisite. No extra ordering implementation was
retained. Fixtures must advertise available capacity; healthy alone does not
mean an execution slot is available.

Evidence: [orchestrator admission tests](../internal/orchestrator/admission_ownership_test.go),
[peer admission tests](../internal/nodeagent/admission_ownership_test.go),
[parallel-tool tests](../internal/deepseek/ownership_test.go), and the
[tested overload contract](../contracts/fixtures/concurrency/task-create-overload.json).
See [porting notes](../contracts/PORTING.md#dispatch-admission-and-request-lifetime).

### Background groups and bounded notifications

[background.Group](../internal/background/background.go) owns a finite set of
service loops, propagates cancellation, fences starts after Stop, and joins
active work. `Run` provides synchronous joined ownership for callbacks.
An owner must not Stop its group from inside its own active worker/callback.
Queues elsewhere are not automatically covered by a background group.

The concrete service-loop owners are:

- [Node runtime](../internal/nodeagent/agent_server.go): beacon, hardware usage,
  orchestrator resolution/registration, model refresh, peer polling, upstream
  SSE subscription, task-history synchronization, peer-memory synchronization,
  dashboard broadcast, orchestrator-memory synchronization, and heartbeat.
  The orchestrator-link group owns cancellation and joining of these serial
  loops. The normal single-`Run` lifecycle starts one instance of each loop; periodic work
  uses the configured/fixed interval or adaptive synchronization wait, not a
  growing queue of missed ticks. An individual pass can still fan out over
  the current peer registry as documented below.
- [Orchestrator runtime](../internal/orchestrator/server.go): health sweep,
  leader logging, task-history synchronization, and discovery synchronization.
  The server group owns those loops and the lazily started snapshot publisher;
  the election component owns its beacon. Shutdown stops publishers/tickers
  and cancels their network passes before joining.
- [Agent loop](../internal/agent/loop.go): caller/turn-owned model and tool
  iterations, with its step bound and cancellable iteration timer.
  [DeepSeek](../internal/deepseek/harness_loop.go) and its
  [adapter](../internal/agent/deepseekadapter/deepseek.go) each own an event
  relay when streaming. Relay buffers match the downstream channel capacity;
  the sender closes its own channel and joins its relay, and sends observe
  cancellation. Pi invokes the same shared loop rather than another job queue.

The census also lists `nodeDispatchGate.acquire`, which is retained for
characterization tests but has no production callers. Its timer is not an
additional live production admission mechanism. Platform-tagged source and
syntactically indexed calls must not be mistaken for simultaneously active
runtime owners.

Hub subscriptions have eight queued snapshots; the dirty signal is
capacity-one. Intermediate full snapshots are intentionally replaceable.
The publisher skips expensive snapshot construction with zero subscribers
and coalesces dirty updates. Agent progress events are also best effort
(bounded send wait, 50 ms), unlike durable task results. These are explicit
loss policies for replaceable notifications, not silent loss of task jobs.

## Capacity, routing, and request lifetime

- Orchestrator global/per-node dispatch gates own reservations. Success,
  failure, retry, cancellation, and hedge losers release leases through
  existing cleanup/once paths. Planning may choose a full node; execution
  waits on bounded capacity, consistent with the selected queued-planning
  policy. GPU offload compatibility is unchanged.
- Node execution and pipeline/shard gates reject already-canceled admission,
  including cancellation racing a slot grant; rollback does not leak a token
  or create active state. Per-request pipeline state is released by the owner.
- Node `max_concurrent_tasks` still applies to `/execute` and stage requests,
  **not** direct generate/chat/proxy/dialogue routes. Extending that scope
  would be a separate behavioral decision.
- Hedges have at most the configured number of slots (capped at four);
  result channels are bounded. Loser cancellation releases late reservations.
  Retry is disallowed after streaming response bytes are committed.
- Router first-token and idle-timeout timers cancel the upstream request;
  request/body cleanup stops them. Their cancellation callbacks are not engine
  workers and do not add another task queue.
- Request contexts propagate through capacity waits, task execution,
  planning/dispatch HTTP calls, streaming, and synchronous synthesis.
  Accepted asynchronous task execution instead uses the canonical task context.

Capacity waiters are caller-owned blocked operations, not an unbounded
internal job channel. This does **not** impose an aggregate HTTP connection
or waiter limit; deployments still need ingress connection/request limits.

Evidence: [node admission tests](../internal/nodeagent/execution_ownership_test.go),
[pipeline tests](../internal/orchestrator/pipeline_ownership_test.go),
existing reservation/retry/cancellation suites, and
[distributed guarantees](./distributed-guarantees.md).

## Streaming, transport, and connection owners

- An agent turn owns its backend, event channel, cancellation, and session
  persistence. A failed SSE write cancels the turn, stops further writes, and
  drains events to join the backend before cleanup. Client cancellation and
  upstream errors use the same turn lifetime. An unencodable stream event
  produces a terminal error, cancels/joins the backend, and is not silently
  discarded. Session persistence still runs after stream failure.
- The dual HTTP/TLS listener owns pending protocol classifiers. Shutdown
  closes their sockets outside the registry lock and joins them, then shares
  a five-second HTTP shutdown budget across both servers. Active handlers
  receive root cancellation. A shutdown timeout is surfaced and remaining
  connections are force-closed.
- First-byte classification has a five-second deadline, HTTP header reads
  five seconds, idle connections 90 seconds. These are time bounds, not
  aggregate connection-count limits.
- The RPC relay owns its listener, tracked connections, and two copies per
  accepted connection. Cancellation closes both sides and joins both copies
  before returning. TLS handshakes have a ten-second deadline.
- Pipeline HTTP encoders use `io.Pipe` writers with capacity-one completion
  channels. Reader/body closure unblocks ordinary pipe writes. Some error
  paths do not explicitly join the encoder before returning; an arbitrary
  legacy activation reader that ignores cancellation can outlive the request.
  No unconditional join was added that could hang on that reader.
- Raft's future adapter uses a capacity-one completion channel around a
  noncancelable future. Caller cancellation may return before that goroutine;
  the Raft operation/transport/shutdown must eventually complete the future.
  Cancellation after submission can leave an unknown operation outcome.

Transport-owned concurrent connections and their copy/classifier goroutines
have no global admission cap. This is an explicit remaining deployment limit,
not a claim of bounded total process concurrency.

Evidence: [stream ownership tests](../internal/orchestrator/harness_turn_ownership_test.go),
[dual-server tests](../internal/peeridentity/ownership_test.go), and
[RPC ownership tests](../internal/nodeagent/rpc_ownership_test.go).

## Stage, process, and root lifecycle

### Pipeline stages

The node stage component owns registered stages, request leases, expiration
timers, and engine runtimes. Per-request operation channels serialize engine
operations and allow cancellation while waiting; no registry lock spans
engine I/O. Duplicate opens are rejected before touching an existing engine
request. Failed opens remove registration and release the lease.

Close fences opens/loads, stops and joins expiry callbacks, closes remaining
requests, releases capacity, and unloads runtimes. Successful opens/loads that
race shutdown perform independent bounded cleanup rather than publishing a
new live resource. Errors are aggregated/surfaced; cleanup continues when an
individual close fails. Expiry uses a five-second cleanup context. Node
shutdown provides the stage component a 30-second context before shutting down
managed engines. Engine cooperation is required; a context deadline does not
kill a blocked Go goroutine.

Evidence: [stage ownership tests](../internal/nodeagent/stage_ownership_test.go).

### Managed subprocesses and shard groups

The supervisor owns process handles, process-tree termination, pipe readers,
readiness checks, wait/restart goroutines, and temporary startup resources.
Its manager wait group and once-guarded close join owned workers. Preflight
runs before opening pipes. Restart attempts/backoff are bounded and canceled
by owner shutdown. Shard groups serialize lifecycle and capacity changes;
partial startup releases reservations and stops already-started workers.

Process creation remains under manager/engine serialization locks so
start/publication/close cannot race. This OS-call-under-lock exception is
retained rather than changing process publication semantics in this phase.
Per-plan pipeline mutexes also remain non-context-aware and their ID map is
retained for the owner's lifetime.

### Root application

SIGTERM and Ctrl+C now use the normal cooperative shutdown path, as does tray
Quit. Startup election/browser waits are cancellable; the startup beacon is
joined. The updater receives the run context and is joined on normal quit.
The self-update callback uses nonjoining shutdown to avoid joining itself and
preserve image handoff. Signal notifications are restored before cleanup so
a second signal can force the platform's normal termination action.

Startup constructors without context support can delay transition into
shutdown. Windows tray behavior and self-update image replacement remain
platform-specific. The real-main SIGTERM subprocess test runs on non-Windows;
Windows control-event delivery is not automated by that test.

The tray menu listener is process-lifetime work: menu Quit returns its loop;
signal/self-update paths exit the process after component cleanup rather than
joining a native tray message loop. The staged update helper is intentionally
detached from the old process. Linux parent-exit polling has a caller-supplied
deadline; staged-image removal retries at most 100 times with 100 ms waits.
These handoff waits are not request-owned jobs. CLI engine diagnostics cap
both probe count and simultaneous probes, then join their wait group; probe
completion depends on each adapter's HTTP timeout. Hardware subprocess probes
use their bounded command contexts.

Evidence: [signal integration test](../cmd/maistr0/signal_shutdown_test.go).

## Discovery, retry, synchronization, and lock policy

- Discovery owns one UDP reader and a service-owned beacon. Concurrent
  Close callers close the socket once and all wait for the reader to finish.
  Peer TTL is 15 seconds. Reconnect delay grows from 250 ms to five seconds
  and retries for the service lifetime.
- Heartbeat probes are serial and bounded. Election callbacks can already be
  running when a monitor closes; failover fences late promotion but does not
  promise to join every callback. Trusted-peer probing fanout scales with the
  currently visible registry rather than a fixed global probe pool.
- Dispatch attempts are capped at five, with context-aware exponential waits
  from 100 ms capped at 800 ms and exclusion of tried node/model pairs.
  Supervisor retries have their existing attempt bounds. Several loops have
  no jitter: synchronized recovery can still create a retry burst. No random
  policy change is claimed here.
- Memory/task sync loops are service-owned, paginated, cancellable, and use
  existing adaptive intervals. Snapshots/page metadata can be proportional
  to stored data; page limits do not make snapshot memory constant-size.
  Persistence formats, transaction ordering, and watermark recovery are unchanged.
- Preserve task-manager lock order: `persistGate` then task `persistMu` then
  `Manager.mu`; release manager state before waiting for a task lock.
  Engine scans take `engineScanMu` before `enginesMu`; engine calls do not
  run under `enginesMu`. Inventory refresh follows `refreshMu` then `modelMu`;
  runtime model selection now belongs to that inventory owner, not agent state.
  Network/disk operations should remain outside state locks
  except the documented supervisor serialization boundary.

Evidence: [discovery close test](../internal/discovery/ownership_test.go) and
existing recovery, synchronization, supervisor, and lock/race tests.

## Dashboard and scripts

Dashboard source is under [web/src](../web/src); static bundles are generated,
not independently maintained implementations. Snapshot rendering coalesces
animation frames and owns EventSource closure. Chat and terminal operations
use abort controllers; history/memory use loading or generation guards.
Fixed page-lifetime pairing/persistence/cluster polling intervals remain.
Under a stalled network some polling fetches can overlap; there is no universal
fetch deadline or global in-flight browser-request cap. This is a retained
limitation, not evidence that every asynchronous producer has been bounded.

The production AST census intentionally excludes scripts and tests. Script
subprocess ownership follows the foreground command or an explicitly
long-lived service, rather than the application's background group.

## Measured correctness costs

Windows, Go 1.27.3, Ryzen AI 9 HX 370; repeated before/after microbenchmarks:

- Pool detached submit median: 307.9 to 304.9 ns/op; 96 B/op unchanged.
  Linked submit: 439.5 to 421.4 ns/op; 223 B/op and three allocations unchanged.
  Ranges overlap; this is not evidence of a performance improvement.
- Node admission: 80.58 to 82.73 ns/op; 40 B/two allocations unchanged.
  Pipeline admission: 65.97 to 71.72 ns/op; 128 B/two allocations unchanged.
- Stage open/close: 1,187 to 1,317 ns/op; 832 to 945 B/op, 20 to 21
  allocations. Cancellable per-request ownership adds one channel, about
  113 B and 130 ns per request. This correctness cost is intentional.
- Post-change split classification: 3,771 to 4,157 ns/op, about 5,803 to
  5,805 B/op and 21 allocations. It includes pipes, buffering, and goroutines;
  there is no paired baseline or isolated lock-cost claim.

These are synchronization microbenchmarks, not end-to-end inference gains.
Phase 4's separate [performance report](./performance-evaluation.md) remains
the measured end-to-end reference.

## Phase 7 ownership comparison

The responsibility refactor keeps existing owners rather than adding a service,
package, or generic interface. The executor's dependency on `agentState` goes
from one to zero; inventory's three-method selection-state dependency is
removed. Its existing telemetry/adapter dependencies remain explicit. General
node state no longer owns active counts or runtime model policy.

`activeMu` owns counts, including ungated work, independently of slot admission.
`modelMu` owns selection and cached flags in one critical section. Configuration
does not wait for engine probes, invalidate otherwise usable inventory, or trigger
additional probes. Refresh publication reapplies current selection under that
same lock. Three copies of flag calculation become one owner-local helper.
No component lock spans a new external call, and shutdown/cancellation semantics
are unchanged. See the [architecture assessment](./architecture.md#node-ownership-boundaries)
for retained cohesive responsibilities.

Before/after measurements used the same Windows Go 1.27.3 / Ryzen AI 9 HX 370
workload, five samples of 200 ms each:

```powershell
go test ./internal/nodeagent -run '^$' -bench 'BenchmarkExecutionAdmission$|BenchmarkProfileInventory$' -benchmem -benchtime=200ms -count=5
```

- Execution admission: median 82.70 to 84.31 ns/op; 40 B/op and two allocations
  unchanged. The ranges overlap; no performance gain is claimed.
- Cached 64-model inventory: median 4,111 to 3,924 ns/op; 22,144 B/op and 66
  allocations unchanged, with zero probes/op.
- Forced 64-model refresh: median 11,055 to 10,494 ns/op; 54,632 to 54,536 B/op
  and 82 to 80 allocations, with one probe/op. Removing cross-owner selection
  snapshots removes two map allocations; the timing ranges overlap.

These are maintenance/allocation checks, not end-to-end inference measurements.
Latency sample quantization in this benchmark does not support a tail-latency
claim. No new optimization policy or added hot-path allocation is involved.

New deterministic tests first reproduced stale flags after a default-only
properties update and after selection changed during a blocked inventory probe.
Both now pass. Extended tests cover snapshot isolation, atomic concurrent
selection/publication, endpoint-specific omitted/null/empty behavior, mixed
execution accounting, and independent concurrent release obligations. The
component tests use injected fake engines and no unrelated runtime startup.

## Verification

Phase 7 validation results:

- `go build ./...`
- `go vet ./...`
- `go test -p=1 -count=1 -timeout=120s ./...`: the initial run passed. The final
  rerun hit the pre-existing intermittent supervisor assertion described below;
  all other packages passed.
- `node --test web/app_test.js`: 59 tests passed.
- Native Windows full race suite:
  `.\build\test-race.ps1 -GoTestArgs @('-p=1','-count=1','-timeout=180s','./...')`.
- Linux full race suite on WSL Ubuntu, Go 1.27.3:
  `go test -race -count=1 -p=1 -timeout=180s ./...`. This includes the real-main
  SIGTERM subprocess test. Both full race suites ran after the final production
  change, sequentially to avoid discovery-port collisions; neither reported
  a data race.
- `go generate ./contracts ./internal/scheduler`: all 66 fixture files have
  identical before/after SHA-256 hashes, including earlier uncommitted additions.
- Two census regenerations were byte-identical; report links resolve;
  `git diff --check` passes. Dependency manifests are unchanged.

The native race helper detected DLL shadowing on the inherited PATH and put
the existing MinGW compiler directory first for that child process. No global
PATH change, dependency installation, or test exemption was needed. Windows
control-event delivery is not covered by the POSIX SIGTERM test; that is a
platform-specific coverage limitation, not a skipped race suite.

Focused tests cover saturation, cancellation, concurrent closure, queued
reference release, fatal panic policy, stream write/encoding failure, relay
joining, stage cleanup, and cooperative signal shutdown. No live model download
or accelerator is required. These results establish the tested failure paths,
not forced cancellation of arbitrary engines or unlimited-connection safety.
Existing user changes and earlier phases remain uncommitted and preserved.

### Existing supervisor test observation window

Phase 8 follow-up: the test now waits for the terminal restart-limit error as
well as failed state/count, removing this observation window without changing
supervisor behavior. Ten isolated repetitions passed. The following records
the historical Phase 7 result rather than rewriting it as a pass.

The final ordinary suite failed
`TestManagerAutomaticallyRestartsCrashLoopWithBackoffLimit`: its predicate
accepts `failed` plus two restarts, then asserts the terminal error. However,
[process exit](../internal/nodeagent/supervisor/manager_process.go) publishes
`failed` with `"process exited with code 17"` before a separate
[restart worker](../internal/nodeagent/supervisor/manager_lifecycle.go) publishes
`"restart limit exceeded"`. The predicate can observe that intermediate state.
This assertion and the relevant state transitions predate Phase 7; the refactor
does not change the supervisor. The full suite initially passed, both full race
suites passed, and the isolated command
`go test ./internal/nodeagent/supervisor -run '^TestManagerAutomaticallyRestartsCrashLoopWithBackoffLimit$' -count=5 -timeout=90s`
passed all five repetitions after the failure. This is an observed test
observation-order issue, not a ThreadSanitizer report. It remains unfixed to
avoid an unrelated supervisor change; the final ordinary run is not reported
as an unconditional pass.

Earlier Phase 5 verification also repeated the orchestrator package after its
stream-encoding change and ran selected stream/admission regressions five times.
The Phase 7 full suites above include those regressions without new exemptions.
