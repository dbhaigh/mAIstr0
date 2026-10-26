# Evidence-based cleanup inventory

## Scope and compatibility

This records the Prompt 2 cleanup against the working tree after the
distributed-system characterization work, not against a clean release checkout.
The earlier fixes and tests are preserved and are not counted as cleanup.
No commits were created.

The cleanup removes 18 uncalled internal functions or methods, one unused
constant, two redundant fields, and three unnecessary adapter forwarding
methods. Five areas of duplicate implementation now share existing owners or
small helpers. No packages, dependencies, platform implementations, routes,
JSON fields, configuration keys, storage encodings, or generated assets were
removed or changed by this phase.

## Evidence and deletion criteria

The audit covered Go production and test code, command entry points, adapters,
HTTP handlers, dashboard source and generated-asset rules, build scripts,
workflows, configuration, and dependency manifests.

Evidence included:

- Repository-wide reference searches and a standard-library Go AST census of
  declarations, identifier occurrences, and identical function bodies. The
  census included platform-specific files and test fixtures; it did not rely
  only on the current platform's build graph.
- `go run golang.org/x/tools/cmd/deadcode@v0.52.0 -test ./...` for whole-program
  reachability from application and test entry points.
- The configured golangci-lint checks, including `unused`, and a supplementary
  `dupl` scan at an 80-token threshold. The initial duplicate scan reported 12
  reciprocal pairs. These were investigation candidates, not deletion proofs.
- Call-site inspection for interface dispatch, optional interfaces, JSON
  methods, reflection, templates, platform tags, runtime tool registration,
  subprocess fixtures, DOM handlers, and test exports.
- Focused package tests after each logical cleanup and final full validation.

An internal exported name was not assumed to be live merely because it was
exported, nor assumed dead merely because textual search found no direct call.
Before snapshots were retained outside the checkout to distinguish this phase
from existing uncommitted work.

## Removed code: before and after

### Uncalled functions and methods

Each item below had no remaining supported caller after tracing its references.
The package implementation, active entry points, and corresponding tests remain.

- [cluster/registry.go](../internal/cluster/registry.go):
  removed `Registry.MostCapableNode`, an unused alias for `Leader`.
  Callers use the retained leader-selection API.
- [consensus/node.go](../internal/consensus/node.go):
  removed `Node.WaitForLeader` and `Node.ClusterID`. Neither had callers.
  Consensus startup, election observation, and identity persistence remain.
- [consensus/tls_transport.go](../internal/consensus/tls_transport.go):
  removed `NewNetworkTransport`, an unused construction wrapper.
  The TLS stream layer and active Raft transport construction remain.
- [discovery/discovery.go](../internal/discovery/discovery.go):
  removed `Listener.FirstOrchestrator`, an unused polling alternative.
  Ranked discovery/election and readiness APIs remain.
- [logging/slog.go](../internal/logging/slog.go):
  removed the uncalled `Debugf` wrapper and unused `DefaultLevel` constant.
  Active structured logging and level configuration remain.
- [peeridentity/dualserver.go](../internal/peeridentity/dualserver.go):
  removed `ServeDual`, an unused background-context wrapper.
  Context-aware serving and the listener-based test seam remain.
- [scheduler/graph.go](../internal/scheduler/graph.go):
  removed `TaskGraph.TransitiveDependents` and its otherwise uncalled
  `Dependents` helper. Active dependency ordering is unchanged.
- [scheduler/agents.go](../internal/scheduler/agents.go):
  removed `UnregisterCustomAgentProfile`, which had no callers.
  Profile registration and lookup remain.
- [scheduler/factory.go](../internal/scheduler/factory.go):
  removed `SubAgentFactory.Get` and `List`. Returned handles are used directly;
  no caller used the factory as a handle registry.
- [deepseek/harness.go](../internal/deepseek/harness.go) and
  [deepseek/tool_registry.go](../internal/deepseek/tool_registry.go):
  removed `Harness.RegisterTool` and `ToolRegistry.Register`. The former was
  uncalled and was the latter's only caller. Typed/default registration and
  executable tool entry points remain.
- [nodeagent/engine_component.go](../internal/nodeagent/engine_component.go):
  removed the uncalled `Agent.refreshReportedModels` facade. The engine
  component's active refresh method and interface remain.
- [orchestrator/server.go](../internal/orchestrator/server.go):
  removed unused constructor `New`; configuration-aware constructors remain.
- [orchestrator/task_planning.go](../internal/orchestrator/task_planning.go):
  removed the uncalled `Server.planSubtasks` facade. The planner-owned method
  still handles actual planning.
- [version/version.go](../internal/version/version.go):
  removed unused `Number`. Version comparison and legacy numeric parsing remain.

### Redundant state

- [consensus/node.go](../internal/consensus/node.go): removed the `clusterID`
  field after removing its only reader. `LoadOrCreateIdentity` still executes:
  its persistent identity creation, validation, and errors are not discarded.
  Only its unused return value is ignored.
- [scheduler/factory.go](../internal/scheduler/factory.go): removed the `agents`
  map, its allocation, and write-only insertion after proving `Get` and `List`
  unreachable. The factory still returns handles and registers role profiles.
  The sequence mutex remains; its type is now `sync.Mutex` because there are
  no remaining read-lock operations. No new lock or hot-path allocation was added.

### Adapter forwarding bodies

Removed `LMStudio.ListModels`, `LlamaCPP.Name`, and `FastFlowLM.Name` from
their [engine adapters](../internal/engine/). Their embedded VLLM implementations
already provide the same methods and still satisfy the engine interfaces.
LMStudio's explicit backend name, llama.cpp capability discovery, and
FastFlowLM device behavior remain separate.

## Consolidated implementations

### Pairing PIN syntax

Before: configuration and node pairing maintained identical six-ASCII-digit
validators.

After: both use `config.ValidPairingPIN` in
[config.go](../internal/config/config.go). Configuration still decides separately
whether an empty value is allowed; expiration and reuse policy are unchanged.
`TestValidPairingPIN` covers accepted digits, leading zeroes, lengths,
whitespace, letters, and non-ASCII digits.

### Trust-pin updates

Before: `TrustPeer` and `TrustPeerDetails` duplicated validation and pin updates.

After: `TrustPeer` delegates to `TrustPeerDetails` in
[identity.go](../internal/peeridentity/identity.go). Existing metadata-preserving
updates, persistence rollback, and connection invalidation have one owner.
`TestTrustPeerRefreshPreservesDetails` checks both entry points, certificate
mismatch, successful replacement, persistence failure, and reopened durable state.

### Request identifiers

Before: the router duplicated random-byte and hexadecimal request-ID generation.

After: [router_handlers.go](../internal/orchestrator/router_handlers.go) calls
the existing `httpx.NewRequestID`; its duplicate helper and imports are gone.
No new ID format or error policy was introduced.

### Upstream HTTP status classification

Before: router and dispatch code had identical status-to-error classification.

After: both use `classifyUpstreamStatus` in
[dispatch_retry.go](../internal/orchestrator/dispatch_retry.go).
Existing classification tests exercise the shared implementation.
Retry budgets, timing, body limits, and streaming response-commit rules remain
owned by their respective callers.

### Recent-task cache reconstruction

Before: `Recent` and `PageRecent` each rebuilt and sorted the same cache.

After: both call `ensureRecentTasksLocked` in
[query.go](../internal/taskmgr/query.go) under their existing lock.
Updated-time descending ordering, descending ID tie-breaking, invalidation, and
defensive result cloning are unchanged. `TestRecentAndPageRecentShareCacheOrdering`
warms the cache through either API and checks ordering before and after mutation.
This is shared maintenance logic, not a claimed performance optimization.

### Compatibility stream line parsing

The OpenAI SSE, Ollama NDJSON, and Anthropic SSE writers now share
`compatStreamParser` for newline framing and `data:` extraction. Each writer
still owns its protocol headers, error/output events, and terminal frames.
Characterization tests cover mid-line and mid-UTF-8 chunk splits, empty chunks,
an upstream error followed by more output, and client disconnect propagation.

`BenchmarkCompatStreamWriters` ran with `-benchmem -count=10 -benchtime=200ms`
on Windows/amd64, Go 1.27.3. Benchstat reported allocations per stream of
OpenAI 87→86, Ollama 51→50, and Anthropic 113→112; bytes/op fell by 1.15%,
2.03%, and 0.70%, respectively. It found no statistically significant
time/op change for any writer.

### Memory-sync accounting wrappers

The node-agent and orchestrator now use `syncio.CountingReadCloser` and
`syncio.CountingResponseWriter` from one internal package. Characterization
tests first verified that both copies counted observed bytes, propagated each
write result, and retained the first write error.

`BenchmarkMemorySyncCountingIO` ran with `-benchmem -count=10 -benchtime=200ms`
on Windows/amd64, Go 1.27.3. Both reader and response-writer cases remained at
0 B/op and 0 allocs/op. Benchstat found no significant time/op change
(reader 2.881→2.894 ns/op, p=0.403; writer 2.120→2.127 ns/op, p=0.796).

## Retained indirect uses and compatibility paths

- `OpenAICompatibleProvider.Model` in
  [provider.go](../internal/piagent/provider.go) remains despite the final
  whole-program scan reporting it unreachable from current roots.
  [agent_loop.go](../internal/piagent/agent_loop.go) explicitly checks the
  optional `interface{ Model() string }` to populate assistant metadata.
  Removing the concrete method would change supported provider behavior.
- Raft store methods such as `FirstIndex`, `StoreLog`, `SetUint64`, and
  `GetUint64` in [bolt_store.go](../internal/consensus/bolt_store.go) implement
  third-party interfaces. A lack of direct calls is not evidence of dead code.
- `TaskGraph.UnmarshalJSON` is invoked by `encoding/json`. JSON-tagged fields,
  reflection-based tool arguments, contract fixtures, and template inputs remain.
- The [consensus package](../internal/consensus/) is not the LAN-election startup
  implementation, but contains tested persistence and consensus functionality.
  Its apparent absence from LAN startup is not sufficient evidence for deletion.
- Legacy version parsing in [version.go](../internal/version/version.go)
  preserves comparisons with older version strings.
- Platform-specific files, unsupported-platform fallbacks, tray entry points,
  and subprocess test programs remain. Native and cross-platform paths were
  considered before removing declarations.
- Dashboard source functions are concatenated and referenced through globals,
  DOM handlers, and CommonJS test exports. The source/test/index name scan found
  no function name used only at its declaration.
- Hashed dashboard assets, the unhashed compatibility alias, and gzip/Brotli
  sidecars have distinct serving/cache roles. Rebuilding reproduced the tracked
  assets; none were treated as disposable duplicate files.

## Duplication retained or deferred

### Different semantics: retain separate ownership

- [workpool](../internal/workpool/) already shares bounded dispatch/peer work.
  Worker execution semaphores, node dispatch gates, pipeline slots, and long-lived
  shard reservations enforce different lifetimes and external capacity rules.
  Pipeline reservations acquire global slots; shard reservations do not.
- [httpx](../internal/httpx/) already shares client and transport profiles.
  Bootstrap/pinned trust, download redirects, context-bounded streaming, and
  short health-check versus longer inference timeouts are intentional policies.
  Two one-line pinned failover client constructions do not justify another
  configurable abstraction.
- Domain categories and ordinary HTTP mapping already use
  [apperr](../internal/apperr/) and shared HTTP helpers. OpenAI, Ollama,
  Anthropic, and SSE error envelopes remain protocol-specific.
- `background.Wait` checks cancellation before and after timer completion;
  the orchestrator's `sleepWithContext` has different simultaneous-ready
  selection semantics. They were not substituted silently.
- OpenAI-compatible adapters already share VLLM protocol handling. llama.cpp
  `/props` enrichment, FastFlowLM XDNA2 fallback, and Ollama protocol handling
  remain backend-specific.
- CLI diagnostics clear the memory path before validation to avoid filesystem
  effects. Startup and print-config role/address/memory overlays differ.
  Shared flag-heavy configuration plumbing would obscure those differences.
- Node-peer and orchestrator task-sync callers have different authority/merge
  policies and observable error prefixes. Snapshot, incremental, and watermark
  sync paths also have distinct failure/advance rules.
- bbolt and in-memory adapters intentionally implement the same store contract
  separately. Legacy fixtures and common semantics tests protect parity.
- Shell and PowerShell scripts support native workflows; release paths also
  distinguish semantic versions from four-component numeric package versions.

### Similar code: defer low-value abstractions

These are not claimed to be semantically impossible to consolidate:

- Small session/shard bucket-enumeration and in-memory copy loops have different
  typed owners, buckets, and results. Generic storage plumbing is not warranted.
- Similar local/remote model handlers, managed-operation auth/decode sequences,
  prefill/decode dispatch, and repeated narrow dependency-interface shapes were
  retained rather than introducing operation flags or generic handler layers.
- Memory cache loading and uncached experience queries resemble one another,
  but have different ownership and query-limit behavior.

## Dependencies

[go.mod](../go.mod) and [go.sum](../go.sum) are unchanged.
`go mod tidy -diff` found no drift. All five direct dependencies remain justified:

- `getlantern/systray`: supported Windows tray integration.
- `hashicorp/raft`: consensus implementation and tests.
- `santhosh-tekuri/jsonschema/v6`: contract validation tests; test-only use
  does not make a module dependency redundant.
- `bbolt`: persistent stores.
- `x/sys`: platform/hardware integration.

The 17 explicitly indirect requirements remain transitively needed.
The dashboard's esbuild development dependency remains its build tool.
Analysis tools were invoked with pinned `go run ...@version` commands; no
analysis dependency was added to the project.

## Performance findings, not optimizations

Inventory refresh already caches/coalesces work. Snapshot clones and
generation-aware maps/sorts enforce ownership and freshness; they were not
removed merely because they allocate.

Follow-ups requiring measurement include snapshot JSON encoding and dashboard
signatures, full memory snapshot scan/sort and byte-limited page serialization,
trusted-interface status maps, and routing candidate/header normalization.
Existing snapshot, concurrent streaming, memory-history, and idle-sync
benchmarks are suitable starting points. No benchmark speedup is claimed.
The factory map removal is justified by unreachable readers, not speculation
about a hot path.

## Verification

Focused package tests passed after each logical cleanup. Final evidence:

- `go build ./...`: passed.
- `go vet ./...`: passed.
- `go test -p=1 -count=1 -timeout=120s ./...`: passed on Windows, including
  discovery and process-level integration tests.
- `go test -race -count=1 ./...`: passed in WSL Linux with exit code 0.
  This is Linux race evidence, not a claim of native Windows race validation.
- `go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0 run`:
  passed with zero issues using the repository configuration.
- `node --test web/app_test.js`: passed, 58 tests.
- `go mod tidy -diff`: passed with no manifest drift.
- `go generate ./contracts ./internal/scheduler`: passed with no fixture drift.
- `npm --prefix web run build` in WSL: passed and reproduced tracked assets.
- Linux amd64, Linux amd64 with the `ubuntu` tag, and Darwin arm64 cross-builds
  with CGO disabled passed before the final platform-neutral classifier/cache
  consolidation. The final native build and full Linux race suite cover those
  later changes; these are cross-build checks, not cross-platform runtime tests.
- `git -c core.safecrlf=false diff --check`: passed.

### Failures observed during validation

An initial native full run and one focused run timed out in managed
llama.cpp shard lifecycle testing. The pre-cleanup source overlay passed the
focused test; therefore this is not evidence that the failure reproduced on
the baseline. The current focused test subsequently passed twice, and the
final isolated full native suite passed. No cleanup change was made to mask
the timing-sensitive failure.

A later Windows run failed `TestListenerWaitReady` because UDP port 7999 was
already bound while the full WSL race suite ran concurrently. The final native
suite ran without simultaneous WSL network tests and passed. No unrelated
processes were terminated.

The dashboard build could not run with the installed Linux esbuild binary
under native Windows. Running the existing installation in WSL succeeded and
produced byte-identical tracked output; no dependencies were reinstalled.

These observations remain operational/test-isolation follow-ups, not proven
production regressions or reasons to weaken tests.
