# Lean / DRY / Fast Program Baseline

Collected 2026-10-26 from revision `2f77419`. Go measurements used Go 1.27.3 on
Windows/amd64, AMD Ryzen AI 9 HX 370 w/ Radeon 890M; benchmark labels report
`GOMAXPROCS=24`. Linux race tests used the existing Go 1.27.3 WSL toolchain.
No production code was changed. The working tree already contained unrelated
Copilot guidance edits.

## Size

The repository has **53,219 non-test Go LOC across 45 packages**. `tokei` was
not available, so a small Python scanner counted tracked `.go` source lines
containing code after ignoring blank and comment-only lines. It excluded
`*_test.go` and paths under `testdata`; it includes build and code-generation
tools. The table joins those LOC with `go test -cover` statement coverage;
`—` means the package was not reported by `go test ./...`.

| Package | Non-test LOC | Coverage |
|---|---:|---:|
| `build` | 208 | — |
| `cmd/maistr0` | 1,536 | 30.5% |
| `cmd/maistr0-bench` | 77 | 72.3% |
| `cmd/updatebundle` | 53 | 0.0% |
| `contracts` | 1 | no statements |
| `contracts/cmd/exportphase1` | 257 | 0.0% |
| `contracts/cmd/exportshards` | 200 | 0.0% |
| `internal/agent` | 423 | 54.0% |
| `internal/agent/deepseekadapter` | 125 | 0.0% |
| `internal/apperr` | 101 | 59.5% |
| `internal/background` | 88 | 86.7% |
| `internal/benchrun` | 414 | 87.4% |
| `internal/closeutil` | 5 | 100.0% |
| `internal/cluster` | 386 | 86.6% |
| `internal/config` | 914 | 81.5% |
| `internal/consensus` | 1,163 | 73.0% |
| `internal/deepseek` | 2,793 | 61.0% |
| `internal/discovery` | 582 | 74.0% |
| `internal/engine` | 2,606 | 63.1% |
| `internal/hardware` | 1,614 | 74.3% |
| `internal/httpx` | 340 | 77.3% |
| `internal/hub` | 170 | 88.9% |
| `internal/inferroute` | 54 | 41.2% |
| `internal/integration` | — | no statements |
| `internal/logging` | 319 | 64.2% |
| `internal/memory` | 4,051 | 70.7% |
| `internal/nodeagent` | 8,005 | 61.3% |
| `internal/nodeagent/supervisor` | 3,243 | 68.4% |
| `internal/orchestrator` | 13,448 | 72.2% |
| `internal/peeridentity` | 1,037 | 80.7% |
| `internal/piagent` | 581 | 67.1% |
| `internal/scheduler` | 1,609 | 82.4% |
| `internal/scheduler/cmd/contractfixture` | 106 | 0.0% |
| `internal/scheduler/cmd/schedbench` | 55 | 0.0% |
| `internal/scheduler/evaluation` | 703 | 93.2% |
| `internal/scheduler/score` | 693 | 87.8% |
| `internal/selfupdate` | 665 | 28.4% |
| `internal/shard` | 1,784 | 70.0% |
| `internal/syncinterval` | 134 | 92.1% |
| `internal/syncpage` | 88 | 76.0% |
| `internal/taskmgr` | 2,062 | 80.3% |
| `internal/testutil` | 64 | 35.7% |
| `internal/trayapp` | 116 | 0.0% |
| `internal/version` | 62 | 89.2% |
| `internal/workpool` | 159 | 90.5% |
| `web` | 125 | 86.3% |

Cross-builds used `CGO_ENABLED=0`, `GOARCH=amd64`, no build tags, and default
linker settings:

| Target | Binary size |
|---|---:|
| Linux/amd64 | 20,327,144 bytes (19.39 MiB) |
| Windows/amd64 | 20,882,944 bytes (19.92 MiB) |

`go list -deps ./cmd/maistr0` reported **272 packages** on Windows/amd64,
including the standard library. `internal/consensus` is not in that command's
dependency graph. Comparing the whole repository package graph with and
without that package shows these external modules reachable only through
`internal/consensus`:

- `github.com/fatih/color`
- `github.com/hashicorp/go-hclog`
- `github.com/hashicorp/go-immutable-radix`
- `github.com/hashicorp/go-metrics`
- `github.com/hashicorp/go-msgpack/v2`
- `github.com/hashicorp/golang-lru`
- `github.com/hashicorp/raft`
- `github.com/mattn/go-colorable`
- `github.com/mattn/go-isatty`

The shared bbolt, `x/sys`, and other modules are not in this exclusive set.
The consensus package remains tested functionality; this dependency result is
not evidence by itself that it should be removed.

## Quality

Commands and results:

- `go run golang.org/x/tools/cmd/deadcode@v0.52.0 -test ./...`: passed; reported
  one unreachable function, `internal/piagent/provider.go:80`
  (`OpenAICompatibleProvider.Model`). The existing
  [cleanup inventory](./cleanup-inventory.md) documents its optional-interface
  use; this is a reachability false positive, not a safe deletion.
- `go run github.com/mibk/dupl@v1.1.0 -threshold 80 .`: exit 0, 27 clone groups;
  13 include production source and 14 are test-only. Representative production
  pairs include `internal/nodeagent/stage_component.go`,
  `internal/nodeagent/status_component.go`, the node/orchestrator task-sync
  paths, and `internal/orchestrator/compat_streaming.go`. The sync owners have
  different authority and merge policies, and the streaming paths have
  protocol-specific behavior; clone reports are investigation leads, not
  deletion or extraction proofs.
- `go test -cover -count=1 ./...`: passed. Per-package statement results are
  in the table above.

## Performance

Repeated samples use `-benchmem -count=10 -benchtime=200ms`; the pinned
`benchstat` version used to summarize them was
`golang.org/x/perf/cmd/benchstat@v0.0.0-20261009192801-be2c69fb417e`.
Raw samples are benchstat-ready:

- [Snapshot and publication](./bench-baseline/snapshot.txt)
- [Scheduler](./bench-baseline/scheduler.txt)
- [Sync paging](./bench-baseline/sync-paging.txt)
- [Streaming and routing](./bench-baseline/streaming.txt)
- [Benchstat summaries](./bench-baseline/benchstat-summary.txt)

Selected medians from `benchstat`:

| Workload | Result |
|---|---|
| Snapshot build, 100 nodes / 50 tasks | 653.6 us/op, 641.5 KiB/op, 1,629 allocs/op |
| 100K in-memory history snapshot | 3.426 ms/op, 25.18 MiB/op, 6 allocs/op |
| 10K full vs incremental snapshot payload | 2.379 MiB vs 2.563 KiB |
| Hub publication with 64 subscribers | 48.32 us/op, 149.4 KiB/op, 7 allocs/op |
| Scheduler assignment, 1,000 node-model entries | 444.2 us/op, 749.2 KiB/op, 26 allocs/op |
| 10K memory page transfer | 19.52 ms/op, 29.54 MiB/op, 69,500 allocs/op |
| 100K memory page transfer | 307.7 ms/op, 588.3 MiB/op, 4.696 million allocs/op |
| 10K page request with 90% of records already consumed | 2.721 ms/op, 3.965 MiB/op, 20,820 allocs/op |
| Transparent route, simulated worker | 95.96 us/op, 31.05 KiB/op, 247 allocs/op |
| Slow streaming route, simulated 2 ms chunk delay | 12.53 ms/op, 51.11 KiB/op, 280 allocs/op |

The 100K transfer benchmark reports a 22.57 MiB source snapshot and transfers
100K records over pages. `BenchmarkProfileSyncPage` also allocates 25.79 MiB
per 100K-record request with 90% consumed. These are synthetic in-memory
workloads, not production traffic measurements.

The 100-concurrent-stream benchmark needed a separate one-iteration mode:
`-count=10 -benchtime=1x -cpu=1`. It measured 34.03 ms per 100-stream batch,
2,939 streams/s, 4.475 MiB/op, and 36,040 allocs/op. The compatibility streaming
benchmark's one-iteration repeated sample was 496.4 us/op and 127.9 KiB/op,
but had 74% time variation and should not be compared to the 200 ms samples.
Under repeated 200 ms samples, the 100-stream benchmark returned HTTP 502
("no additional compatible inference node is available") in 2 samples; the
compatibility benchmark also failed all 10 samples with local worker
connection-refused errors. The isolated one-iteration runs passed. Failure logs
are retained as [router](./bench-baseline/streaming-initial-failure.txt) and
[compatibility](./bench-baseline/streaming-compat-failure.txt) diagnostics.
Treat this as a benchmark/load reliability and overload-contract question, not
as a proven production regression.

CPU and heap profiles used the existing synthetic `httptest` benchmark
workloads; heap observations below use pprof's `alloc_space` sample index. The
profiles and `go tool pprof -top` output are saved under
[bench-baseline](./bench-baseline/):

| Workload | Profiles | Profile evidence |
|---|---|---|
| Routing (`BenchmarkProfileRouting`) | [CPU](./bench-baseline/routing.cpu.pprof), [heap](./bench-baseline/routing.heap.pprof) | CPU top was `runtime.cgocall` (22.55%); allocation top was `bufio.NewReaderSize` (14.35%), followed by cluster clone and HTTP parsing work. |
| Snapshot publication (`BenchmarkPublishSnapshotRequests`) | [CPU](./bench-baseline/snapshot-publication.cpu.pprof), [heap](./bench-baseline/snapshot-publication.heap.pprof) | Aggregated profile includes synchronous baseline and queued publisher sub-benchmarks. The publisher request itself measured 5.481 ns/op and 0 B/op; synchronous snapshot work allocated heavily (`bytes.Clone` 20.46%, `fmt.(*buffer).write` 19.40%). |
| Memory sync (`BenchmarkProfileMemorySyncHTTP`) | [CPU](./bench-baseline/memory-sync.cpu.pprof), [heap](./bench-baseline/memory-sync.heap.pprof) | Allocation profile: `memory.snapshotSince` 60.77%, `memoryStore.Snapshot` 11.84%, and `PageSnapshot` 7.57%. CPU was GC-heavy (`runtime.scanObject` 7.67%). |

These are instrumented profiles across synthetic sub-benchmarks; profile
percentages are not uninstrumented production shares. Full top tables are in
[profile-tops.txt](./bench-baseline/profile-tops.txt).

## Test health

- `go test -shuffle=on -count=3 ./...`: passed on Windows/amd64; no flaky or
  order-dependent test was observed in these runs. `internal/integration` and
  `internal/nodeagent/supervisor` took 93.9 s and 99.8 s, respectively, across
  the three repetitions.
- `go test -race -count=1 ./...`: passed in Ubuntu WSL with the existing
  `~/.local/go1.27.3/bin/go` toolchain; no toolchain was installed.
- `go build ./...`, `go vet ./...`, and `node --test web/app_test.js`: passed;
  the Node suite reported 61 passing tests.
- A test binds a fixed UDP port: `internal/discovery/discovery_test.go`'s
  `TestListenerWaitReady` uses the discovery listener on UDP port 7999.
  TCP test listeners found by the scan use `127.0.0.1:0`. The prior
  [cleanup inventory](./cleanup-inventory.md) records a Windows collision on
  UDP 7999; the current shuffled run passed.
- There are 23 direct `time.Sleep` calls in Go `*_test.go` files; the longest
  literal delay is 50 ms. The supervisor fake-engine fixture is an exception
  outside `*_test.go`: its `tree` mode sleeps 150 ms and its `waitForever`
  helper sleeps for an hour between checks. These are process-lifecycle test
  fixtures, not ordinary in-process test waits.

## Ranked findings by expected payoff vs effort

Ranks are engineering estimates based on the evidence above, not claims of
measured improvement. Validate each workload and its semantic constraints
before changing implementation.

| Rank | Finding and evidence | Expected payoff | Effort |
|---:|---|---|---|
| 1 | Memory sync materializes/copies substantial history: 100K page request is 307.7 ms, 588.3 MiB, and 4.696M allocations; the HTTP sync allocation profile attributes 60.77% to `snapshotSince`. | High latency/GC reduction | High |
| 2 | Large paged transfers amplify work: 10K transfer is 19.52 ms and 29.54 MiB; 100K is 307.7 ms and 588.3 MiB. Start with page working-set and repeated-copy analysis while preserving watermarks and mixed-version behavior. | High for large stores | High |
| 3 | Fixed UDP 7999 makes `TestListenerWaitReady` susceptible to external port collisions; a previous collision is documented even though this shuffled run passed. | More reliable CI | Low |
| 4 | The 100-stream router benchmark returns 502 under sustained 200 ms repetitions but passes one-batch samples at `-cpu=1`. Confirm overload semantics and benchmark capacity assumptions before pursuing throughput work. | High if workload is supported | Medium |
| 5 | Snapshot construction is measurable: 100-node/50-task `BuildSnapshot` costs 653.6 us and 641.5 KiB; profiles show clone and formatting allocations. The queued publisher request is already 5.481 ns and allocation-free, so focus on payload construction rather than enqueueing. | Medium | Medium |
| 6 | Scheduler work grows with candidate count: 1,000 node-model entries cost 444.2 us and 749.2 KiB/op, versus 8.503 us and 10.83 KiB at 10 entries. Optimize only for a supported large-cluster workload. | Medium at scale | Low-Medium |
| 7 | Coverage is lowest in `internal/selfupdate` (28.4%), `cmd/maistr0` (30.5%), `internal/testutil` (35.7%), and `internal/inferroute` (41.2%). Prioritize lifecycle and routing error-path tests. | Medium risk reduction | Low-Medium |
| 8 | `dupl` reported 13 production-source clone groups. The node/orchestrator sync groups are large, but their authority and merge behavior differ; assess shared pure transformations rather than extracting whole flows. | Medium maintenance | Medium-High |
| 9 | LOC is concentrated: orchestrator 13,448, nodeagent 8,005, and memory 4,051 LOC; together they account for 47.9% of measured production Go LOC. Use responsibility boundaries and ownership evidence, not file-size limits, to select further splits. | Medium maintainability | High |
| 10 | Nine external modules are only reachable through `internal/consensus`, but the CLI dependency graph and binary do not include them. Removing them would not shrink the measured CLI binaries and could remove tested functionality. | Low unless consensus is retired | High |
