# Profile-guided performance evaluation

## Outcome

Retained one production change: [memory page construction](../internal/memory/sync_pages.go)
now holds pointers into the caller's snapshot instead of boxing a copy of every
record into an interface for every page. Only selected records are copied into
the returned, value-typed page. Sorting, legacy sequence synthesis, duplicate
selection, byte accounting, cursor advancement, errors, and watermarks are
unchanged. There are no new dependencies, locks, configuration keys, routes,
wire formats, or storage formats.

On the measured synthetic workload:

- A complete 100,000-record paged serialization took **489.71 -> 292.11 ms**
  per transfer, a **40.4% reduction**. Allocation volume fell
  **1,307,987,144 -> 616,771,420 bytes/transfer**, a **52.8% reduction**;
  allocations fell **7,096,177 -> 4,696,059**.
- The in-process memory-sync HTTP pipeline improved beyond the isolated page
  helper. With 99% of a 10,000-record snapshot already consumed, mean operation
  time was **4.409 -> 3.493 ms**, handler p50 **4.163 -> 3.318 ms**, and handler
  p95 **5.914 -> 4.534 ms**. Throughput was **226.8 -> 286.3 requests/s**.
- In three separate paired, unprofiled resource trials, whole-process CPU
  normalized per transfer fell **0.75571 -> 0.41048 CPU-seconds**. This includes
  harness/setup overhead and is not hardware inference time.
- **Peak resident memory did not consistently improve.** Lower allocation
  volume is not a claim of lower peak RSS or lower retained heap.

Do not extrapolate these numbers to token generation, GPU performance, WAN
latency, or another host. No scheduler algorithm or inference transport was
changed.

## Evidence and environment

- [Main results and all 260 original samples](./performance-evaluation.json):
  26 benchmark cases, five uninstrumented trials before and after.
- [Paired routing controls](./performance-routing-controls.json): five more
  trials per version, alternating original/optimized compiled binaries.
- [Paired CPU/RSS measurements](./performance-resources.json): three trials per
  version/workload, including all raw resource measurements and timing scope.
- Raw profiles are attached to the engineering session as a profile archive.
  They include exact command manifests, before/after production source,
  benchmark JSON events, CPU/memory/mutex/block profiles, execution traces,
  trace-derived scheduling/synchronization profiles, and text summaries.
  Test binaries remain outside the checkout and are not included in the archive.
  Profiles contain symbol information; trace viewing does not require a binary.
  Instruction-level disassembly requires rebuilding the corresponding binary.

Host: AMD Ryzen AI 9 HX 370, 24 logical CPUs. Comparative measurements used
Go 1.27.3, Linux amd64 under Ubuntu WSL, and benchmark `GOMAXPROCS=2`. The host
was shared, not isolated. Both sides used the same fixtures, toolchain, flags,
and logging policy. Standard-library `httptest`, synthetic engines, controlled
delays, fake managed-engine operations, and temporary stores were used. There
were no model downloads or real inference runs.

Each ordinary case used five one-second uninstrumented trials. Workflow and
persistent-memory workloads used 256 operations per trial to bound retained
task/history state. Separate three-second profiling runs captured CPU,
allocation/retained-heap, execution-trace, mutex, and blocking data together;
instrumented timings are excluded from performance comparisons.

Latency samples use a bounded rolling window of 8,192 observations. Each trial
reports nearest-rank p50/p95/p99; the result file summarizes those trial
quantiles, not pooled samples. For concurrent routing, latency is per request
and throughput counts 16 requests per batch. For memory HTTP, handler latency
excludes client decoding, while operation time/throughput includes it.
The existing complete-transfer benchmark reports mean time and allocations,
not latency quantiles.

The native Windows smoke checks showed coarse/zero sub-millisecond clock
observations. Comparative latency claims therefore use Linux measurements,
not those Windows smoke results. Very small component measurements, especially
idle publication, include appreciable timing-instrumentation overhead.

## Profile coverage

The [profiling runner](../build/profile-performance.ps1) records 14 workload
groups on each side. All 28 traces were parsed successfully, with scheduling
and synchronization profiles and heap-object metrics extracted.

- **Direct inference and transparent non-streaming routing:**
  [routing fixtures](../internal/orchestrator/profile_benchmark_test.go) exercise
  the real mux, selection, capacity reservation, HTTP node client, fake upstream,
  and response handling. Successful responses and reservation release are
  asserted. The benchmark server now initializes the production TTFB/idle
  defaults instead of leaving zero-duration timers; production defaults are
  unchanged. Loopback network/runtime work dominated these profiles; they do
  not justify a new transport abstraction.
- **Slow streaming:** the same router receives four flushed synthetic SSE chunks
  and a terminal event, with a controlled 2 ms header/chunk delay. Profile CPU
  utilization was low relative to wall time; trace scheduling stacks include
  incremental `httpx.StreamWriter.Copy` and the idle-timeout reader. Existing
  disconnect/cancellation/failure cleanup tests remain the correctness gate.
  This is not a real model TTFT measurement.
- **Concurrent heterogeneous routing:** batches of 16 requests across three
  synthetic nodes with capacities 1/2/4, scores 20/50/80, and upstream delays
  3/1/0 ms. Requests must succeed and leave no active reservations. Measured
  latency includes capacity waiting; the queued-planning semantics are retained.
- **Workflow planning, dispatch, retries, and synthesis:** two dependent
  explicit subtasks, a deliberately failing preferred worker, successful retry,
  and a real synthesis request. Each workflow asserts its final output and at
  least one failed attempt. The fixture backoff is 1 ms, **not the production
  retry policy**. Fixed iteration counts prevent unlimited retained task history.
- **Inventory and telemetry:**
  [node benchmarks](../internal/nodeagent/profile_benchmark_test.go) compare
  cached and invalidated 64-model inventories, assert probe counts, and exercise
  CPU/GPU usage recording, runtime-device reconciliation, bounded history, and
  hardware generations. All hardware samples and engine inventories are fake.
- **Dashboard:** the existing `BenchmarkBuildSnapshot` builds/serializes
  100 nodes, four models each, and 50 tasks.
  [Publication benchmarks](../internal/hub/profile_benchmark_test.go) run the real
  publisher with zero, one, and 64 subscribers. Changed approximately 60 KiB
  synthetic frames are drained through every subscription channel. The fixture
  removes the debounce to measure completed publication, not a 25 ms timer;
  it does not simulate browser rendering, slow-client sockets, or SSE framing.
  With zero clients, the builder must never run.
- **Memory:** [memory benchmarks](../internal/memory/profile_benchmark_test.go)
  exercise temporary bbolt reads/writes/repeated merges/incremental snapshots,
  10K/100K page preparation with 0/90/99/100% consumed, and the existing complete
  100K transfer. HTTP adapter/store/header/encoding/decoding benchmarks cover
  0/90/99% consumed for a 10K snapshot and enforce the 1 MiB response budget.
  The HTTP adapter is invoked in process: this excludes socket/TLS costs.
  Complete transfer means serialization of every bounded page, not WAN transfer.
- **Shard planning/lifecycle:** real `shard.Plan` with a synthetic 16 GiB model
  and two nodes with device-memory budgets, then create/stop/start/delete using
  fake managed engines. Persisted groups and capacity reservations must be
  cleaned up. These measurements exclude actual child startup and GPU loading;
  the existing process-level managed-shard integration test covers real fake
  subprocess creation/termination.

## Why this optimization

Before editing, the complete-transfer CPU profile attributed **64.1% cumulative
CPU samples** to `PageSnapshot`. Its allocation profile attributed **85.9%
cumulative allocation space**, including **66.7% directly**, to that function.
The helper rebuilt/boxed the entire snapshot on every page, including records
already covered by the cursor. A 100K transfer consequently allocated about
1.3 GB to serialize about 23.7 MB of paginated JSON.

The retained change removes the per-record interface copy without changing
the shared paging algorithm. It adds no cache, invalidation protocol, union
type, or new state owner. Afterward the full-transfer allocation profile's
direct `PageSnapshot` share was 37.1%, with remaining work in record metadata,
keys, the shared pager, JSON, and GC. These shares are descriptive: different
adaptive iteration counts make aggregate profile totals unsuitable for direct
before/after CPU comparisons.

[Reference-parity tests](../internal/memory/sync_page_parity_test.go) preserve
the old boxed representation as a test oracle and compare every page, cursor,
`more` flag, watermark, and error. Cases include all record kinds, shuffled
inputs, identical sort keys, missing upper bounds, multiple origins, synthesized
and explicit legacy records, empty windows, multiple pages, oversized envelopes/
records, invalid timestamp encoding, and skipped invalid records. Input
snapshots/cursors/watermarks must not change. Existing 64 MiB paging, HTTP,
replication, compatibility, and recovery tests remain enabled.

## Latency, allocation, and resource results

Medians of five trial measurements:

- **First HTTP page (10K records, 0% consumed):** mean 12.542 -> 11.348 ms;
  handler p50 9.820 -> 8.272 ms; p95 11.448 -> 9.630 ms;
  throughput 79.73 -> 88.12 requests/s;
  bytes/op 39,592,990 -> 35,411,564;
  allocations/op 43,595 -> 29,096.
- **HTTP page with 90% consumed:** mean 6.274 -> 5.048 ms;
  p50 5.378 -> 4.328 ms; p95 7.360 -> 5.699 ms;
  throughput 159.4 -> 198.1 requests/s;
  bytes/op 24,868,529 -> 21,698,404;
  allocations/op 33,036 -> 22,033.
- **HTTP page with 99% consumed:** mean 4.409 -> 3.493 ms;
  p50 4.163 -> 3.318 ms; p95 5.914 -> 4.534 ms;
  bytes/op 21,086,593 -> 18,178,104;
  allocations/op 30,312 -> 20,212.
- **Isolated 100K first-page construction/encoding:** mean
  21.324 -> 13.615 ms; p50 20.833 -> 13.161 ms;
  p95 26.992 -> 16.768 ms; bytes/op 57,150,493 -> 27,055,566.
- **Isolated 100K already-consumed window:** mean 16.407 -> 9.178 ms;
  p50 16.066 -> 8.934 ms; p95 20.836 -> 13.833 ms;
  bytes/op 44,007,121 -> 15,206,695. Metadata/key construction remains linear,
  and sorting still examines all records even when the output is empty.
- **Complete 100K transfer:** five before means ranged 483.55-503.54 ms;
  five after means ranged 289.36-293.46 ms. Encoded bytes and record counts
  are unchanged. Throughput derived from the median is approximately
  204K -> 342K records/s.

Paired, fixed-operation resource trials used the already-built original and
optimized binaries without profiling, alternating run order. `/usr/bin/time`
measured the whole process, including setup, one benchmark warmup operation,
GC, and cleanup. CPU is normalized by measured plus warmup operations, so it is
not a pure function-only metric:

- Complete 100K transfers, 20 timed operations plus warmup:
  median **0.75571 -> 0.41048 CPU-seconds/operation**; ranges
  0.74857-0.76571 and 0.40952-0.43762. Median peak RSS
  **175.81 -> 176.10 MiB**: effectively unchanged.
- 99%-consumed HTTP pipeline, 1,000 timed operations plus warmup:
  median **0.00793 -> 0.00642 CPU-seconds/operation**; median peak RSS
  **87.83 -> 85.46 MiB**, but overlapping broad ranges
  50.82-90.47 and 50.70-93.00 MiB do not establish a resident-memory gain.

The instrumented transfer traces observed heap-object peaks of
165.77 -> 163.27 MiB, while the HTTP group observed
**65.02 -> 71.88 MiB**. These whole-process peaks include setup/calibration,
uncollected garbage, and profiling effects; they are not retained live heap.
The transfer's final-GC sampled retained heap was about **1.06 MiB on both
sides**. Allocation churn improved; peak-memory guarantees did not.

## Controls, regressions, and rejected changes

The first sequential matrix showed unrelated direct/transparent routing means
12.2%/8.5% slower afterward, despite neither path calling the changed pager.
Those results are retained in the main data, not discarded. Alternating
original/optimized binaries on the same two workloads produced
**503.85 -> 513.15 us (+1.8%)** for direct routing and
**533.22 -> 530.70 us (-0.5%)** for transparent routing. This supports timing
drift, not a demonstrated pager-caused routing regression; it is not a promise
of identical tails on other systems. Remaining control shifts include inventory
refresh +6.0%, concurrent routing +2.9%, and slow streaming +2.3% in the original
matrix. No improvement is claimed for those paths.

Investigated opportunities and decisions:

1. **Inventory/capability reuse:** generation-keyed snapshots, refresh coalescing,
   and inventory TTLs already exist. Cached reads still clone owned data.
   Do not remove ownership copies or weaken freshness for a microbenchmark gain.
2. **Routing/serialization allocations:** retained only the measured page-copy
   removal. Snapshot JSON is visible in profiles, but hand-written JSON or a new
   dependency is not justified by this run.
3. **HTTP reuse:** existing shared clients/transports remain unchanged. Loopback
   syscall/futex samples are not evidence for changing proxy/TLS/timeouts or
   connection shutdown semantics.
4. **Incremental streaming:** the router already copies incrementally through
   `httpx.StreamWriter` with bounded buffers. Slow-upstream traces predominantly
   wait; no full-response buffering was introduced or removed.
5. **Bounded buffers:** wire pages, request limits, stream buffers, publisher
   request coalescing, and per-subscriber queues are retained. Export snapshots
   and pager metadata still scale with total history; this change does **not**
   make store-side synchronization constant-memory.
6. **Idle/cancellation work:** zero-subscriber publication performs no build.
   Cancellation, terminal-event, capacity-release, and background shutdown
   tests remain the gate; no additional goroutines are introduced.
7. **Coalescing unchanged state:** publisher generation/byte comparison and
   debouncing already handle this. The changed-frame benchmark deliberately
   bypasses deduplication; it is not evidence to remove it.
8. **Persistence batching:** the bbolt workload is I/O-heavy. Combining writes
   would change durability/transaction boundaries; no safe transaction batch
   with demonstrated end-to-end benefit was established.
9. **Contention:** HTTP trace synchronization totals are dominated by idle pool
   workers and benchmark/runtime waiting. Sampled memory-HTTP mutex delay was
   about 18 ms over the entire baseline profiling process, mostly runtime locks.
   Do not interpret summed goroutine wait as queue latency or replace safe locks.
10. **Further scan/key/copy changes:** sorting/filtering earlier could reduce more
    work, but legacy numbering and unstable equal-key duplicate ordering need
    care. Retaining the original record order and pager is safer than introducing
    speculative cache/index logic. Store-side indexed pagination is future work,
    not part of this optimization.

## Reproduction

Use PowerShell 7 and the repository's Go toolchain. Choose output outside the
checkout; the runner refuses to overwrite an existing measurement manifest.
Capture before applying one change, then capture after:

```powershell
.\build\profile-performance.ps1 -OutputDirectory "$env:TEMP\maistr0-perf-before" -Mode Both
# Apply one logical change.
.\build\profile-performance.ps1 -OutputDirectory "$env:TEMP\maistr0-perf-after" -Mode Both
.\build\compare-performance.ps1 `
  -BeforeDirectory "$env:TEMP\maistr0-perf-before" `
  -AfterDirectory "$env:TEMP\maistr0-perf-after" `
  -OutputFile "$env:TEMP\maistr0-perf-comparison.json"
```

For this run, both captures additionally used `-WslDistribution Ubuntu` and
`-GoExecutable` pointing to the installed Linux Go 1.27.3 binary.
Substitute your installed Linux Go path. `--exec` preserves regex arguments
without invoking a shell. Use `-Workloads memory-http,sync-page,sync-transfer`
for a focused capture. `-Mode Analyze` extracts summaries from existing traces.
Binary trace-to-pprof output is copied directly from the child stdout stream,
not through PowerShell text redirection.

The manifest records each exact `go test` command. Its common flags are:
`-p=1 -run=^$ -bench=<selector> -benchmem -cpu=2 -timeout=180s`;
baseline adds `-count=5 -benchtime=1s -json`; profiling adds CPU/memory/trace/
mutex/block output flags, a 64 KiB memory sampling rate, mutex fraction 1,
block sampling rate 1,000 ns, and an out-of-tree test binary. Fixed workloads
use `-benchtime=256x`. Do not use the profiling log as the uninstrumented baseline.
The comparison script reconstructs fragmented Go JSON output, requires matching
workload/environment metadata and repetition counts, and preserves every sample.

Inspect an extracted profile without its original test binary:

```sh
go tool pprof -top before/sync-transfer-cpu.pprof
go tool pprof -top -sample_index=alloc_space before/sync-transfer-memory.pprof
go tool pprof -top -sample_index=inuse_space after/sync-transfer-memory.pprof
go tool trace after/slow-stream-trace.out
```

The resource trials used `/usr/bin/time` with
`-f '{"user_seconds":%U,"system_seconds":%S,"elapsed_seconds":%e,"max_rss_kib":%M}'`
and the original/optimized saved test binaries. Common binary flags were
`-test.run=^$ -test.benchmem -test.count=1 -test.cpu=2 -test.timeout=180s`;
complete-transfer selector `^BenchmarkMemoryHistoryPageTransfer$/^100K$` used
`-test.benchtime=20x`, and HTTP selector
`^BenchmarkProfileMemorySyncHTTP$/^99PercentConsumed$` used
`-test.benchtime=1000x`. Three trials alternated version order. Routing control
trials used `go tool test2json` with the direct/transparent saved binaries,
one-second benchmark duration, and five alternating-order trials.

## Validation

Passed after the production change:

- `go build ./...`
- `go vet ./...`
- `go test -p=1 -count=1 -timeout=120s ./...` on native Windows.
- `node --test web\app_test.js`: 58 passed.
- Focused memory-page reference/error/HTTP tests.
- Linux race tests of every new benchmark fixture with `-benchtime=3x`.
- Full Linux/WSL `go test -race -count=1 -p=1 -timeout=180s ./...`.
- Full native Windows race suite through
  `.\build\test-race.ps1 -GoTestArgs @('-p=1','-count=1','-timeout=180s','./...')`.
- `go generate ./contracts ./internal/scheduler`: no fixture drift.
- `go mod tidy -diff`: no dependency drift.
- `git diff --check`: no whitespace errors.
- Native/WSL profiling-runner smoke tests, including paths containing spaces,
  binary trace conversion, result shape checks, and the no-overwrite guard.

`golangci-lint` was unavailable in the native/WSL PATH and standard Go binary
directories; it was not installed for this phase. Build, vet, correctness, and
both full race suites passed independently.

The Windows compiler preflight detected DLL shadowing by installed applications;
the repository helper put MinGW's directory first for that process only. No
global environment changes or toolchain installation were made. Unlike earlier
environment limitations, the native Windows race suite actually passed here.
Windows and WSL networking tests were not run concurrently.
