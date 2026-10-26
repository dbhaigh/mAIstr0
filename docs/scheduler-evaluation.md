# Scheduler evaluation and routing diagnostics

## Decision

Keep the existing production scoring algorithm and weights. The simple
baselines show useful tradeoffs, but none improves all workloads without a
failure-rate or tail-latency regression. No adaptive production policy is
proposed or enabled.

This evaluation adds opt-in candidate diagnostics and a standalone, synthetic
benchmark command. It does not change assignment ordering, capacity semantics,
API/storage contracts, configuration defaults, or GPU offload support.

## Current selection pipeline

### Planning

1. [dispatcherComponent.schedulerNodes](../internal/orchestrator/dispatcher_component.go)
   starts with the registry's healthy, ID-sorted snapshot, overlays managed shard
   advertisements, and includes pending task assignments as queued load.
2. [AssignGraph and pickBest](../internal/scheduler/assign.go) traverse subtasks in
   graph order. Each assignment increments simulated load, and graph out-degree
   adds pending-dependent pressure for subsequent assignments.
3. [score.Prepare](../internal/scheduler/score/score.go) requires a healthy node,
   every required effective capability, and a compatible ready model. Model
   specialty tags and minimum size affect eligibility. A missing model size
   retains the existing permissive minimum-size behavior.
4. NPU-targeted models additionally require a compatible device/vendor/backend,
   a reported memory budget that fits, and applicable quantization/context
   constraints. A zero free-memory value currently falls back to total device
   memory; it is not proof of available free memory. Ordinary GPU model placement
   remains offload-compatible, rather than enforcing shard-style VRAM-only fit.
5. Per-node model preference combines default/preferred model, task/general/fast
   tags, and accelerator compatibility. A preferred model is a soft preference
   here, not an exact-name filter.
6. The additive scorer combines model match, hardware, GPU compatibility,
   required/preferred capabilities, quality, topology, latency preference, and
   learned history. It subtracts effective load, pending dependents, capacity
   overage, and fresh utilization penalties. Full nodes can win planning:
   capacity is a penalty, not a hard filter.
7. Equal node scores preserve input order, which is node-ID order for ordinary
   registry snapshots. Equal model scores preserve advertised model order.
   Neither tie rule was changed.

The scorer is pure and lock-free. Historical bias and measured token throughput
are resolved by the caller before scoring; the benchmark uses no learned
history. An assignment is a plan, not a capacity lease.

### Transparent routing and retries

[assignRouterRequest](../internal/orchestrator/router_handlers.go) filters exact
requested model names, engine readiness, protocol compatibility, already-tried
node/model pairs, and circuit-breaker state before invoking the scorer. An
explicit managed-shard model resolves to its ready head instead.

[Task retry selection](../internal/orchestrator/dispatch_retry.go) excludes tried
node/model pairs and uses a fresh scheduler snapshot. Task and transparent-router
retry paths retain their own bounded attempt budgets, status classification,
cancellation, and backoff. Streaming does not retry after response commitment.
Task dispatch also waits for dependencies before reserving capacity.

### Reservation and execution

[dispatchCapacityGate](../internal/orchestrator/capacity_gate.go) rechecks
health/address identity. Capacity older than six seconds by `LastSeen` triggers
a coalesced authenticated probe; probe failure ejects the node. A nonpositive
advertised concurrency limit retains the configured fallback limit.

[nodeDispatchGate](../internal/orchestrator/node_dispatch_gate.go) accounts for
both local reservations and externally reported occupancy. It acquires a node
slot atomically, then the global dispatch slot, with cancellation rollback.
Full nodes wait; no execution starts just because a planning score won.

Pipeline placement separately sorts complete ready-stage candidates by layer
capacity, fast score, then node ID. Pipeline reservation acquires all members
and a global slot. Persistent shard-node reservations acquire all members but
not a global request slot. Both retain sorted lock acquisition and rollback.
Managed shard placement retains its device-memory/weight/KV-cache safeguards.

### Missing and stale telemetry

Utilization telemetry is fresh only when its timestamp is present, not in the
future, and at most 30 seconds old, inclusive. Missing, future, and older samples
contribute no GPU/NPU/CPU/RAM utilization penalties; they do not by themselves
exclude a healthy compatible node. Missing/future timestamps have unknown
(zero) diagnostic age and `telemetry_fresh=false`; `telemetry_present`
distinguishes missing data.

This is separate from capacity freshness and NPU model-fit budgets. Scheduling
cannot guarantee current physical memory from an old snapshot, prevent model
inventory changes after planning, or atomically observe external work on another
machine. Worker admission and reservation revalidation remain necessary.

## Structured diagnostics

Set the existing orchestrator `log_level` to `debug` to enable candidate/filter
and reservation events. The existing info-level assignment message is now a
structured `scheduler decision` winner event.

[diagnostics.go](../internal/scheduler/diagnostics.go) emits:

- Stage, fixed reason code, and ranking type: registry/protocol/retry filters,
  additive planning, pipeline placement, or reservation.
- Pseudonymous node/model references: truncated SHA-256 fingerprints, never raw
  names or addresses. These support correlation, not strong anonymization
  against guessing known identifiers.
- Health, reported or simulated queue pressure, limit and available capacity,
  effective pipeline layer limit, and the requirement for a real reservation.
- Ready/compatible/resource-eligible model counts when eligibility was evaluated,
  plus an internal accelerator-issue mask. `eligibility_evaluated=false` means
  zero counts are not a fresh inventory assessment.
- Every additive bonus/penalty, final score, utilization freshness/age, and
  estimated completion time. Only `ranking=additive` has an additive factor sum;
  pipeline ordering is layer capacity first, then fast score.
- A process-local `decision_seq` groups scoring candidates with their winner
  when debug logging is enabled.

Reasons include `unhealthy`, `required_capability`, `engine_unready`,
`model_requirements`, `accelerator_memory_insufficient`, `model_filter`,
`protocol_incompatible`, `already_tried`, `circuit_open`, `node_capacity_wait`,
`global_capacity_wait`, and `reserved`. Pipeline/shard events report incomplete
placement, unready members, waits, and successful reservations. At reservation
stages with an existing gate, active/available values reflect local plus
external occupancy, rather than only the last advertised counters.

No prompt, response, task description, expected output, role text, capability
names, credentials, or raw model/node names are included in these new events.
`ScoreFactors` is deliberately not serialized wholesale because its capability
slice can contain operator-provided strings. Existing HTTP/SSE event contracts
are unchanged. Keep logs within the cluster's operational trust boundary.

Disabled candidate diagnostics do not hash identifiers or construct attributes.
Enabled diagnostics deliberately do extra eligibility scans and JSON work;
enable them temporarily during investigations, not for overhead comparisons.

## Reproducing the evaluation

Run from the repository root:

```powershell
go run ./internal/scheduler/cmd/schedbench -seed 17 -requests 256 -iterations 100000 -repetitions 5 -output docs\scheduler-evaluation-seed17.json
go run ./internal/scheduler/cmd/schedbench -seed 29 -requests 256 -iterations 100000 -repetitions 5 -output docs\scheduler-evaluation-seed29.json
```

Omit `-output` for JSON on stdout. No model download, inference server, hardware
probe, recorded conversation, or dependency installation is needed.

The [evaluation package](../internal/scheduler/evaluation/) supplies:

- Round-robin: scan from a rotating cursor until the first eligible candidate.
- Least-loaded: lowest `(active + queued) / concurrency limit`, with stable ties.
- Capability-aware: hardware plus required/preferred capability and GPU bonuses,
  intentionally without load/history adaptation.
- Existing scorer: production preparation and `ScorePreparedValue`, with the
  default weights. A regression test compares its decisions directly with
  production planning, including queued and full nodes.

All four use the same production hard-eligibility and model-preparation
predicates. The baselines compare node ranking, not independent model-selection
algorithms. Round-robin can examine fewer candidates; counters record only those
actually examined and found eligible.

### Trace and simulation model

Each seed produces nine scenarios with eight nodes and 256 requests:
homogeneous, heterogeneous, uneven model availability, missing/stale/future
telemetry, long-running requests, bursts, simultaneous arrivals, node failures,
and insufficient NPU memory.

Basic nodes have two slots and 80 synthetic tokens/second. Heterogeneous cases
use 20/40/80/160 tokens/second and one to three slots, with independent prefill
delays. Output lengths are 24-103 tokens; every tenth long-running request is
20 times larger. Bursts contain 32 arrivals every two seconds; concurrent groups
contain eight arrivals every 600 ms. Failure cases interrupt nodes 3 and 7 for
five-second windows. Normal heartbeats preserve each scenario's specified
telemetry-age offset.

The deterministic discrete-event simulator:

- Orders equal-time arrivals by trace index and uses per-node FIFO waits and
  atomic virtual reservations. Full nodes may receive plans, never extra slots.
- Releases interrupted/expired work, skips abandoned queue entries, and retries
  interrupted work after 100 ms on an untried node, up to five attempts. Every
  synthetic node has one model, so node exclusion equals node/model exclusion.
- Uses a 30-second request deadline. Failure/recovery/deadline events precede
  completion and arrivals at the same instant; completion exactly at the
  deadline is therefore a timeout.
- Records all terminal outcomes, total queue wait across attempts, successful
  TTFT and end-to-end distributions, throughput, and per-node utilization.
  Percentiles use nearest rank; utilization imbalance is population standard
  deviation. Its denominator includes downtime and ineligible nodes.

TTFT is final successful start plus synthetic prefill minus original arrival;
service duration adds output tokens divided by synthetic service rate.
Successful-only latency distributions must always be read with failure rate:
dropping slow work can otherwise appear to improve the tail.

This is not a replica of the complete production workpool, network, breaker,
hedging, sharding, or streaming retry lifecycle. Simultaneous virtual arrivals
test ranking/load accounting, not OS thread contention. The separate parallel
microbenchmark exercises immutable snapshots with worker-local policy state;
its amortized wall ns/op is throughput, not individual request latency.

### CPU and allocation measurements

Each policy/scenario has five real ranking measurements, isolated from simulated
service time. Each sample records actual iterations, wall ns/decision, process
user-plus-kernel CPU ns/decision, allocation count, and allocated bytes.
Process CPU uses Windows `GetProcessTimes` or Linux/macOS `getrusage`, not a
wall-time proxy. Batches double until at least 100 ms of work and a nonzero CPU
interval are observed. Windows CPU accounting remains quantized; compare repeated
samples and avoid interpreting sub-tick differences as improvements.

These samples include production model preparation and ranking, with effective
capabilities already cached. They exclude snapshot construction, graph building,
experience lookup, logging, reservation waits, network I/O, and inference.
Runtime allocation counters can include small background-runtime noise.

For standard Go benchmarks, including complete assignment and diagnostics:

```sh
go test -json ./internal/scheduler ./internal/scheduler/evaluation ./internal/orchestrator -run '^$' -bench 'BenchmarkAssignWithConfigNodes80|BenchmarkAssignWithExperienceNodes32ManyModels|BenchmarkDecisionDiagnostics|BenchmarkPolicies($|Parallel)|BenchmarkCapacityReservationDiagnostics' -benchmem -count=3
```

Use the same toolchain, CPU, power state, `GOMAXPROCS`, workload parameters, and
logging sink for comparisons. The default command does not rewrite global
configuration or system settings. CPU accounting is supported on Windows,
Linux, and macOS; other platforms return an explicit measurement error.

No real inference was measured. There is no real model, context window,
engine-specific throughput, or warm/cold inference claim in these results.
The output records synthetic service/prefill parameters, timeout, outages,
scoring configuration, Go/OS/architecture/parallelism metadata, and a SHA-256
fingerprint of each complete generated cluster/trace.

## Recorded findings

Raw data:

- [Seed 17 results](./scheduler-evaluation-seed17.json).
- [Seed 29 results](./scheduler-evaluation-seed29.json).
- [Go microbenchmark JSON events](./scheduler-microbenchmarks.jsonl).

Measurements used Go 1.27.3, Windows amd64, AMD Ryzen AI 9 HX 370, and
`GOMAXPROCS=24`. There are 72 policy/scenario outcomes and 360 CPU samples.
Every calibrated CPU sample is positive and at least 100 ms long; no invalid
execution starts were observed.

### Ranking overhead

For the homogeneous eight-node snapshot, standard benchmark wall time was
approximately 75 ns for round-robin, 631-636 ns for least-loaded/capability-aware,
and 970-996 ns for the existing scorer. Round-robin examined one eligible
candidate; the others examined eight. All reported zero allocations for these
fixtures. The existing scorer took about 9.6 microseconds at 80 nodes and
96 microseconds at 800 nodes: linear scanning, not a measured bottleneck here.

Homogeneous median process CPU was about 78 ns/decision for round-robin,
625-703 ns for the simple full-scan policies, and 938 ns for the existing scorer.
These CPU estimates are quantized, unlike the Go wall ns/op measurements.
Zero-allocation ranking results do not imply allocation-free complete assignment
or arbitrary preferred-capability inputs.

### Workload tradeoffs

- Homogeneous and stale-telemetry cases: round-robin, least-loaded, and existing
  scoring all completed every request with roughly 1.31-second successful p99
  end-to-end latency. Capability-only placement overloaded a stable favorite.
- Bursts: least-loaded improved successful p99 from 21.75 to 15.45 seconds
  (seed 17), and 18.65 to 14.00 seconds (seed 29). Throughput rose from 7.55 to
  8.45 and 7.18 to 8.97 requests/second respectively; both policies had zero
  failures. However, least-loaded had higher mean queue wait in both seeds and
  worse p95 TTFT: 8.55 versus 6.95 seconds, and 7.50 versus 6.05 seconds.
- Long-running work: existing scoring failed 26/25 requests in the two seeds,
  versus 31/26 for least-loaded and 78/75 for round-robin. No candidate policy
  eliminates the intentionally overloaded workload's deadline failures.
- Node failures: existing scoring had higher successful throughput than
  least-loaded in both seeds, with no failures for either. Tail latency was
  mixed: existing scoring had p99 7.40 versus 7.47 seconds in seed 17, but
  10.88 versus 8.15 seconds in seed 29. Utilization standard deviation favored
  existing scoring in both cases.
- Uneven models and resource limits: no policy executed an incompatible
  specialty model or an NPU model exceeding the reported budget. Existing and
  least-loaded outcomes matched on these traces; round-robin was not uniformly
  as efficient.

Least-loaded is a useful future comparison for burst handling, not sufficient
evidence to replace quality/history/topology-aware production scoring. These
synthetic rates do not establish a universal winner or real-model latency gains.

### Diagnostics overhead

The disabled event helper measured about 9.7 ns with zero allocations; enabled
JSON emission measured 4.29-4.48 microseconds and 23 allocations per event.
The full single-node reserve/release benchmark measured about 0.45 microseconds
with debug off and 4.69-4.78 microseconds with debug on. Enabled candidate
explanation scans add work beyond the emission-only measurement.

Before/after complete-assignment samples showed allocations dropping from
156 to 33 for the 80-node fixture and 163 to 40 for the 32-node/multi-model
fixture, due to replacing the formatted assignment log with structured
attributes. Timing samples overlap; no portable speedup is claimed.

## Regression and compatibility evidence

New tests cover privacy-safe reason/score output, debug-on/off decision parity,
telemetry boundary timestamps, eligibility explanations, all-policy hard-filter
parity, deterministic trace/replay behavior, complete terminal accounting,
hand-calculated queue/TTFT/latency values, deadline/failure/recovery cleanup,
capacity limits, NPU memory rejection, and machine-readable report shape.

Existing tests for full-node queued planning, model/capability rejection, NPU
vendor/quantization/context constraints, concurrent reservation, cancellation
rollback, transparent routing, and contract fixtures remain valid.

Validation passed:

- `go build ./...`.
- `go vet ./...`.
- `go test -p=1 -count=1 -timeout=120s ./...` on Windows.
- `go test -race -count=1 -p=1 -timeout=180s ./...` in WSL Linux.
- `node --test web/app_test.js`: 58 passed.
- Configured golangci-lint v2.15.0: zero issues.
- `go generate ./contracts ./internal/scheduler`: no fixture drift.
- `go mod tidy -diff`: no dependency drift.
- CGO-disabled Darwin arm64 and Linux amd64/`ubuntu` cross-builds.

Native Windows race tests were not run in this phase; Linux race success is
not presented as native Windows race evidence. No prior work was reverted and
no commits were created.
