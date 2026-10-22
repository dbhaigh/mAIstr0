# Performance baseline

Collected 2026-10-21 on Windows/amd64 with Go 1.27.0 on an AMD Ryzen AI 9 HX
370. Values below are medians from five benchmark samples using
`-benchtime=200ms -benchmem -count=5`; timing and allocation figures are
per-operation unless noted.

The run covered scheduler, orchestrator, memory, and task-manager benchmarks.
The simulated engine and in-memory stores were used for execution, streaming,
and sync fixtures. HTTP paths use local `httptest` workers. The bbolt tests use
temporary databases, with database setup outside the timed loops.

## Measurements

- Scheduler `AssignWithExperience`, one model per node: 10 node-model entries
  took 7.65 us / 8.1 KB / 30 allocs; 100 took 87.2 us / 68.3 KB / 120 allocs;
  1,000 took 703 us / 665 KB / 1,022 allocs.
- `executeSubtask` against the simulated worker: 100 us / 9.9 KB / 112 allocs.
- Full `dispatchWithContext` with in-memory task and memory stores: 248 us /
  49.7 KB / 374 allocs.
- Transparent router, batches of 100 concurrent streams: 17.2 ms per batch /
  7.9 MB / 31,592 allocs, with a median throughput of 5,817 streams/s across
  samples. Individual sample times ranged from 16.5 to 31.5 ms.
- OpenAI-compatible API streaming through the simulated worker: 365 us /
  37.7 KB / 311 allocs.
- SSE cluster snapshot generation and JSON encoding for 100 nodes, four models
  per node, and 50 tasks: 1.01 ms / 569 KB / 1,626 allocs. No dashboard
  subscribers were connected, so this does not measure socket writes or
  per-subscriber fan-out.
- In-memory memory-history merge: 10K experiences took 18.6 ms / 25.5 MB /
  50,146 allocs. The existing 100K stress case took 190.5 ms / 273 MB /
  500,633 allocs.
- In-memory task sync merge: 10K tasks took 9.21 ms / 9.81 MB / 70,126 allocs.
- bbolt memory experience record: 2.81 ms / 26.7 KB / 197 allocs.
- bbolt task save: 1.28 ms / 7.9 KB / 54 allocs; checkpoint save: 1.45 ms /
  8.9 KB / 57 allocs.

## Estimated top five bottlenecks

These rankings estimate workload impact from measured cost and operation
frequency, not from production profiling or traffic traces.

1. **Memory sync merge allocation bursts.** The requested 10K merge allocates
   about 25.5 MB and takes 18.6 ms; the 100K stress case grows to 273 MB and
   190.5 ms. Large snapshots can create noticeable latency and GC pressure.
2. **Router under high concurrency.** A 100-stream batch takes a median 17.2 ms
   and allocates about 7.9 MB. The result is throughput-oriented, not a
   per-stream latency or tail-latency measurement.
3. **Task sync merge.** Merging 10K tasks costs about 9.2 ms, 9.8 MB, and
   70K allocations per snapshot.
4. **Synchronous bbolt writes.** Individual measured writes take 1.3–2.8 ms.
   Task creation, attempts, checkpoints, and learning can perform multiple
   writes along a user-visible workflow, so the cost can accumulate.
5. **Large-cluster scheduler assignment.** At 1,000 node-model entries, one
   assignment takes about 703 us and allocates 665 KB. The measured cost grows
   with candidate count; the 100-entry case is about 87 us.

The 100-node SSE snapshot is a nearby tracking item at 1.01 ms and 569 KB per
generation. It runs periodically and was measured without connected clients;
measure connected-client fan-out separately before treating it as a production
limit.

## Measured optimization comparisons

The optimization samples below use 10 runs of each relevant benchmark with
`-benchtime=200ms -benchmem`; before/after results were compared with
`benchstat`. The concurrent router baseline was notably noisy, so treat its
latency change as directional.

| Benchmark | Time before → after | B/op before → after | Allocs/op before → after |
|---|---|---|---|
| Scheduler, 1,000 nodes | 815.2 → 702.8 us (-13.8%, p<0.001) | 649.4 → 665.8 KiB (+2.5%) | 1,022 → 24 (-97.7%) |
| Simulated `executeSubtask` | 130.7 → 135.2 us (not significant) | 9.577 → 9.355 KiB (-2.3%) | 112 → 104 (-7.1%) |
| Full simulated dispatch | 396.8 → 245.5 us (-38.1%, p<0.001) | 49.32 → 48.27 KiB (-2.1%) | 374 → 366 (-2.1%) |
| Router, 100-stream batch | 49.36 → 28.23 ms (-42.8%, p=0.043) | 7.585 → 4.140 MiB (-45.4%) | 31,770 → 30,890 (-2.8%) |
| OpenAI-compatible streaming | 414.8 → 206.3 us (-50.3%, p<0.001) | 36.52 → 35.33 KiB (-3.3%) | 310 → 290 (-6.5%) |
| Shared node-client transport | 78.28 → 87.52 us (not significant) | 5.072 → 5.060 KiB (not significant) | 63 → 63 |

The scheduler change's main gain is fewer per-node allocations; its measured
bytes per operation rise slightly. The typed dispatch payload reduces
allocations, but the direct execution benchmark did not show a significant
latency win. The router buffer pool and direct compatibility dispatch reduced
bytes and allocations, while the shared transport reduced connections accepted
by the local test server from two to one; that transport fixture did not show a
throughput improvement.

## Reproduction

See [benchmarks/README.md](../benchmarks/README.md) for commands to rerun the
benchmarks and compare repeated before/after samples with `benchstat`.
