# Performance benchmarks

The Go benchmark fixtures exercise scheduler assignment at 10, 100, and 1,000
node-model entries; simulated dispatch and API/router streaming; 100 concurrent
router streams; SSE snapshot generation; 10K-record memory and task sync merges;
five-node idle sync traffic; and memory/task bbolt writes. Sync and API fixtures
use in-memory stores and simulated peers/engines; bbolt benchmarks use temporary
databases.

Run all repository benchmarks with allocation reporting:

```sh
go test ./internal/... -run '^$' -bench . -benchmem
```

Measure the five-node idle sync exchange and its estimated bytes/sec at 15- and
30-second memory heartbeat intervals:

```sh
go test ./internal/nodeagent -run '^$' -bench '^BenchmarkIdleSyncTraffic5Node$' -benchtime=1x -benchmem
```

The benchmark reports per-cycle memory/task bytes and both bytes/sec estimates.
Its HTTP byte count includes headers and bodies, but excludes TLS and TCP/IP
framing. Before/after measurements and topology assumptions are in
[the sync traffic report](../docs/sync-traffic.md).

For more stable comparisons, collect repeated samples before and after a change
on the same machine with the same Go version and power settings:

```sh
go test ./internal/... -run '^$' -bench . -benchmem -count=10 > before.txt
# Make the change, then collect the second sample:
go test ./internal/... -run '^$' -bench . -benchmem -count=10 > after.txt
benchstat before.txt after.txt
```

Install `benchstat` separately with
`go install golang.org/x/perf/cmd/benchstat@latest`; it is not a module
dependency. Compare `ns/op`, `B/op`, and `allocs/op`, and avoid interpreting
results from different hosts as a code-only change. The concurrent router
benchmark measures each batch of 100 streams and reports both `streams/op` and
`streams/s`. `BenchmarkNodeClientSharedTransport` alternates requests through
node-client roles, drains and closes each response, and reports the total
connections accepted by its local server to verify cross-client reuse.

See [the recorded baseline](../docs/perf-baseline.md) for the initial
measurements and their workload notes.
