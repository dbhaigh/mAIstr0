# Test health and determinism

## Changes

- Discovery listener tests bind to `127.0.0.1:0` and use the listener's assigned
  address. Beacon tests inject interface routes and use an ephemeral UDP socket.
  Production discovery continues to use UDP port 7999 and its existing retry
  intervals. The mutable package-level interface-address test hook was replaced
  with injected providers.
- Polling waits now use channels, tickers, or `testing/synctest` virtual time;
  pairing-window checks use an injected clock. The fake engine's deliberate
  startup/exit delays remain configurable.
- Added `t.Parallel()` to isolated discovery, syncinterval, hub, workpool,
  heartbeat, snapshot-publishing, capacity-gate, and task-cache tests. The
  workpool goroutine-count test and tests that call `t.Setenv` remain serial.
- CI now runs `go test -short -shuffle=on -count=1 -timeout=120s ./...`.

## Inventory

### Fixed ports

No test listener still binds a fixed port. Discovery and beacon listener tests
use ephemeral addresses. Fixed localhost ports in tests are request fixtures,
expected configuration values, or protocol examples rather than bound
listeners. Production discovery's port 7999 is unchanged.

### Sleeps

No real test sleep exceeds 100 ms. Remaining real sleeps are small simulated
upstream delays used to exercise timeout behavior:

- 50 ms in [`api_worker_install_test.go`](../internal/orchestrator/api_worker_install_test.go),
  [`managed_engine_client_test.go`](../internal/orchestrator/managed_engine_client_test.go),
  and [`agent_test.go`](../internal/nodeagent/agent_test.go).
- 30 ms in the two upstream-delay cases in
  [`peerchat_test.go`](../internal/nodeagent/peerchat_test.go).

The 20 ms and 40 ms sleeps in
[`adaptive_test.go`](../internal/syncinterval/adaptive_test.go) run inside
`testing/synctest` and advance virtual time. The 1 ms sleep in
[`performance_benchmark_test.go`](../internal/orchestrator/performance_benchmark_test.go)
is benchmark pacing. The fake engine uses configured 60 ms startup and 100 ms
exit delays for lifecycle tests.

### Wall-clock checks

No tests assert that a measured elapsed duration falls within a narrow timing
window. The remaining `time.Until` assertions verify configured request
deadlines: the 14–15 minute shard-proxy budget in
[`shard_proxy_timeout_test.go`](../internal/nodeagent/shard_proxy_timeout_test.go)
and the 29–30 minute shard-task budget in
[`shard_tasks_test.go`](../internal/orchestrator/shard_tasks_test.go).
Other short deadlines bound polling or cancellation and do not assert elapsed
wall time.

### Process-wide state

Tests using `t.Setenv` are kept non-parallel because environment changes are
process-wide. They are in:

- [`main_test.go`](../cmd/maistr0-bench/main_test.go)
- [`release_test.go`](../internal/integration/release_test.go) and
  [`shard_fixture_test.go`](../internal/integration/shard_fixture_test.go)
- [`agent_test.go`](../internal/nodeagent/agent_test.go)
- [`local_models_test.go`](../internal/nodeagent/supervisor/local_models_test.go),
  [`manager_test.go`](../internal/nodeagent/supervisor/manager_test.go),
  [`lifecycle_characterization_test.go`](../internal/nodeagent/supervisor/lifecycle_characterization_test.go),
  [`model_profile_test.go`](../internal/nodeagent/supervisor/model_profile_test.go),
  and [`readiness_timeout_test.go`](../internal/nodeagent/supervisor/readiness_timeout_test.go)
- [`server_test.go`](../internal/orchestrator/server_test.go)

Golden-update flags and package-level routing fixtures are read-only during
normal test runs. Discovery no longer has a mutable global interface-address
hook.

## Shuffled and race results

- `go test -shuffle=on -count=5 ./...`: passed on Windows; no order-dependent
  failure observed.
- Linux/WSL `go test -race ./...`: the first full run had one failure in
  `TestManagedShardProcessLifecycle/vllm-mp`, where the managed fake engine
  exited before readiness; there was no race report. The subtest passed alone
  under `-race`, and an immediate second full Linux race run passed. The test
  was not quarantined; this one-off process-start failure is recorded here for
  follow-up if it recurs.

## Package timing comparison

Captured with `go test -json -count=1 ./...` before and after the test-health
changes. Values are package-reported elapsed seconds; the sum is not suite wall
time. These are single-run observations, so small changes may be measurement
noise.

| Package | Before (s) | After (s) |
|---|---:|---:|
| `cmd/maistr0` | 3.180 | 3.106 |
| `cmd/maistr0-bench` | 2.184 | 1.974 |
| `contracts` | 1.549 | 1.661 |
| `internal/agent` | 0.503 | 0.817 |
| `internal/apperr` | 0.720 | 0.543 |
| `internal/background` | 0.581 | 0.424 |
| `internal/benchrun` | 1.191 | 1.857 |
| `internal/closeutil` | 0.543 | 0.563 |
| `internal/cluster` | 0.909 | 0.876 |
| `internal/config` | 1.144 | 1.309 |
| `internal/consensus` | 4.522 | 3.878 |
| `internal/deepseek` | 1.499 | 1.686 |
| `internal/discovery` | 1.911 | 0.618 |
| `internal/engine` | 1.510 | 1.611 |
| `internal/hardware` | 4.976 | 2.959 |
| `internal/httpx` | 1.460 | 1.072 |
| `internal/hub` | 1.703 | 1.493 |
| `internal/inferroute` | 0.782 | 0.547 |
| `internal/integration` | 38.471 | 34.908 |
| `internal/logging` | 0.732 | 0.558 |
| `internal/memory` | 4.664 | 4.316 |
| `internal/nodeagent` | 6.514 | 5.704 |
| `internal/nodeagent/supervisor` | 37.060 | 34.415 |
| `internal/orchestrator` | 22.955 | 21.528 |
| `internal/peeridentity` | 2.401 | 2.578 |
| `internal/piagent` | 2.004 | 1.539 |
| `internal/scheduler` | 1.291 | 1.596 |
| `internal/scheduler/evaluation` | 3.438 | 3.705 |
| `internal/scheduler/score` | 0.992 | 1.194 |
| `internal/selfupdate` | 9.645 | 8.059 |
| `internal/shard` | 0.978 | 1.082 |
| `internal/syncinterval` | 0.374 | 0.295 |
| `internal/syncio` | 0.799 | 0.754 |
| `internal/syncpage` | 0.276 | 0.307 |
| `internal/taskmgr` | 2.099 | 1.815 |
| `internal/testutil` | 0.273 | 0.324 |
| `internal/version` | 0.230 | 0.306 |
| `internal/workpool` | 0.361 | 0.388 |
| `web` | 0.983 | 0.823 |
| **Package elapsed sum** | **167.407** | **153.188** |

The package elapsed sum decreased by 8.5%. The discovery package had the
largest targeted improvement; several small package increases show why the
single-run aggregate should be treated as indicative, not a benchmark claim.

## Validation

| Command | Result |
|---|---|
| `go build ./...` | Passed |
| `go vet ./...` | Passed |
| `go test ./...` | Passed |
| `go test -shuffle=on -count=5 ./...` | Passed on Windows |
| `node --test web/app_test.js` | Passed: 61 tests |
| `wsl -d Ubuntu -- bash -lc 'cd /mnt/c/Users/ahaigh/source/repos/mAIstr0 && ~/.local/go1.27.3/bin/go test -race ./...'` | Passed on the second full run; see the transient failure note above |
| `python scripts\check_doc_links.py` | Passed after this report was added |
| `git diff --check` | Passed |
