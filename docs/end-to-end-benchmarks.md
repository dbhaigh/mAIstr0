# End-to-end benchmarks and release acceptance

## What is measured

[The recorded Linux run](./end-to-end-benchmarks.json) compares an engine-direct
HTTP endpoint, the real transparent router with one worker, and that router
with three heterogeneous workers. All run on the same host using fake engine
facades, not full node agents, mTLS, GPUs, real models, or subprocess inference.
The small/large names represent different modeled service delays, not model
parameter counts. These results do **not** establish a real-inference speedup.

The 54 results cover two service profiles, 32/512 input words, concurrency 1/4,
streaming/non-streaming, modeled cold starts, and an injected transient failure.
Warm cases contain 100 requests each; cold cases contain one, and fault cases
three. Workers have 2/1/3 slots and base service times 2/4/1 ms; the large profile
multiplies base service by four, context adds one millisecond per 64 words,
network delay is 2 ms, streaming adds four 1 ms intervals, and cold load adds
30 ms. Timer precision, scheduling, logging, HTTP, and fixture overhead remain
in measured latency. Direct and single-router workloads use the same two-slot
backend; multi-node adds resources and is not a scheduler-policy comparison.

Each result retains request outcomes, raw elapsed/first-byte/first-token samples,
completion throughput, successful-request distributions, process allocation
deltas, queue/admission and dispatch setup timing, engine-service wall time,
scheduler wall time, retries, recovery, and busy-slot fractions. Busy-slot
fraction is occupied engine slots divided by capacity and elapsed time, **not
physical CPU/GPU utilization**. Engine service excludes modeled network and
engine queue wait. Client latency minus service is not purely scheduler overhead.
Recovery measures time from the start of the controlled sequential fault case
to its first successful completion.

Quantiles use nearest rank: p50 needs 20 successes, p95 needs 100, and p99 needs
1,000. Smaller samples produce JSON null, not invented tails. Non-streaming
TTFT is null. Streaming TTFT means the first nonempty OpenAI `delta.content`,
not response headers or a role-only event; a valid stream must finish `[DONE]`.
Failed requests remain in raw samples and error totals but not successful
latency distributions. Direct and single-router fault cases each have one
expected error; multi-router retries another worker and has none. This retry
semantics difference is intentional and must not be hidden in throughput claims.

In this single recorded Linux/amd64 Go 1.27.3 run, the short small non-streaming
concurrency-one workload measured direct/single/multi p50 completion of
5.23/6.02/6.13 ms and throughput 189.1/164.2/162.4 requests/s: routing has a cost.
For the long large streaming concurrency-four workload, the corresponding p95
completion was 47.46/77.76/35.37 ms and throughput 86.9/77.1/144.7 requests/s.
The multi-node improvement uses six slots rather than two; the single-router
tail regression is retained, not hidden. These are observations from one
synthetic run, not repeatability-backed real-inference performance claims.
The sequential injected failure recovered to a first success in
8.10/113.84/114.61 ms; multi-router used one retry and avoided a client error,
whereas direct and single-router each returned one error.

Missing server instrumentation is null. Engine-direct scheduler observations
are null, not measured zero. Process allocations include the harness and
in-process fake servers; they are not isolated scheduler allocations.
Physical CPU/GPU/RSS, actual tokens per second, and historical migration are
not measured here. Use [scheduler evaluation](./scheduler-evaluation.md) and
[performance profiles](./performance-evaluation.md) for separately scoped
scheduler CPU/allocations, workflow, inventory, dashboard, memory/sync, and shard
measurements; do not merge them into one end-to-end hardware claim.

## Reproduce deterministic workloads

From the repository root, use a **new absolute** report filename:

```powershell
$env:MAISTR0_E2E_REQUESTS = "100"
$env:MAISTR0_E2E_REPORT = Join-Path (Get-Location) "end-to-end-local.json"
$env:MAISTR0_BENCH_REVISION = git rev-parse HEAD
$env:MAISTR0_BENCH_DIRTY = "true" # Set accurately for the checkout.
go test ./internal/orchestrator -run '^TestEndToEndInferenceBenchmarks$' -count=1 -timeout=15m
```

The equivalent bash environment variables work on Linux. Requests per warm case
are bounded to 1..2000. Without these variables, ordinary tests run the same
matrix with three samples and write no report. The weekly
[benchmark workflow](../.github/workflows/benchmarks.yml) records 100 samples.
The recorded environment includes Go, OS/architecture, source version/revision,
dirty state, logical CPUs, and GOMAXPROCS; retain this metadata with results.

## Real engines: explicit operator setup

Copy [the example configuration](../examples/benchmark.json) outside the checkout
and replace its endpoint, model/revision, engine, settings, and hardware
placeholders with accurate values. Use identical engine versions, quantization,
context window, generation settings, models, and backend capacity for comparable
direct/single-router runs. Input lengths are synthetic **words**, not tokenizer
tokens; `output_tokens` is an upper bound, not an observed token count.

```powershell
go run ./cmd/maistr0-bench -config C:\bench\config.json -output C:\bench\result.json
```

On Linux substitute absolute Linux paths. Use trusted HTTPS or trusted loopback;
the runner refuses redirects and has no insecure TLS bypass. An optional
`bearer_token_env` names a protected environment variable, never a literal token.
Reports omit URLs, bearer values, prompts, responses, and upstream error text.
Operator-provided metadata must also be free of secrets.

The runner bounds workers, total requests, input, response size, and per-request
deadline; cancellation terminates active HTTP work. It refuses existing output
files and creates private POSIX files (apply Windows ACLs separately). Recorded
request errors produce a report **and a nonzero exit**, not success-shaped data.
Warmups are excluded from samples. `operator-cold` requires exactly one target,
one workload, one request, concurrency one, no warmup: independently reset the
engine before **each** invocation. The runner does not unload models or restart
services. Real hardware remains an opt-in
[qualification exercise](./hardware-validation.md), not deterministic CI.

## Request diagnostics and dashboard interpretation

Existing bounded router events now correlate `accepted`, `scheduled`, `queued`,
`attempt_started`, `dispatched`, `upstream_response`, `first_upstream_byte`, and
one terminal `completed`/`failed` by generated request/attempt identifiers.
Existing attempt/rejection/finished phases remain. Retries create new attempts;
selection can fail before a queued event. Queued-to-attempt-started includes
capacity/shard admission. Attempt-started-to-dispatched is setup, not remote
network time. First byte is not first token, and a partial stream can still end
failed. Consumers must tolerate unknown phases and eviction: production keeps
only 200 events, not an audit trail. Request-ID generation/unsupported-route
failures before ID assignment cannot have a correlated lifecycle.

Debug lifecycle logs contain generated identifiers, fixed codes, status and
timing, not prompts, credentials, output or raw endpoint/model identifiers.
The authorized event API retains existing operational node/model identifiers.
Transparent routing now contributes to the existing assignment timing counter;
that metric is wall time, not scheduler CPU.

Dashboard resource displays distinguish unavailable utilization from measured
zero and label telemetry freshness. Missing/future/invalid timestamps are not
fresh; the server freshness window is authoritative when available. Started
members of a non-ready shard group are not shown as a healthy ready group.

An isolated Windows five-sample transparent-router microbenchmark after these
changes measured 85.7–107.3 microseconds/op and 236–238 allocations/op; the earlier
baseline was 90.4–110.4 microseconds/op and 250–252 allocations/op. Ranges overlap;
this is an overhead guard, not a claimed speedup. An overlapping-build trial was
discarded. Windows submillisecond clock quantization makes that benchmark's
individual request tail values unsuitable for inference-latency claims.

## Native package acceptance and release gates

PowerShell 7 on Windows or Ubuntu with Go and native archive tooling:

```powershell
.\build\verify-release.ps1 -OutputDirectory C:\bench\new-release-check
```

Use a new native Linux path on Ubuntu. The script builds twice with trimpath and
fixed source version, normalizes archive metadata, compares executable/archive
SHA-256, then extracts and runs the packaged binary under an isolated home.
It checks version/configuration, loopback startup, fingerprint stability, memory
fact persistence, replacement/restart, and diagnostics. Discovery, peer updates,
browser and tray UI are disabled; no live model is downloaded. Windows exercises
crash/restart recovery; Linux requests cooperative shutdown. The replacement is
the same-source second build, **not an old-version upgrade migration**.

Release tags must equal the source version, and both numeric-iteration and
SemVer publication depend on reusable fast CI plus full qualification: contracts/bundle drift, ordinary
and Linux race tests, vet, vulnerability scan, lint, dashboard tests, fake-process
builds, release configuration and native package acceptance. Archives include
security/changelog and machine-readable documentation. CI uploads acceptance
manifests. Local checks are not remote CI or publisher-signature verification;
checksums prove consistency, not a trusted publisher.
See [the current CI gate split and regression policy](./ci-quality-gates.md).

Published application targets include Windows/Linux amd64 and macOS amd64/arm64.
Linux/Ubuntu package names do not imply a separately qualified inference
backend. macOS packages are unsigned and not notarized; upstream accelerator
support is not a hardware qualification claim. Native Windows vLLM remains
unsupported.

## Local verification record

Validated on 2026-10-26 against source revision
`3a8864a2c281340745bd89794ba242785297df45` plus the uncommitted improvement work,
using Go 1.27.3:

- `go build ./...`: passed.
- `go vet ./...`: passed.
- `go test -p=1 -count=1 -timeout=180s ./...`: passed on Windows.
- `.\build\test-race.ps1 -GoTestArgs @('-p=1','-count=1','-timeout=180s','./...')`:
  full Windows race suite passed with the scoped MinGW PATH repair.
- `go test -race -count=1 -p=1 -timeout=180s ./...`: full Linux/WSL race suite
  passed, run separately from the Windows networking suite.
- `node --test web/app_test.js`: 61 passed, including missing/zero/freshness
  and degraded-member checks; two dashboard builds were byte-identical.
- `go generate ./contracts ./internal/scheduler` and `go test ./contracts -count=1`:
  passed; all 66 generated fixture hashes remained unchanged.
- `go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0 run --timeout=5m`:
  passed, zero issues.
- `go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...`: passed, no
  vulnerabilities found.
- `go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.10 -shellcheck= -pyflakes=`:
  passed; external shellcheck/pyflakes execution was disabled.
- `go run github.com/goreleaser/goreleaser/v2@latest check`: passed.
- Native Windows verification script: two executables and two ZIP archives were
  byte-identical; packaged installation/replacement passed. The final isolated
  acceptance fixture also passed under the Windows race detector.
- Equivalent Linux Go/tar commands: two executables and two normalized tar.gz
  archives were byte-identical; packaged installation/replacement passed.
  Linux PowerShell is not installed locally, so the Ubuntu PowerShell wrapper
  itself remains a CI check, not a claimed local pass.

Initial failures in the longer benchmark's fake-node status-refresh seam and
Linux smoke test's colliding binary/config-home paths were corrected in the
fixtures, without weakening production freshness or startup validation.
No real-model/hardware run, historical-release migration, GUI installation,
remote CI run, tag creation, publication or publisher-signature verification
was performed. No dependency manifests were changed.
