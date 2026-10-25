# Development

Use the repository root as the working directory. The required Go toolchain
version is specified in [go.mod](../go.mod); the web tests use Node.js 22.
Commands below are available in both PowerShell and bash.

## Build and test

PowerShell:

```powershell
go build ./...
go vet ./...
go test ./...
node --test web/app_test.js
```

bash:

```sh
go build ./...
go vet ./...
go test ./...
node --test web/app_test.js
```

The Go suite includes contract, integration, and deterministic simulated-engine
tests. The race detector needs cgo and a working C compiler. Run it with the
helper scripts, which accept optional packages and `go test` flags (default
`./...`):

```sh
./build/test-race.sh            # Linux/macOS
```

```powershell
.\build\test-race.ps1           # Windows (PowerShell 5.1 or 7)
.\build\test-race.ps1 -CheckOnly
.\build\test-race.ps1 ./internal/hub -count=1
```


The managed sharding process test builds three small Go fake binaries and
exercises the real planner, process supervisor, health sweeps, router, and
rebuild without GPUs:

```sh
go test ./internal/integration -run TestManagedShardProcessLifecycle -count=1 -v
```

It uses fixture node HTTP/relay adapters, not a real tensor protocol or an
authentication test. llama.cpp lifecycle runs on Windows and Linux; managed
vLLM lifecycle runs only on Linux (no native Windows support). CI separately
builds these binaries and the S3 supervisor fake because Go's `./...` excludes
`testdata` packages. Hardware compatibility and throughput require the
[real-machine checklist](./hardware-validation.md).

### Native Windows race tests

Native Windows race tests pass with Go 1.27.3 and MSYS2 MinGW-w64 GCC. The
common failure is not a missing compiler but DLL shadowing: GCC's `cc1.exe`
loads runtime DLLs (`libgcc_s_seh-1.dll`, `libgmp-10.dll`,
`libwinpthread-1.dll`, `zlib1.dll`, `libzstd.dll`, …) through `PATH`. If another
application's directory that ships older copies, such as
`C:\Program Files\Multipass\bin`, comes earlier in `PATH`, `cc1.exe` exits with
`0xC0000139` (entry point not found) and Go reports only
`runtime/cgo: ... exit status 2` or a C compiler failure.

`build\test-race.ps1` detects this. It locates the compiler from `go env CC` or
common MSYS2 paths, compiles and runs a small C program, and, if the inherited
`PATH` fails, puts the compiler directory first for that run only. It lists
the directories that shadow compiler DLLs. To make plain
`go test -race` work, move the compiler's `bin` directory (for example
`C:\msys64\mingw64\bin`) ahead of those directories in your user or system
`PATH`. Windows searches the machine `PATH` before the user `PATH`.

Older Windows configurations also failed before tests started because
ThreadSanitizer could not allocate shadow memory (error 87); treat that as an
environment limitation, not a pass. The CI race job on Linux remains the
release gate. Ubuntu 24.04 WSL has also had an intermittent node-agent test
failure without a race report; use the Linux CI result to separate an
environment flake from a race finding.

### Race tests in WSL

For a user-local WSL installation, run the Linux toolchain rather than the
Windows executable inherited through `PATH`. The local validation environment
uses checksum-verified Go 1.27.3 at `~/.local/go1.27.3/bin/go`, without changing
system packages or shell profiles. From the repository root in PowerShell:

```powershell
wsl.exe -d Ubuntu --cd "$((Get-Location).Path)" -- bash -lc '~/.local/go1.27.3/bin/go test -race ./...'
```

Substitute your installation path or set the Linux `PATH` to your installation.

## Background work and performance feedback

The orchestrator and node own background work through `internal/background.Group`.
Start workers with `Go`, pass their context through waits and network requests,
and call `Stop` before closing stores or engines. `Stop` cancels and joins the
workers; it must not be called from one of those workers. Periodic callbacks run
serially through `RunEvery`; adaptive sync keeps its watermark-aware debounce
using `syncinterval.WaitDebouncedContext`. Legacy hardware/engine probe interfaces
still finish under their own bounds while shutdown waits for their worker.

`hub.Publisher.Request` is nonblocking and coalesces snapshot requests. Builders
run serially, only with subscribers; byte-identical payloads are suppressed.
Subscriber generations ensure a new client receives the current state even when
it has not changed. Full queues prevent deduplication from permanently suppressing
the final state of a slow client.

Profile uncached package timings before optimizing test waits:

```sh
go test -json -count=1 ./internal/nodeagent ./internal/orchestrator > test-timings.json
```

Fixture isolation removed default-database contention, optional host inventory
probes, and non-loopback placeholder network waits without dropping assertions.
Measured on the same Windows host, node-agent time fell from 77.391s to 4.379s
(94.3%) and orchestrator time from 64.247s to 17.328s (73.0%). These are measured
samples, not portable timing thresholds; real process integration coverage remains.

Benchmark actual snapshot construction separately from the idle/queued fast path:

```sh
go test -run '^$' -bench 'BenchmarkBuildSnapshot|BenchmarkPublishSnapshotRequests|BenchmarkPublisherRequest' -benchmem ./internal/orchestrator ./internal/hub
```

The [scheduled benchmark workflow](../.github/workflows/benchmarks.yml) runs weekly
and manually. It records five samples with fixed `GOMAXPROCS`, environment metadata,
JSON events, and benchstat-compatible text as 90-day artifacts. Compare like-for-like
runner/toolchain samples before claiming a regression or speedup; the workflow does
not automatically fail on timing variance.

## Contracts and fixtures

Check generated contract fixtures with:

```sh
go generate ./contracts ./internal/scheduler
git diff --exit-code -- contracts/fixtures
go test ./contracts
```

Package characterization goldens have an explicit update flag; update them
only for an intentional, reviewed change:

```sh
go test ./internal/orchestrator ./internal/scheduler ./internal/taskmgr ./internal/config -args -update
```

Review all generated fixture changes. Do not regenerate a golden merely to
make a failing test pass.

## Release builds

The local cross-build scripts are available for both shells:

```powershell
.\build\build.ps1
```

```sh
./build/build.sh
```

They produce Windows and Linux amd64 binaries under `dist/`. Tagged releases
use [GoReleaser](../.goreleaser.yaml) and the
[release workflow](../.github/workflows/release.yml) to publish platform
archives and SHA-256 checksums. Before a release, update
`internal/version/version.go` and its version test, move the completed
`Unreleased` changelog entries to a dated release section, run the required
checks and cross-build script, then push a matching `v<version>` tag. The
GoReleaser workflow embeds the tag version in published artifacts.

## Optional user services

Example service definitions are available for
[systemd](../packaging/systemd/maistr0.service) and
[launchd](../packaging/launchd/com.maistr0.agent.plist). Review each file and
set the executable path for the target host before enabling it. A systemd user
service can be enabled with `systemctl --user daemon-reload` followed by
`systemctl --user enable --now maistr0`. On macOS, place the reviewed plist in
`~/Library/LaunchAgents/` and load it with:

```sh
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.maistr0.agent.plist
```

## CI

### Rapid iteration releases

The source release number is now `0.0.24.3`. Increment the fourth numeric
component for rapid iterations (`0.0.24.4`, ..., `0.0.24.10`); use the next
third component for a new patch series. This is a numeric four-component
release convention, not SemVer. Peer manifests accept both three and four
components and compare components numerically, with an omitted fourth
component treated as zero.

The release workflow detects four-component tags and uses the repository
build scripts plus GitHub CLI packaging; ordinary three-component tags
continue through GoReleaser. The tag must match the source version.
Do not publish a tag for a different source version.

On Windows, build without interrupting a running installed executable:

```powershell
.\build\build.ps1 -WindowsBinary maistr0-update.exe
```

Otherwise run the build script with its default output name. Both build
scripts now generate checked `.peer.json` metadata for Windows, Linux, and
Ubuntu outputs, so cross-platform peer publishers do not depend on manually
created manifests. Binaries and metadata remain ignored build products.

GitHub Actions checks contract drift, tests on Ubuntu and Windows, runs race
tests on Linux, runs `go vet` and `govulncheck`, builds the application, tests
the dashboard, and publishes a golangci-lint result and Go coverage report.
See [CONTRIBUTING.md](../CONTRIBUTING.md) for review expectations.
