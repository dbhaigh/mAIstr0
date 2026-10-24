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
tests. Run the race suite on a supported Linux environment:

```sh
go test -race ./...
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

The native Windows race runtime can fail before tests start because
ThreadSanitizer cannot allocate shadow memory (error 87). See the CI race job
for the supported Linux gate. Ubuntu 24.04 WSL has also had an intermittent
node-agent test failure without a race report; use the Linux CI result to
separate an environment flake from a race finding.

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

GitHub Actions checks contract drift, tests on Ubuntu and Windows, runs race
tests on Linux, runs `go vet` and `govulncheck`, builds the application, tests
the dashboard, and publishes a golangci-lint result and Go coverage report.
See [CONTRIBUTING.md](../CONTRIBUTING.md) for review expectations.
