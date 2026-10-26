# CI quality gates and regression prevention

## Audit and changes

The Phase 9 audit covered every workflow, Go build tags and platform adapters,
Go 1.27.3 requirements, Node/esbuild lockfiles, contract/schema tests, lint,
both release paths, and the native package verifier.

- Contract generation and Go vulnerability scanning previously ran on both
  operating systems. They now run once in shared PR checks. Schema/HTTP contract
  tests still execute inside the Windows and Linux Go suites.
- Standalone fake-binary build jobs duplicated builds already performed by
  supervisor and shard-process tests. Full qualification still builds and runs
  those fakes, including Linux-only vLLM fixtures; the redundant jobs are removed.
- PRs previously ran full process/race/package qualification in the same matrix.
  Fast tests now use Go's explicit short mode. Only supervisor fake-engine
  process tests and the two process integration entry points opt out; full mode
  remains unchanged. Other deterministic HTTP/distributed tests remain in PRs.
- Every-package build/vet, formatting, workflow syntax checks, frontend audit,
  dependency-diff review, immutable action pins and explicit aggregate checks
  replace incomplete or implicit gates.
- Native package acceptance is independent of unit-test jobs. Failed unit tests
  do not prevent packaging from being tested, and neither can publish a release.

The existing Windows/Linux matrix check names are retained. The former separate
lint check is incorporated into **Shared quality checks**. All PR events run
without path filters; there is no `pull_request_target`, `continue-on-error`,
ignored test exit code, or success-shaped missing-baseline fallback.

## Required PR checks

[CI](../.github/workflows/ci.yml) runs on every PR, main push, manual dispatch,
and reusable release call:

- **Shared quality checks**: gofmt; generated fixture drift including unexpected
  files; reproducible dashboard drift; Node dashboard and benchmark-gate tests;
  golangci-lint v2.15.0; govulncheck v1.9.0; npm audit at high severity; actionlint
  v1.7.10. Actionlint checks workflow syntax/expressions/job dependencies, not
  external shellcheck or pyflakes execution.
  Standard-library policy tests also prevent mutable action refs, broad default
  write permissions, inherited release secrets, privileged PR triggers and
  explicit error suppression from being reintroduced.
- On PRs, pinned Dependency Review rejects newly introduced moderate-or-higher
  dependency vulnerabilities and retries incomplete dependency snapshots for up
  to 120 seconds. API/feature availability errors are not ignored. All declared
  dependency changes, including transitive licenses and redistribution duties,
  require explicit human review in the [PR checklist](../.github/PULL_REQUEST_TEMPLATE.md).
  There is no approved legal SPDX allowlist, so CI does not claim legal approval.
- **Test and build (ubuntu-latest/windows-latest)**: `go build ./...`,
  `go vet ./...`, and `go test -short -count=1 -timeout=120s ./...`.
- **Required CI**: always evaluates both upstream job results and fails for
  failure, cancellation or skip. It has no token permissions and no checkout.

**Repository setting still required:** the read-only GitHub inspection on
2026-10-26 reported `main` as unprotected and no repository rulesets. After these
workflows land, configure branch protection/rulesets to require **Required CI**.
Remove obsolete standalone lint/fake-build required contexts if configured
later. Workflow files alone cannot enforce merge protection, and requiring a
new context before its workflow is published would block all merges. No remote
repository settings were changed by this work.

## Full qualification and release acceptance

[Qualification](../.github/workflows/qualification.yml) runs on main pushes,
weekly, manually, and as a release prerequisite; it is not a required long PR
job. Authors changing concurrency/process/storage behavior should run it locally
or manually before merge.

- Windows/Linux full `go test -count=1 -timeout=180s -coverprofile=coverage.out
  -covermode=atomic ./...`, retaining process tests and coverage artifacts.
- Full Linux `go test -race -count=1 -timeout=180s ./...`. Native Windows race
  testing is supported locally through the MinGW-aware helper, not silently
  claimed for an unconfigured hosted Windows C toolchain.
- Independent Windows/Linux `verify-release.ps1` jobs build and archive twice,
  check byte reproducibility, extract the package, start it in an isolated home,
  and verify identity and memory survive same-source binary replacement.
- Linux validates GoReleaser v2.15.0 and builds every package with `-tags=ubuntu`
  and cgo disabled. There are no `ubuntu`-specific source files today; the tag is
  retained for the existing release variant, not treated as accelerator proof.
- **Qualified** always requires both full tests and independent packaging.
  Both numeric-iteration and semantic [release paths](../.github/workflows/release.yml)
  require fast CI plus full qualification and a tag matching the source version.

The native verifier's PowerShell commands check each Go/tar exit code explicitly.
Other workflow scripts select Bash, whose Actions invocation enables `-e` and
`pipefail`, including command substitutions. Artifact/cleanup steps use
`always()` to preserve failure evidence; they do not override earlier failures.
Missing acceptance manifests fail upload rather than becoming proof of success.

## Timing and allocation regression policy

The approved [policy](../build/benchmark-gate.json) fixes baseline commit
`3a8864a2c281340745bd89794ba242785297df45`. Updating that SHA, workloads,
toolchain or thresholds requires reviewed requalification, not automatic
replacement by the latest successful or failing run.

The [scheduled workflow](../.github/workflows/benchmarks.yml) uses a single
Ubuntu 24.04 amd64 runner for each comparison, not cross-run hardware timings.
[The standard-library Node runner](../build/benchmark-gate.mjs) compiles both
checkouts first and alternates baseline/candidate order across six rounds,
200 ms per benchmark, GOMAXPROCS=2, cgo disabled and identical Go versions.
It checks the complete case set, required metrics, repetition counts, fixture
node/task counts, and exact approved baseline before comparing.

- Timing regression: candidate median is **over 50% slower**, adds **over
  100 microseconds/op**, and its minimum exceeds the baseline maximum.
- Material timings whose range exceeds 50% of their median, or suspected
  regressions with overlapping ranges, are **inconclusive**, not code regressions.
- Timing below the 100 microsecond floor is reported but not gated. A run with
  no material timing cases is inconclusive, not a timing pass.
- Allocations: median grows over 15%, by over 256 B/op or over one allocation/op
  respectively, with non-overlapping sample ranges. Zero baselines are handled
  explicitly without infinite ratios.
- Exit codes: **0 passed**, **1 confirmed regression**, **2 inconclusive or
  invalid comparison**. Missing/noisy/unsupported evidence cannot turn green;
  environmental/tool failures are distinguished from measured regressions.

Every run retains raw Go JSON samples, medians/ranges, status per metric,
source commits/dirty state, CPU, OS, Go/Node versions and test parameters for
90 days. The 100-request direct/single/multi-router matrix remains a separate
synthetic diagnostic, not a real-model latency gate. The policy is deliberately
insensitive to microsecond-level hosted-runner fluctuations and does not claim
that within-run pairing eliminates every environmental effect.

Local end-to-end exercise of the actual approved baseline and candidate passed:
12 benchmark cases, six rounds per side. This validates the runner/gate on one
Linux host, not future hosted-runner stability. Injected regression, noise,
missing-case/metric, malformed-output, fixture-drift and below-floor tests
exercise both failing and inconclusive outcomes.

## Local validation record

Go 1.27.3 validation on 2026-10-26:

- `go build ./...` and `go vet ./...`: passed on Windows and Linux.
- `go test -short -count=1 -timeout=120s ./...`: passed on Windows and Linux.
- `go test -p=1 -count=1 -timeout=180s ./...`: passed on Windows.
- `CGO_ENABLED=0 go build -tags=ubuntu ./...`: passed on Linux.
- Final full race suites passed, run sequentially: Windows
  `.\build\test-race.ps1 -GoTestArgs @('-p=1','-count=1','-timeout=180s','./...')`,
  then Linux `go test -race -count=1 -p=1 -timeout=180s ./...`.
  An initial Windows race attempt timed out in shard-fixture HTTP cleanup,
  without a data-race report. Three isolated lifecycle repetitions passed;
  fixture teardown now stops the orchestrator producer before node HTTP
  fixtures, and the final full suites passed. This does not establish a
  Windows runtime defect or justify ignoring later timeouts.
- All 70 Node tests passed: 61 dashboard, six comparison-tool, three workflow
  policy tests. The synthetic router cleanup regression passed ten repetitions.
- golangci-lint, actionlint, govulncheck, exact GoReleaser v2.15.0 configuration
  checks, and npm audit passed. All 66 generated fixture hashes remained unchanged.
- The actual paired performance runner passed 12 cases, six rounds per side,
  against the approved baseline. Re-evaluating those samples with the final
  stricter invalid-metric/allocation-overlap checks also passed.
- Independent Windows package verification passed two-build/archive
  reproducibility and packaged install/replacement. Linux Go/tar package
  equivalence is recorded in [the preceding acceptance run](./end-to-end-benchmarks.md);
  Linux PowerShell is unavailable locally, so the Ubuntu PowerShell wrapper
  remains a hosted CI check, not a claimed local pass.
- Formatting normalizes checkout CRLF differences plus one existing gofmt
  correction; no production behavior or dependency manifests changed.

These are local results, not hosted Actions or PR dependency-diff review
evidence. Merge protection remains an administrator task; hosted release
publication status must be checked separately.

## Reproduce locally

Fast checks, from the repository root:

```sh
go build ./...
go vet ./...
go test -short -count=1 -timeout=120s ./...
go generate ./contracts ./internal/scheduler
node --test web/app_test.js build/benchmark-gate.test.mjs build/workflow-policy.test.mjs
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0 run --timeout=5m
go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.10 -shellcheck= -pyflakes=
go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
npm --prefix web audit --audit-level=high
```

Formatting must leave `gofmt -l` empty; generation must leave tracked and
untracked fixtures/bundles unchanged. Use `npm --prefix web ci` and
`npm --prefix web run build` for a fresh Linux web checkout. Do not replace
shared cross-platform node_modules just to run a native rebuild; see
[development](./development.md) for local environment constraints.

Full tests and package checks remain the commands in
[development](./development.md) and [package acceptance](./end-to-end-benchmarks.md).
For the paired performance gate on Linux:

```sh
git worktree add --detach /tmp/maistr0-baseline 3a8864a2c281340745bd89794ba242785297df45
node build/benchmark-gate.mjs build/benchmark-gate.json /tmp/maistr0-baseline "$PWD" /tmp/maistr0-new-comparison
git worktree remove /tmp/maistr0-baseline
```

Use new output/worktree paths and the Linux Go binary in WSL. Create/remove the
worktree with Linux git when measuring in Linux; a Windows gitdir pointer is not
a valid Linux baseline. Existing measurement files are never overwritten.

## Permissions, caching and hardware boundaries

All external actions are pinned to verified upstream commit SHAs with version
comments; monthly [Dependabot](../.github/dependabot.yml) action updates keep pins
reviewable. Go caches are keyed by the resolved toolchain, OS/architecture,
go.mod and go.sum. Only the npm download cache is shared, keyed by lockfile;
node_modules and platform executables are not cached across operating systems.
Cached tests cannot hide a run: Go test commands use `-count=1`.

Workflows default to `contents: read`, checkout never persists credentials, and
aggregate jobs have no permissions. Only the trusted tag-publication job gets
`contents: write`; release tokens are scoped to publish steps, never PR jobs or
test/package jobs. Protect release tags and review workflow/pin changes.

Published targets remain Windows/Linux amd64. Default/scheduled tests use fake
engines and processes, not accelerator inference or live downloads. Native
Windows vLLM is explicitly unsupported. GPU/NPU combinations, real model
throughput, historical-version upgrade migration, GUI installation and macOS
binary support are not qualified by these jobs; follow the opt-in
[hardware checklist](./hardware-validation.md). Setup/download/API outages and
unsupported environments must be reported as such, never as test passes.
