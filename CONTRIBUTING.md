# Contributing

Thanks for helping improve mAIstr0. For repository-specific code conventions,
architecture, lock ordering, and contract updates, see [AGENTS.md](./AGENTS.md).
The [engineering improvement plan](./docs/engineering-improvement-plan.md)
tracks the status of the phased work.

## Contracts and concurrency

- Characterization and HTTP contract coverage lives beside the implementation,
  especially in
  [`internal/orchestrator/api_contracts_test.go`](./internal/orchestrator/api_contracts_test.go)
  and the package `testdata/contracts/` directories. Do not change behavior or
  regenerate a golden just to make a failing test pass; document intentional
  behavior changes and update the affected fixtures.
- Keep critical sections short and do not call another component or perform
  network, engine, or disk I/O while holding a state lock. Task-manager writes
  follow `persistGate` → per-task `persistMu` → `Manager.mu`; see
  [`AGENTS.md`](./AGENTS.md) for the full lock-order rules. Document any new
  component mutex beside the owning struct.
- For exported HTTP and scheduler fixture drift, run
  `go generate ./contracts ./internal/scheduler`, inspect the generated files,
  then run `go test ./contracts`. Package golden files are updated only with
  their explicit `-args -update` test flag.

## Branches and pull requests

- Start from an up-to-date `main` and use a focused short-lived branch, such as
  `feature/<topic>`, `fix/<topic>`, or `docs/<topic>`. Do not commit directly
  to `main`.
- Keep each pull request scoped to one logical change. Separate mechanical
  refactors from behavior changes where practical, and preserve unrelated
  working-tree changes.
- Describe behavior and compatibility impact, important design decisions, and
  any follow-up work. Include tests and documentation updates in the same
  change. Update `changelog.md` for user-visible behavior or configuration
  changes.
- Do not claim a check passed if it was skipped or blocked by a local toolchain;
  report the limitation and rely on the matching CI result.

## Checks

Before requesting review, run:

```sh
go generate ./contracts ./internal/scheduler
git diff --exit-code -- contracts/fixtures
go test ./contracts
go test ./...
go vet ./...
go build ./cmd/maistr0
node --test web/app_test.js
```

CI runs those checks on Ubuntu and Windows. It also runs
`go test -race ./...` on Linux. Race-detector limitations seen on Windows and
Ubuntu 24.04 WSL are recorded in
[README Development](./README.md#development); use Linux CI for the race gate
if the local platform cannot run it.

## Changes to API or persisted formats

Treat routes, HTTP methods, status codes, JSON fields and optionality, config
keys/defaults, task states, and bbolt encodings as compatibility contracts.
For an intentional wire-format change:

1. Explain the motivation, compatibility impact, rollout or migration plan,
   and mixed-version behavior in the pull request.
2. Prefer additive optional fields and preserve existing routes and response
   envelopes where possible. Do not silently rename or reinterpret existing
   fields.
3. Update `contracts/openapi.yaml`, the relevant JSON Schemas and fixtures,
   and [contracts/PORTING.md](./contracts/PORTING.md). Add deterministic
   request/response tests for both the new behavior and compatibility path.
4. Regenerate fixtures with the documented commands, inspect the generated
   diff, and update the README and changelog when users or client authors are
   affected.

For storage changes, include a backward-compatible migration or recovery
strategy and tests using legacy data. A contract change is not complete until
the implementation, tests, language-neutral artifacts, and documentation
agree.
