# Repository guidance

Use this as the repository-wide guide for coding agents. Contributor workflow
and required checks are in [CONTRIBUTING.md](./CONTRIBUTING.md); the current
improvement-phase status is in
[docs/engineering-improvement-plan.md](./docs/engineering-improvement-plan.md).

## Architecture

- `cmd/maistr0` is the composition root. `internal/orchestrator` coordinates
  tasks, planning, routing, persistence, and paired workers; `internal/nodeagent`
  owns worker execution, engines, telemetry, and its orchestrator link.
- LAN election ranks currently visible candidates and expires peers by TTL. It
  is not consensus: a partition can produce a local winner on each side.
  Mutating requests must still check current authority and authenticated trust.
  See [docs/lan-election.md](./docs/lan-election.md).
- `internal/scheduler` filters eligible nodes and scores assignments.
  `internal/workpool` bounds shared work; orchestrator and node capacity gates
  enforce their respective limits. Keep readiness and telemetry freshness in
  eligibility decisions.
- `contracts/openapi.yaml`, `contracts/schemas/`, and `contracts/fixtures/`
  are the language-neutral HTTP and data-contract references. Update them with
  tests whenever a wire contract intentionally changes.

## Concurrency and state

- Keep critical sections short. Do not hold state locks over network, disk, or
  engine calls, or while calling another component.
- Preserve the task-manager lock order:
  `persistGate` → per-task `persistMu` → `Manager.mu`. Release `Manager.mu`
  before waiting for a task lock; state replacement and close take the exclusive
  `persistGate`.
- Engine scans acquire `engineScanMu` before `enginesMu`; do not hold
  `enginesMu` while calling engines. Inventory refresh follows
  `refreshMu` → `modelMu` → agent state.
- Pass contexts through handlers, dispatch, and capacity waits. Preserve the
  workpool's documented cancellation and shutdown semantics.

## Tests and contract updates

Run the applicable checks before proposing a change:

```sh
go test ./...
go vet ./...
go build ./cmd/maistr0
node --test web/app_test.js
```

Run `go test -race ./...` on a supported Linux environment when changing
concurrent code. Windows may fail before tests start because ThreadSanitizer
cannot allocate shadow memory (error 87); Ubuntu 24.04 WSL has also had an
intermittent node-agent test failure without a race report. See
[docs/development.md](./docs/development.md) for the recorded limitations and
CI race job.

For package characterization goldens, run the relevant package tests with
`-args -update`, for example:

```sh
go test ./internal/orchestrator ./internal/scheduler ./internal/taskmgr ./internal/config -args -update
```

For exported HTTP and scheduler fixtures, run
`go generate ./contracts ./internal/scheduler`. Review every generated change;
normal CI regenerates these files and fails if `contracts/fixtures/` drifts.
Do not update a golden merely to make a failing test pass.

## Implementation conventions

- Handlers should decode and validate input, check authentication/current
  authority before state changes, delegate to a component, and use the shared
  typed-error-to-HTTP mapping. Preserve status codes, body shapes, and routes
  unless an intentional contract change is documented. Cover handlers with
  `httptest` and deterministic fixtures.
- Give components explicit dependencies and narrow interfaces. Keep state with
  the component that owns it, document mutex ownership, and add focused tests
  when extracting responsibilities.
- Depend on persistence interfaces rather than storage implementations.
  Keep bbolt-specific logic in adapters; preserve bucket/key formats and
  recovery behavior. Test adapters and in-memory implementations against the
  same store semantics.
- Prefer small, behavior-preserving changes. Update the README/changelog for
  user-visible changes and the OpenAPI, schemas, fixtures, and porting guide
  for wire-format changes.
