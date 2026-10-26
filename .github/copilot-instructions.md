## mAIstr0 Engineering Rules for GitHub Copilot

You are modifying the existing mAIstr0 repository, a Go-based local-first distributed AI orchestration platform.

## Non-negotiable rules

1. Inspect the current code before proposing or implementing changes. Treat the current repository as authoritative; historical plans and previous reviews are evidence, not proof that a task remains unfinished.
2. Read `AGENTS.md`, `improvements.md`, the relevant architecture documentation, and applicable tests before modifying a subsystem.
3. Preserve existing HTTP endpoints, JSON schemas, configuration keys, command-line behavior, and bbolt on-disk formats unless a specific task explicitly authorizes a compatibility change.
4. Do not rewrite working components merely to make them shorter, more fashionable, or architecturally different.
5. Do not introduce abstractions, dependencies, caching, goroutines, or parallelism without a concrete justification.
6. Before removing code, prove that it is unused across production code, tests, generated code, build scripts, configuration, runtime registration, reflection, and external contracts.
7. Prefer standard-library functionality and existing project dependencies.
8. Make concurrency, cancellation, resource ownership, timeouts, retries, and cleanup behavior explicit.
9. Add regression tests for every bug fix and characterization tests before behavior-preserving refactors.
10. Measure performance before and after performance changes using repeatable workloads and representative cluster configurations.
11. Never claim an optimization is faster without measured evidence. Report regressions as well as improvements.
12. Run the applicable formatting, static analysis, unit tests, race tests where supported, integration tests, frontend tests, and build checks.
13. Update documentation and changelog entries when externally observable behavior changes.
14. Never commit, push, delete user work, or overwrite unrelated changes without explicit approval.

## Required completion report

For every phase, provide:
- The problem and evidence supporting the change.
- Files and components changed.
- Dead code and duplication removed, including how usage was verified.
- Tests added and commands executed, with actual results.
- Before/after performance measurements where relevant.
- Compatibility, security, and concurrency implications.
- Remaining risks and deferred work.
- A concise proposed commit message.

Stop and report blockers rather than weakening tests or inventing successful results.

## Architecture map
- `cmd/maistr0`: CLI composition root; loads configuration and starts node and/or orchestrator roles.
- `internal/agent`: cluster-aware interactive agent orchestration.
- `internal/apperr`: typed domain error categories shared by services and HTTP adapters.
- `internal/closeutil`: best-effort deferred close helper for read-only resources.
- `internal/cluster`: in-memory registry of registered nodes.
- `internal/config`: JSON configuration loading, normalization, validation, and sharded-model settings.
- `internal/consensus`: Raft consensus and persistent consensus state.
- `internal/deepseek`: native DeepSeek interactive harness and tools.
- `internal/discovery`: LAN peer discovery and announcements.
- `internal/engine`: LLM engine interfaces, adapters, catalog, and pipeline stages.
- `internal/hardware`: host capability and hardware-usage detection.
- `internal/hub`: Server-Sent Events fan-out.
- `internal/inferroute`: protocol-aware inference routing helpers.
- `internal/integration`: process-level integration tests.
- `internal/logging`: console filtering and daily log files.
- `internal/memory`: knowledge-store interfaces, persistence, and peer synchronization.
- `internal/nodeagent`: worker service, APIs, engine execution, identity, and telemetry.
- `internal/orchestrator`: control-plane APIs, election, task planning, dispatch, and routing.
- `internal/peeridentity`: cluster certificates, trust pins, and pairing identity.
- `internal/piagent`: Pi interactive harness backed by shared mAIstr0 tools.
- `internal/scheduler`: task decomposition, dependency graphs, and worker assignment.
- `internal/scheduler/score`: deterministic, lock-free scheduler scoring.
- `internal/scheduler/cmd/contractfixture`: scheduler contract-fixture generator.
- `internal/syncinterval`: adaptive synchronization intervals.
- `internal/syncpage`: cursor- and watermark-based synchronization pagination.
- `internal/taskmgr`: task lifecycle, dependencies, recovery, persistence, and synchronization.
- `internal/trayapp`: platform-specific system-tray integration.
- `internal/version`: shared build-version metadata.
- `internal/workpool`: bounded concurrent work execution and capacity reservations.

## Compatibility
- Do not change JSON wire formats, API routes or methods, bbolt bucket names or key encodings, or configuration keys/defaults without an explicit migration and a changelog entry.
- For intentional wire-contract changes, update the OpenAPI spec, schemas, fixtures, and related tests.

## Style and implementation
- Prefer the standard library; ask before adding any dependency.
- Put `context.Context` first in parameters whenever a function accepts a context.
- Wrap errors with context using `%w`; represent domain errors with `internal/apperr`, not HTTP status codes.
- Use `log/slog` for application logging.
- Use table-driven tests; avoid sleeps longer than 100 ms in tests.
- Before adding a helper, search for an existing one; put shared code in an `internal/` package instead of copying it.
- Do not add allocations or locks to hot paths without a benchmark that measures their impact.

## Done criteria
- Run `go build ./...`, `go vet ./...`, and `go test ./...`.
- Run `go test -race ./...` where supported; report platform limitations rather than treating them as a pass.
- Run `node --test web/app_test.js`.
