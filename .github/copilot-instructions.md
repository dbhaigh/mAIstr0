# mAIstr0 Copilot Instructions

## Architecture map
- `cmd/maistr0`: CLI composition root; loads configuration and starts node and/or orchestrator roles.
- `internal/agent`: cluster-aware interactive agent orchestration.
- `internal/apperr`: typed domain error categories shared by services and HTTP adapters.
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
