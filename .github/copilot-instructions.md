# mAIstr0 Copilot Instructions

These rules apply to repository work. See [README.md](../README.md), [improvements.md](../improvements.md), and the relevant architecture and development docs for detail.

## Workflow

- Inspect the current implementation, relevant documentation, and tests before changing code; the current repository is authoritative.
- Keep changes focused and behavior-preserving. Add regression tests for bug fixes; do not rewrite working components or change unrelated code.
- Prefer the standard library and existing dependencies. Ask before adding a dependency.
- Preserve user work. Do not commit, push, or delete user changes without explicit approval.
- Wrap errors with `%w`, use `internal/apperr` for domain errors, use `log/slog`, and put `context.Context` first in function parameters.

## Compatibility and sensitive data

- Keep API routes, JSON field names, configuration keys, and bbolt bucket names/key formats unchanged unless the task explicitly says otherwise. Make compatibility-surface changes additive only.
- For an explicitly authorized contract change, include a migration and changelog entry; update the OpenAPI spec, schemas, fixtures, and tests as applicable.
- Never put raw prompts, secrets, pairing PINs, API keys, or private keys in logs or examples.

## Evidence before changes

- Before optimizing, collect representative baseline and after measurements with benchmarks or pprof; include the `benchstat` comparison in the report. Do not claim gains without evidence.
- Before removing code, use `deadcode` and repository-wide reference evidence. Check interface dispatch, reflection, generated code, build tags, runtime registration, and external contracts before concluding code is unused.

## Concurrency and ownership

- Document which state each mutex protects and its lock order near the owner. Keep locks out of network, disk, and engine calls.
- Give goroutines a clear owner and cancellation path; cancel and join owned goroutines during shutdown. Document unavoidable exceptions and preserve established lifecycle behavior; see [docs/concurrency-ownership.md](../docs/concurrency-ownership.md).

## Documentation and validation

- For user-visible behavior or configuration changes, update `README.md` or the relevant docs and `changelog.md`.
- Run these from the repository root and report exact results:
  - `go build ./...`
  - `go vet ./...`
  - `go test ./...`
  - `node --test web/app_test.js`
  - `go test -race ./...` on Linux where available; report platform limitations instead of treating an unavailable run as a pass.
- Report changed files, checks and results, performance evidence where relevant, compatibility/concurrency implications, and remaining risks.
