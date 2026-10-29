# Documentation

Start with the [project overview and quick start](../README.md), then use the
guides below.

## Product guides

- [API reference](./api.md)
- [Dashboard workflows](./dashboard.md)
- [Dashboard architecture](./dashboard-architecture.md)
- [Dashboard events, measurements, and delta proposal](./dashboard-events.md)
- [Configuration](./configuration.md)
- [Node service controller](./node-service-controller.md)
- [Engine support and network guidance](./engines.md)
- [Memory and task synchronization](./sync.md) and
  [sync protocol details](./memory-sync.md)
- [Pairing and security](./pairing-and-security.md)
- [Sharding troubleshooting](./sharding-troubleshooting.md)

## Architecture and operations

- [Architecture](./architecture.md), [LAN election](./lan-election.md), and
  [distributed guarantees](./distributed-guarantees.md)
- [Consensus migration](./consensus-migration.md)
- [Concurrency and resource ownership](./concurrency-ownership.md)
- [Task routing and decomposition quality](./routing-evaluation.md),
  [scheduler evaluation](./scheduler-evaluation.md), and
  [shard planner](./shard-planner.md)
- [Distributed model parallelism](./distributed-model-parallelism.md)
- [Hardware validation](./hardware-validation.md)
- [Development and validation](./development.md)
- [Test health and determinism](./test-health.md)
- [CI quality gates](./ci-quality-gates.md)
- Run `python scripts/check_doc_links.py` from the repository root to validate
  local Markdown links and heading fragments.

## Evidence and project history

- [Lean baseline](./lean-baseline.md)
- [Performance baseline](./perf-baseline.md) and
  [performance evaluation](./performance-evaluation.md)
- [End-to-end benchmarks and release acceptance](./end-to-end-benchmarks.md)
- [Sync traffic measurements](./sync-traffic.md)
- [Cleanup inventory](./cleanup-inventory.md)
- [Dispatcher retries](./dispatcher-retries.md)
- [Engineering improvement assessment](./engineering-improvement-plan.md)
- [Security review](./security-review.md)
- [Open items](../improvements.md)
- [Archived October 2026 improvement phases](./archive/improvements-2026-10.md)

## Contracts and project policies

- [OpenAPI specification](../contracts/openapi.yaml)
- [JSON Schemas](../contracts/schemas/)
- [Contract porting guide](../contracts/PORTING.md)
- [Changelog](../CHANGELOG.md)
- [Security policy](../SECURITY.md)
- [Security notes](./security.md)
- [Contributing](../CONTRIBUTING.md)
- [License](../LICENSE)
