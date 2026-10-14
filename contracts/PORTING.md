# Porting guide

Use the contracts in this directory as the language-neutral wire-format
reference when implementing a mAIstr0 client, node agent, or compatible service
in another language.

## Contract sources

- [`openapi.yaml`](./openapi.yaml) lists every route registered by the
  orchestrator and node agent. `x-maistr0-registered-routes` preserves the
  exact Go mux patterns, including methodless and subtree registrations.
- [`schemas/`](./schemas/) contains JSON Schema 2020-12 definitions for
  configuration, tasks, assignments, node status, discovery announcements,
  and memory records.
- [`fixtures/phase1/`](./fixtures/phase1/) contains exported HTTP golden cases.
  Each file has a `request`, `expected_response`, and `normalization_rules`.
  Response envelopes retain the HTTP status and content type; their `body` is
  either a JSON value or the normalized streaming body.
- [`fixtures/scheduler/assignment.json`](./fixtures/scheduler/assignment.json)
  contains fixed node, model, and subtask inputs with the scheduler's exact
  assignments and score factors.

## Wire-format conventions

- JSON field names and optionality follow the schemas. Treat absent optional
  fields as unspecified, not as empty strings or zero values. Do not reject
  unknown object properties; the schemas deliberately allow additive fields.
- Timestamps are RFC 3339 date-time strings. Preserve their timezone and
  fractional-second precision when forwarding records.
- Task status values are `pending`, `running`, `synthesizing`,
  `awaiting_approval`, `completed`, `failed`, and `cancelled`. Task graph node
  keys are subtask IDs; graph edges point from prerequisite to dependent.
- Assignment scores are explained by `score_factors`; optional factors may be
  absent when they do not apply. Do not rely on one particular score as a
  stable API guarantee.
- Discovery announcements require `role`, `id`, `http_addr`, and `peer_addr`.
  The optional `orchestrator_id` identifies the associated coordinator; it
  must not be inferred merely from another visible announcement.
- Memory snapshots may include `sync_version` and per-origin `watermarks`.
  These are additive synchronization metadata. Peers that do not negotiate
  incremental sync must continue to exchange full snapshots, and missing
  record `origin` or `sequence` values must remain acceptable for legacy data.
- The node's internal queued-task count is not a wire field. Do not add it to
  `NodeStatus` payloads when porting the implementation.
- Methodless routes and subtree patterns in the OpenAPI route manifest reflect
  existing mux behavior. Route documentation is an inventory, not a guarantee
  that every handler accepts every HTTP method or arbitrary subtree path.

## Golden fixtures and drift checks

The exported phase-one fixtures are generated from the existing orchestrator
goldens. Their request entries include the method and path; task cases that
depend on prior state describe that setup, and cases sharing one 404 response
list their request variants. Normalization rules identify volatile timestamps,
IDs, local worker URLs, scores, and streaming values.

Regenerate the exported and scheduler fixtures after an intentional contract
change:

```sh
go generate ./contracts ./internal/scheduler
```

Review generated changes along with the implementation change. The scheduler
fixture generator calls the real Go scheduler with fixed inputs; it is not a
second implementation of its scoring rules. Contract route, fixture, and
schema validation runs with:

```sh
go test ./contracts
go test ./...
go vet ./...
node --test web/app_test.js
```

CI regenerates both fixture sets, checks that generation leaves no diff, and
runs the focused contract tests before the complete Go test suite.
