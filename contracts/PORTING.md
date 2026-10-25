# Porting guide

Use the contracts in this directory as the language-neutral wire-format
reference when implementing a mAIstr0 client, node agent, or compatible service
in another language.

## Contract sources

- [`openapi.yaml`](./openapi.yaml) lists every route registered by the
  orchestrator and node agent. `x-maistr0-registered-routes` preserves the
  exact Go mux patterns, including methodless and subtree registrations.
- [`schemas/`](./schemas/) contains JSON Schema 2020-12 definitions for
  configuration, tasks and durable dispatch attempts, assignments, node and
  engine readiness, sharded-model pipeline plans, router request events,
  discovery announcements, and memory records.
- [`fixtures/phase1/`](./fixtures/phase1/) contains exported HTTP golden cases.
  Each file has a `request`, `expected_response`, and `normalization_rules`.
  Response envelopes retain the HTTP status and content type; their `body` is
  either a JSON value or the normalized streaming body.
- [`fixtures/scheduler/assignment.json`](./fixtures/scheduler/assignment.json)
  contains fixed node, model, and subtask inputs with the scheduler's exact
  assignments and score factors.
- [`fixtures/shards/`](./fixtures/shards/) is generated from Go wire types for
  both managed sharding modes, create/list/get/delete, and degraded/failed
  snapshots. Each response includes its HTTP status and schema where it has
  a body. A successful delete is `204` with no body.

## Managed shard groups

Managed groups are distinct from the protocol-v2 stage pipeline below.
`POST /api/shards` accepts `CreateShardGroupRequest`: a model profile, mode
(`llamacpp-rpc` or `vllm-mp`), and optional pinned node IDs. It returns `201`
with a ready `ShardGroup`; invalid placement returns `400`, conflicting model
or active operations `409`, missing capacity/members/persistence `503`, and
upstream start failures `502`. `GET /api/shards` returns `{"groups":[]}`;
`GET /api/shards/{id}` returns a group or `404`. Deletion stops the head before
workers and returns `204`, or an error if teardown/persistence fails.

Dashboard shard setup reads configured engine model profiles through
`GET /api/nodes/{id}/engines/managed/{engine_id}/profile`. The node-local
`GET /api/engines/managed/{id}/profile` route is restricted to the current
paired orchestrator, parses allow-listed GGUF or Hugging Face metadata, and
measures model weight files without exposing their paths. Supply a positive
`requested_context` query value to include the KV-cache context in the profile.
Do not substitute model-name/size estimates when profile metadata or measurable
weight files are unavailable; reject the candidate and let the planner explain
placement refusals.

Preserve the lifecycle values `planning`, `starting_workers`, `starting_head`,
`ready`, `degraded`, `failed`, and `stopping`. Do not infer readiness from node
heartbeats or a loaded persisted record. Member loss first degrades the group;
the next health sweep tears down remaining members and fails it. Startup
recovery verifies members and reserves capacity before restoring readiness.
Rebuild creates a new group; there is no in-place rebuild route. Failed
history may coexist with the new ready group for the same model.

The transparent router uses the group's model name and sends inference to the
head through `/api/engines/managed/{id}/proxy/{path...}`. Non-ready groups
return `503`; one request at a time occupies the group's request slot.
Preserve HTTP/SSE bodies and do not silently fall back to individual shards.

Group members may include `rpc_relay_endpoint` (remote authenticated relay)
and `rpc_backend_address` (local worker backend), plus vLLM `rank` and
`master_addr`. Never treat the RPC backend address as a remote safe endpoint.
Managed status exposes `relay_endpoint`. llama.cpp head start plans carry
typed `rpc_relays` targets with `peer_id` and `address`; the supervisor converts
them to local loopback relay endpoints. vLLM plans carry `nnodes`, `node_rank`,
`master_addr`, and parallelism sizes; native Windows launch is unsupported.
Node managed lifecycle/proxy operations require the pinned, paired **current**
orchestrator, not merely any TLS client. vLLM distributed traffic additionally
requires a trusted private interface or the explicit insecure override.

Do not change bbolt bucket names/keys when porting this behavior; the fixture
generator is a wire-format reference, not a database migration. See
[network security](../README.md#distributed-shard-network-security) and the
[troubleshooting guide](../docs/sharding-troubleshooting.md).

## Wire-format conventions

- Production HTTP listeners add an `X-Request-ID` response header and cap request
  bodies at 256 MiB, while routes may enforce smaller limits. Request logs
  contain only the request ID, method, status, and duration; they omit URLs,
  request bodies, prompts, and model outputs.
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
- Task creation accepts `plan_mode` values `auto`, `heuristic`, and
  `heuristic_v1`; omitting the field uses the current heuristic. `auto` plans
  are produced by a local coordinator and may be cached by normalized
  description. `routing_preference` accepts `quality` or `latency` and applies
  to all subtasks in the request. Completion-time score factors are heuristic
  estimates based on model size, queue depth, and historical throughput; they
  are explanatory values, not latency guarantees.
- Orchestrator configuration may opt into speculative hedging with
  `speculative_hedging_enabled`; it is disabled by default.
  `max_concurrent_hedges` defaults to one and is capped at four and by
  `max_concurrent_dispatches`. When enabled, a second eligible node may receive
  a task after a model/task-type p95 delay with enough history; cancellation of
  the losing request does not change the persisted task result.
- `/router/{path...}` is a separate transparent inference surface; it accepts
  only the provider paths listed in the OpenAPI document and does not invoke
  workflow planning. Router events contain request/attempt metadata only, not
  prompts or response bodies. Retries stop once response content is committed.
- Node agents serve read-only `GET /api/cluster/health` and `GET /api/shards`
  by proxying them, unchanged, to the joined orchestrator; they return `503`
  before a node has joined. Node `GET /api/tasks` forwards its query string, so
  pagination parameters behave as they do on the orchestrator.
- Nodes that omit engine status remain compatible with older clients. Once a
  node reports engines, scheduling should use each engine's readiness and
  advertised protocol capabilities.
- Node shard-readiness fields are optional and additive. `current_stage_assignments`
  is the number of stage assignments currently held by that node. A pipeline
  plan's stages use half-open layer ranges and must be contiguous, non-overlapping,
  and cover the model's complete layer range; Go validation enforces these
  cross-stage invariants. Nodes may configure `supported_shard_models` and
  `max_layers_it_can_host` before stages are loaded so the orchestrator can place
  the first plan; loaded assignments are also reflected in readiness.
- A shard-readiness declaration alone is not proof of runtime support. Engine
  status may include `pipeline_stage`, populated only after probing the local
  engine's `/stage/capabilities` endpoint and validating its protocol version,
  model/layer limits, and tensor dtypes. `pipeline_stage_error` reports probe
  failures; clients must not infer capability from model inventory.
- The legacy `/stage/forward` newline-delimited JSON protocol remains available
  for compatibility. Protocol v2 uses `/stage/open`, `/stage/prefill`,
  `/stage/decode`, and `/stage/close` over the existing trusted mTLS surface.
  Prefill/decode bodies use `application/vnd.maistr0.pipeline-stage-v2`: a
  4-byte big-endian JSON-header length, the JSON header, then the contiguous
  tensor payload described by its activation metadata. Only activations cross
  stage boundaries; each stage retains its request-local KV state until close.
  Open/close requests use `PipelineStageControlRequest`. Cancellation must close
  all opened stages and release their capacity slots.
- Sharded models are registered additively in orchestrator startup config under
  `sharded_models`. The orchestrator persists placement plans locally by
  `logical_id`, revalidates them against current node readiness, and uses the
  logical ID as the only client-visible model name.
- Discovery announcements require `role`, `id`, `http_addr`, and `peer_addr`.
  The optional `orchestrator_id` identifies the associated coordinator; it
  must not be inferred merely from another visible announcement.
- Memory snapshots may include `sync_version` and per-origin `watermarks`.
  These are additive synchronization metadata. Peers that do not negotiate
  incremental sync must continue to exchange full snapshots, and missing
  record `origin` or `sequence` values must remain acceptable for legacy data.
- Sync protocol v3 adds optional gzip and empty-exchange responses without
  changing routes or JSON records. Clients advertise response compression with
  `Accept-Encoding: gzip`; send compressed request bodies only after the peer
  responds with protocol v3. Compression is used only for payloads of at least
  1 KiB when it reduces the body size, and the configured sync limit applies to
  both compressed and decompressed bodies. A v3 request with no changes returns
  204 plus sync-version and watermark headers; do not POST an empty task page or
  memory snapshot. Protocol v1/v2 peers continue to receive JSON, v2 paging
  remains supported, and unsupported or ahead watermarks retain full-snapshot
  fallback.
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

CI regenerates all three fixture sets and fails on modified or newly generated
untracked files under `contracts/fixtures/`. Contract tests check registered
routes against OpenAPI, shard-operation fixture coverage, and request/response
schemas, including generated Go member fields.
