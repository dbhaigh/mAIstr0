# API reference

The complete route and schema contract is
[contracts/openapi.yaml](../contracts/openapi.yaml). The
[JSON Schemas](../contracts/schemas/) and
[porting guide](../contracts/PORTING.md) describe request and response
compatibility.

## Access and trust

The orchestrator dashboard and API default to `http://127.0.0.1:7450`; a node
uses port `7451`. Plain HTTP is loopback-only. Remote cluster clients must use
HTTPS and a paired mutual-TLS identity. Pairing and the trust boundary are
described in [Pairing and security](./pairing-and-security.md).

## Tasks and workflows

- `POST /api/tasks` submits a description or an explicit subtask/dependency
  plan.
- `GET /api/tasks` and `GET /api/tasks/{id}` list and inspect work.
- `GET /api/tasks/{id}/dependencies` returns the workflow graph.
- `POST /api/tasks/{id}/cancel` cancels work.
- `POST /api/tasks/{id}/approve` resumes a task waiting for human approval.
- `GET /api/status` reports service and persistence health.
- `GET /api/events` streams cluster snapshots using SSE.

For task planning, `"plan_mode":"auto"` is the recommended option. It asks a
local coordinator model for a plan and caches successful plans in a bounded
in-memory cache. Omitting `plan_mode` uses the current heuristic;
`"plan_mode":"heuristic_v1"` selects the legacy heuristic explicitly.
Explicit plans are available for callers that need fixed IDs, roles, and
dependencies.

The automatic planner retries another advertised node/model if the selected
coordinator model fails. If no candidate succeeds, the error is returned.
The heuristic recognizes enumerations, comparisons, and “for each” work,
and adds a final synthesis task for fan-out plans. Plans are checked for
missing dependencies and cycles. Subtasks can specify roles, expected output,
JSON output contracts, required fields, and bounded retries. Set
`"require_review":true` to require reviewer approval before synthesis, or
`"require_approval":true` to pause completed work for a human. Approval state
and outputs survive restarts.

`"routing_preference":"latency"` favors faster completion estimates;
`"quality"` is the default and retains model-quality bonuses. Speculative
hedging is disabled by default and can be enabled in orchestrator
configuration. When enabled, at most the configured bounded number of secondary
attempts may be dispatched after historical p95 latency.

Example task submission:

```powershell
$body = @{ description = "Summarize this report"; plan_mode = "auto" } | ConvertTo-Json
Invoke-RestMethod -Uri "http://127.0.0.1:7450/api/tasks" -Method Post `
  -ContentType "application/json" -Body $body
```

```sh
curl -X POST http://127.0.0.1:7450/api/tasks \
  -H 'Content-Type: application/json' \
  -d '{"description":"Summarize this report","plan_mode":"auto"}'
```

## Inference-compatible APIs

- `POST /v1/chat/completions` exposes the OpenAI-compatible chat API.
- `POST /v1/messages` exposes the supported Anthropic Messages surface.
- `GET /api/tags`, `POST /api/chat`, and `POST /api/generate` expose the
  Ollama-compatible surface.
- The bundled catalog is available from `GET /api/models/catalog`.
- Node model pulls use `POST /api/nodes/{id}/models/pull`.

Streaming is supported on the compatibility surfaces. OpenAI chat streams
use SSE; Ollama streams use newline-delimited JSON. The Anthropic-compatible
route is available only when a ready node engine advertises that protocol.
OpenAI-compatible chat preserves system, user, assistant, and tool message
roles. Task history records selected node/model/engine, status, and failure
class for each bounded dispatch attempt.

## Managed shard groups

The dashboard starts a shard setup wizard when a healthy node is Ctrl-clicked.
It lists only models with configured managed shard engines and readable model
profiles. The orchestrator proxies profile reads through
`GET /api/nodes/{id}/engines/managed/{engine_id}/profile`; the node endpoint is
restricted to the current paired orchestrator and does not expose model paths.
`POST /api/shards` accepts the selected profile, mode, and pinned node IDs.
Placement uses current free device memory with a 2 GiB per-device safety
margin; insufficient or unknown memory is rejected rather than estimated from
host RAM.

## Transparent router

The transparent router does not invoke task planning or translate provider
payloads. It supports:

- `POST /router/v1/chat/completions`
- `POST /router/v1/completions`
- `POST /router/v1/messages`
- `POST /router/api/chat`
- `POST /router/api/generate`

The router selects a healthy node/model whose ready engine advertises the
requested protocol, and retries only bounded transient failures. Streaming
failover is allowed only before response bytes are committed. Breaker state
and prompt-free request events are available from `GET /api/router/requests`
and SSE snapshots. Circuit breakers operate per node/model pair and admit a
limited half-open probe after backoff.

Router first-byte and idle-stream timeouts are distinct from the total
deadline. Optional client rate and concurrency limits are keyed by a hash of
the API key or authorization value, or by the direct remote address. Forwarded
address headers are not trusted. Prompts, credentials, and model outputs are
not exposed by request events or metrics.

## Metrics

`GET /metrics` serves dependency-free Prometheus text metrics;
`GET /api/metrics` remains a compatible alias. Metric families include queue
depth, dispatch attempts and failures, bounded failure classes, latency
histograms, active streams, sync progress, inventory freshness, and pairing
events. Labels are kept bounded to avoid per-node metric cardinality.

The dashboard groups cluster health and workload, the interactive orchestrator,
and memory/learning views. Task history is pageable, and a subtask assignment
can navigate to and highlight its selected node.

The interactive harness can be switched for new conversations; existing
conversations remain on their original harness. An interrupted agent turn
cancels downstream model dispatches while preserving the session. The cluster
view includes discovered LAN peers, node/engine readiness, model inventory,
tasks, and setup/pairing controls. If persistent memory is unavailable,
`GET /api/status` reports degraded persistence so the dashboard can warn that
conversation history and learned data will not survive restart.

## Distributed models

Sharded models are declared in the orchestrator's `sharded_models`
configuration. Eligible nodes need a matching source model,
`max_layers_it_can_host`, and an engine that advertises the protocol-v2 stage
capability. The scheduler places contiguous layer ranges and exposes the
logical model ID through the API.

The current production request path targets trusted LAN clusters and pipeline
parallelism only. The separate deterministic planner can propose llama.cpp RPC
or vLLM multi-node layouts from supplied metadata, but does not launch engines
or activate those layouts in request dispatch. The simulated stage engine is
available for development and tests; ordinary llama.cpp and vLLM
OpenAI-compatible adapters are not production partial-layer backends. A stage
failure cancels the request; live KV state is not migrated. See
[Configuration](./configuration.md) for engine and sharded-model settings and
[the shard planner guide](./shard-planner.md) for planning semantics.
