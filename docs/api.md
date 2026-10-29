# API reference

## Transparent-router lifecycle diagnostics

The existing router event API adds `accepted`, `scheduled`, `queued`,
`dispatched`, `upstream_response`, `first_upstream_byte`, and terminal
`completed`/`failed` phases, retaining previous attempt/rejection/finished
phases and JSON fields. Correlate by generated request/attempt IDs; tolerate
unknown phases and bounded-log eviction. These events are diagnostics, not a
durable audit log. First upstream byte is not first generated token.
See [timing, privacy, retention and benchmark semantics](./end-to-end-benchmarks.md#request-diagnostics-and-dashboard-interpretation).

## Enrollment security migration

New `POST /api/pairing/connect` requests supply `address`, a valid one-use `pin`,
and the out-of-band verified `fingerprint` (64 hexadecimal characters, SHA-256
of the complete DER leaf certificate), unless the coordinator's strict policy
is disabled and the connection is on a prefix assigned to one of its local
interfaces. The same-subnet path independently validates client TLS identity;
discovery IDs and addresses alone are not authorization. TLS redirects are
refused and enrollment certificates must match the actual TLS leaf before
trust changes.

`POST /api/discovery/{id}/pairing/invite` requires `pin` and the destination
node's verified `fingerprint`. Incoming invitations remain pending until local
`POST /api/pairing/invitation/accept` receives
`{"fingerprint":"<independently verified orchestrator SHA-256>"}`.
Missing/malformed fingerprints return 400, mismatches 403 (pending invitation
retained), and expired/consumed invitations 410. Invitation status includes an
offered fingerprint for comparison, not automatic trust.

`GET /api/pairing/status` adds the public `fingerprint` and
`require_pairing_verification`; the active PIN is local-plaintext-only, while
its expiry remains visible to support the local dashboard. For nodes,
`auto_join_available` reports a discovered orchestrator on a connected local
interface prefix when strict verification is disabled.
`GET /api/pairing/policy` reads the cluster-wide policy. Local-dashboard-only
`PATCH /api/pairing/policy` changes it immediately on the coordinator and
propagates it to joined peers during normal refresh. Strict mode affects only
new joins and re-pairs; it does not revoke existing trust.
Public engine endpoints omit URL userinfo/query/fragment and sanitize inventory
errors. Saving an unchanged redacted endpoint preserves its existing credentials;
change credentials in protected configuration. Existing trust files need no
conversion. See [migration](./pairing-and-security.md#migration).

## Peer executable distribution and shard membership

- `GET /api/updates/manifest?os=darwin&arch=arm64` returns
  `{ "version": "0.0.24.1", "os": "darwin", "arch": "arm64",
  "sha256": "<64 lowercase hex characters>", "size": 14000000 }`.
- `GET /api/updates/binary` with the same query downloads that executable.
  Both listeners expose these read-only endpoints to paired mTLS peers
  (or local loopback). An unavailable target returns 404. No arbitrary
  filesystem paths or download URLs are accepted. The manifest may include an
  Ed25519 `signature`; a receiving node configured with
  `peer_update_publisher_key` requires and verifies it before staging. Without
  that key, the legacy checksum-only behavior remains. See the executable-trust warning in
  [configuration](./configuration.md#automatic-peer-executable-updates).
- `POST /api/shards/{id}/members` accepts
  `{ "node_ids": ["AHDEV01", "AHDT00"] }` and returns the ready group.
  Retain the current model-head node. New members must be healthy and have
  the required managed worker; the dashboard installs missing RPC workers
  using the existing protected installer before sending this request.
  The operation drains current inference, stops the head before workers,
  refreshes device telemetry, recomputes proportional comfortable-memory
  placement, then starts workers before the head. It keeps the group ID,
  model and creation time. Requests during reload are unavailable.
  Invalid selections fail before teardown. Runtime or placement failures
  remain visible on the saved group; inspect the error and use Start to retry.
  The existing pairing/current-authority fence and operation exclusion apply
  on both the coordinator and worker-dashboard forwarding route. reference

## Discovery authentication status

`GET /api/security/status` returns the local instance's discovery mode:
`{"discovery_authentication":"unauthenticated"}` or
`{"discovery_authentication":"hmac-sha256"}`. It never returns the configured
secret. LAN announcements remain unauthenticated by default; when
`discovery_hmac_secret` is configured, announcements include an HMAC and
freshness timestamp, and listeners reject forged, stale, and replayed packets.
See [pairing and security](./pairing-and-security.md#optional-lan-discovery-authentication).

## Automatic RPC worker installation

`POST /api/nodes/{id}/engines/managed/install-worker` accepts
`{"source":"official"}` or `{"source":"xdna"}`. The node-local
`POST /api/engines/managed/install-worker` requires the current paired
orchestrator's pinned certificate. A successful response includes `source`,
`version`, `updated`, current `engines`, optional fresh `devices` memory readings,
and an optional update-deferred
`warning`. Missing releases or failed verification return `502`; unsupported
platforms return `501`; installation requiring stopped engines returns `409`.
See [automatic RPC workers](./configuration.md#automatic-rpc-workers).

## Local model heads

`GET /api/nodes/{id}/engines/managed/head-models?requested_context=4096`
discovers readable Ollama/LM Studio GGUF weights and verified Hugging Face
downloads on the selected head node.
It returns `models` (opaque `id`, display `name`, `source`, and parsed `profile`)
and `warnings`, without exposing filesystem paths.

`POST /api/nodes/{id}/engines/managed/configure-head` accepts
`{"model_id":"<discovered ID>","requested_context":4096}` and returns the
persisted stopped managed head. It requires an installed runtime and idle
managed engines; unknown models return `404`, busy engines `409`. The corresponding
node-local routes under `/api/engines/managed/` require the pinned current
orchestrator, just like installation.

## Compatible model acquisition

Task submission automatically attempts local GGUF loading before Hugging Face
downloads when no ready model satisfies planning or subtask requirements.
Only idle nodes with sufficient known GPU memory are eligible; active groups
and operator-configured heads are preserved. Successful acquisition starts a
managed single-node shard group visible in **Shard groups**, then retries task
assignment against the refreshed inventory. Use its normal Stop/Start/Delete
controls afterwards. Downloaded weights remain cached when a group is deleted.
Update both the orchestrator and any nodes that should download GGUFs.

For manual acquisition, install the managed runtime first, then submit:

```powershell
$body = @{ model = "qwen2.5-3b-instruct-q4"; requested_context = 4096 } | ConvertTo-Json
Invoke-RestMethod -Method Post -ContentType "application/json" -Body $body `
  -Uri "http://127.0.0.1:7450/api/nodes/NODE_ID/engines/managed/download-model" `
  -TimeoutSec 3600
```

Supported recommendation IDs:
- `qwen2.5-3b-instruct-q4`: `Qwen/Qwen2.5-3B-Instruct-GGUF`,
  `qwen2.5-3b-instruct-q4_k_m.gguf` (about 2.1 GB).
- `qwen2.5-7b-instruct-q4`: `bartowski/Qwen2.5-7B-Instruct-GGUF`,
  `Qwen2.5-7B-Instruct-Q4_K_M.gguf` (about 4.7 GB).

The response is one local-model object (`id`, `name`, `source: "huggingface"`,
`profile`). Select it in the shard wizard or pass its opaque ID to
`configure-head`, then create a group with its actual profile. Existing local
Ollama and LM Studio models need no download: use `head-models` and
`configure-head` directly. No filesystem paths or arbitrary download URLs are
accepted. A model is published into the persistent runtime model cache only
after Hugging Face LFS SHA-256, exact size, and GGUF metadata verification.
Partial or invalid downloads are removed. Downloads and verified cached reads
are bounded; requests preserve cancellation. The largest transfer is capped at
6 GiB, with a one-hour acquisition deadline.

The corresponding node-local `POST /api/engines/managed/download-model`
requires the pinned current orchestrator's mTLS identity. Invalid IDs/contexts
return `400`; authorization failures return `403`; download/verification failures
return `502` and are explained in node logs. These are additive routes; local
inventory sources additionally accept `huggingface`. Older peers return an
explicit unsupported-route error and must be upgraded for acquisition.

`POST /api/shards` accepts optional paired `head_node_id` and `head_engine_id`
fields for llama.cpp RPC. These select the exact model head, including when
its devices receive no layers. If `pinned_nodes` is supplied, include the head.
Omit both fields to retain automatic first-available head selection.

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
- `GET /api/events` streams full cluster snapshots using SSE. Clients that
  advertise gzip may receive a compressed HTTP stream; the decoded event data
  and SSE framing are unchanged.

For task planning, `"plan_mode":"auto"` is the recommended option. It asks a
local coordinator model for a plan and caches successful plans in a bounded
in-memory cache. Omitting `plan_mode` uses the current heuristic;
`"plan_mode":"heuristic_v1"` selects the legacy heuristic explicitly.
Explicit plans are available for callers that need fixed IDs, roles, and
dependencies.

The automatic planner retries another advertised node/model if the selected
coordinator model fails. If no candidate succeeds, the error is returned.
The default heuristic recognizes enumerations (including semicolon-delimited
topic lists), comparisons, “for each” work, supported action conjunctions, and
explicit sequential cues. It infers researcher/reviewer roles for supported
action wording and adds a final synthesis task for fan-out plans. The explicit
`heuristic_v1` mode retains the legacy heuristic. See the
[routing and decomposition evaluation](./routing-evaluation.md) for the
synthetic fixture results. Plans are checked for missing dependencies and
cycles. Subtasks can specify roles, expected output, JSON output contracts,
required fields, and bounded retries. Set
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

Ready groups are included in task-planner and subtask scheduler inventory as
one logical model on the registered, healthy head. Planning, dispatched work,
and synthesis use the managed head's OpenAI chat endpoint; they do not request
standalone execution of the complete model from a member node. The group's
request gate serializes these calls with direct inference. Persistent group
reservations are not acquired again for each subtask.

Internal shard generation requests SSE and assembles the assistant content;
reasoning-only deltas are counted for progress but are not returned as the
answer or planner JSON. Premature stream termination, upstream stream errors,
and token-limit truncation fail explicitly instead of returning partial output.
The node proxy flushes response headers and every copied chunk. Planning,
generation, and synthesis HTTP calls honor `dispatch_timeout_seconds`; shard
queue waits are included and caller cancellation is preserved.

Use `POST /v1/chat/completions` or `POST /router/v1/chat/completions` with the
group's exact model name for explicit inference without automatic task planning.
The legacy `/api/generate?stream=true` surface buffers managed-shard generation
and returns its result as one SSE output event; use OpenAI streaming for
incremental tokens.

`DELETE /api/shards/{id}` is also forwarded by node-served dashboards. The
dashboard exposes deletion for failed cards. A successful delete returns `204`;
cleanup failures keep the group and return an error. Runtime installations and
original weights are retained. See [using a shard group](./dashboard.md#use-a-shard-group)
for dashboard steps and a PowerShell request.

`POST /api/shards/{id}/start`, `/stop`, and `/restart` require an empty body
and return `200` with a `ShardGroup`. These actions are also forwarded by node
dashboards. Stop drains active inference, stops head before workers, releases
capacity, and persists `stopped` without deleting the group. Start accepts
stopped/failed groups and replans using the saved model, pins, and previous head
against current memory before starting workers and head. Restart stops then
starts under one operation claim. Start-ready and stop-stopped are idempotent.
Concurrent operations, invalid states, or engine/model ownership conflicts
return `409`; missing group/action `404`, nonempty body/placement refusal `400`,
upstream/persistence failure `502`, and unavailable members/capacity `503`.

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


## Shard-group API examples

The same create operation is available through the API. For example, after
configuring the engines and choosing eligible node IDs, send this payload to
`POST /api/shards` (replace the model metadata and IDs with actual values):

```json
{
  "model": {
    "name": "my-sharded-model",
    "architecture": "llama",
    "parameters": 7000000000,
    "layer_count": 32,
    "hidden_size": 4096,
    "kv_heads": 8,
    "head_dim": 128,
    "vocab_size": 32000,
    "quantization": "q4_0",
    "file_size_bytes": 4000000000,
    "requested_context": 4096
  },
  "mode": "llamacpp-rpc",
  "pinned_nodes": ["worker-1", "worker-2"]
}
```

Save the JSON as `shard-group.json` and submit it with
PowerShell's `curl.exe` (replace the URL with the orchestrator's listener URL):

```powershell
curl.exe --fail-with-body `
  -H "Content-Type: application/json" `
  --data-binary "@shard-group.json" `
  "http://127.0.0.1:7450/api/shards"
```

A successful request returns `201` with the ready group. Check `GET /api/shards`
or `GET /api/shards/{id}` for the plan, rationale, members, and state.

Requests within a group are serialized. Member/process loss first produces `degraded`;
the next health sweep tears down the group and marks it `failed`. Neither
state accepts inference. Rebuild by creating a new group with the same model
name after fixing the cause; a failed group may remain for diagnosis.
`DELETE /api/shards/{id}` stops the head before workers and removes the record.
Restart recovery verifies every member before restoring readiness.
Automatic llama.cpp heads are also checked against the saved model's name and
weight metadata. Starting a saved group restores its original local GGUF model
if another stopped group reconfigured the shared automatic head. If those
weights are no longer available, startup fails explicitly rather than running a
different model under the saved name. Runtime health checks and inference reject
mismatched heads; stop and start the group to restore the intended model.

### Explicit shard chat request

To explicitly select the group instead of using automatic task planning,
   send an OpenAI-compatible request to the orchestrator's
   `POST /v1/chat/completions` with the exact model name shown on its card.
   For example, save this as `shard-chat.json`, replacing `model` with your
   ready group's model:

   ```json
   {
     "model": "Llama-3.2-3B-Instruct-Q4_K_M",
     "messages": [
       {"role": "user", "content": "<your prompt>"}
     ],
     "stream": false
   }
   ```

   ```powershell
   curl.exe --fail-with-body `
     -H "Content-Type: application/json" `
     --data-binary "@shard-chat.json" `
     "http://127.0.0.1:7450/v1/chat/completions"
   ```

The equivalent API calls use an empty request body:

```powershell
curl.exe --fail-with-body -X POST "http://127.0.0.1:7450/api/shards/GROUP_ID/stop"
curl.exe --fail-with-body -X POST "http://127.0.0.1:7450/api/shards/GROUP_ID/start"
curl.exe --fail-with-body -X POST "http://127.0.0.1:7450/api/shards/GROUP_ID/restart"
```

Each successful call returns `200` and the updated group. Start on a ready
group and stop on a stopped group are idempotent. Concurrent lifecycle
operations return `409`.
