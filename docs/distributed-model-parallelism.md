# Distributed Model Pipeline Parallelism

Status: the simulated token-by-token pipeline path is implemented. Production
llama.cpp and vLLM stage backends are not implemented.

## Goal and boundary

Pipeline Parallelism (PP) lets one logical model run across multiple LAN nodes,
with each node executing a contiguous range of layers. To clients, the elected
orchestrator remains one OpenAI-compatible host. This extends the current
full-model-per-engine routing model; existing `engine.Engine` adapters do not
execute partial layer ranges. PP therefore needs an explicit stage-capable
engine/runtime contract rather than treating any installed model as shardable.

## Data model and node readiness

- **`ShardedModel`** describes the logical model: stable `logical_id`, source
  model ID and immutable revision, total layer count, and the model metadata
  required to interpret inputs and activations (including tokenizer and model
  configuration identity).
- **`Stage`** identifies an ordered stage by node ID, engine, and half-open
  layer range `[start_layer, end_layer)`. The ranges must be ordered, contiguous,
  non-overlapping, and cover `[0, total_layers)`.
- **`PipelinePlan`** binds a logical model revision to its ordered stages and a
  plan version/coordinator epoch. Node addresses are resolved from the current
  healthy registry rather than trusted as permanent plan data.

Node status additively advertises `supported_shard_models`,
`max_layers_it_can_host`, and `current_stage_assignments`. Configured model
names are an allow-list and the configured maximum is an upper bound; neither
one proves that an engine can execute partial layers. Readiness is intersected
with the selected engine's verified protocol-v2 `pipeline_stage` capability
document, obtained from its local `GET /stage/capabilities` endpoint. Loaded
assignments also contribute to status. The placer rechecks model support,
engine capability, node health, and capacity before dispatch.

The orchestrator registers logical models through its `sharded_models` startup
configuration. It persists each resulting plan in a local sidecar keyed by
`logical_id`, then reuses it while every stage remains healthy and ready.

## Placement and client-visible model

The currently authoritative elected orchestrator owns model plans. A dedicated
pipeline placement path matches readiness entries for the same model revision
and partitions all layers into supported contiguous ranges. The existing
subtask scheduler assigns a complete model to a node and is not a pipeline
placer. A plan is published only when every stage is eligible.

`ShardedModel.logical_id` is the sole client-facing ID. `/v1/models` lists that
ID once, regardless of stage count, and a replan onto other nodes does not
change it. `/v1/chat/completions` resolves that ID to a complete current plan
and dispatches through the stages; stage IDs, node IDs, and activation data are
never exposed as models. Existing non-sharded model routing remains unchanged.
If no complete healthy plan exists, retain the logical ID but return an
unavailable error for inference.

The LAN election ranks visible candidates and is not consensus. PP uses the same
current-authority checks. Plans carry a coordinator epoch as metadata; this is
not a consensus term or a guarantee of cluster-global uniqueness. A network
partition can still produce independent local plans.

## Stage execution, activation, and streaming

Use the existing authenticated orchestrator-to-node mTLS HTTP transport for
stage calls; do not add unauthenticated LAN stage endpoints or require new
node-to-node trust. Protocol v2 opens request-local state with `/stage/open`,
prefills with `/stage/prefill`, performs one token step at a time with
`/stage/decode`, and releases state with `/stage/close`. Prefill/decode use a
binary envelope containing a 4-byte big-endian header length, JSON metadata,
and an optional typed activation tensor payload. Only activations cross stage
boundaries; each stage retains its own request-local KV cache. The legacy
`/stage/forward` endpoint remains for compatibility but is not the v2 dispatch
path.

The final stage returns token IDs and UTF-8 text. The orchestrator adapts these
to the existing OpenAI SSE writer or non-streaming completion response. The
simulated engine exercises this protocol; the ordinary llama.cpp and vLLM
OpenAI-compatible adapters do not implement it. A failed request does not
migrate live KV state to a replacement node and must restart from a fresh
prefill.

Keep `POST /v1/chat/completions` request and response shapes, including the
existing `stream` option. Non-streaming requests return the normal completion;
streaming requests emit only generated text as OpenAI SSE chunks and finish
with `[DONE]`. Intermediate activations and stage events never enter the client
stream. Propagate client cancellation through all stage calls. Once response
bytes are committed, a stage failure ends the stream with an error event where
possible; it cannot transparently retry the partially emitted completion.

## Failure and capacity

Before starting inference, reserve capacity for the entire plan, not one stage
at a time. The orchestrator's capacity gate acquires one global slot and a
local reservation on every participating node under a stable lock order; if
any stage cannot be reserved, it releases the whole attempt and waits or
returns unavailable. Node execution gates still enforce their local
concurrency limits while each stage runs. Hold the orchestrator reservation
through generation and release it on completion, cancellation, or timeout.

If a stage goes offline or loses its reservation, cancel the in-flight pipeline
and return an unavailable error. Do not retry on an arbitrary node or continue
with missing KV state. Mark the plan unusable, then place a complete
replacement plan for later requests if readiness and capacity permit. Keep the
logical model ID stable through this process; reject work rather than silently
fall back to a different model or a partial pipeline.

Protocol-v2 open requests are idle leases on the node. Prefill and decode
activity refreshes the lease; after ten idle minutes, the node closes the
runtime request and releases its stage lease and execution capacity. The
orchestrator also attempts a bounded best-effort close when an open
acknowledgement is uncertain. Automatic replanning drains active orchestrator
requests and unloads reachable prior-plan stages before loading replacement
assignments.

## Contract changes and v1 exclusions

Keep changes additive: node shard-readiness and engine `pipeline_stage`
capability fields are optional, and older nodes that omit them remain valid but
are not PP candidates. Define the engine capability, tensor-envelope, stage
request, and plan schemas alongside the implementation; do not make existing
node-status, engine, model, or OpenAI fields newly required. The pipeline plan
and stage protocol are internal to the trusted cluster. A stage-capable runtime
must implement the protocol-v2 capability, load/unload, open, prefill, decode,
and close operations; ordinary OpenAI-compatible inference endpoints are
insufficient.

V1 excludes Tensor Parallelism, training, and Mixture-of-Experts expert
parallelism. It also does not promise live stage migration or a new public
pipeline-management API.
