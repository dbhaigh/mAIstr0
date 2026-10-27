# Dashboard guide

## Local Network discovery status

The Local Network panel labels discovery announcements as
**HMAC-SHA256 enabled** or **Unauthenticated (default)** for the local
instance. This reports whether the optional `discovery_hmac_secret` is
configured; it does not display the secret. HMAC authenticates discovery
announcements but does not replace node pairing, which still requires the
orchestrator's independently verified certificate fingerprint and one-use PIN.
Configure the same discovery secret on all cluster members to enable the
authenticated mode.

## Sharding

A managed shard group runs one model across multiple machines. It is different
from distributing independent tasks or replicating a whole model on each node.
The **Shard groups** panel under Job routing shows group state and placement
details. Ctrl-click a healthy node in the cluster view to configure a group;
the orchestrator plans placement, reserves member capacity, starts workers
before the head, and exposes a ready head through
`POST /router/v1/chat/completions` using the group's model name.

- `llamacpp-rpc`: compatible RPC-enabled llama.cpp worker and head builds;
  heterogeneous supported GPUs are permitted. The wizard automatically
  installs missing `ggml-rpc-server` workers. Configure a managed `llama-server`
  with a compatible build and model on at least one selected node.
  Pair the nodes and make their authenticated RPC
  relays reachable before creating the group.
- `vllm-mp`: Linux-only homogeneous GPU nodes, matching GPU counts and models,
  compatible vLLM builds, and a trusted private network. Configure a managed
  `vllm` on each node. The default topology uses tensor parallelism within a
  node and pipeline parallelism between nodes; non-head ranks are headless.
- Free **device** memory must fit weights, KV cache at the requested context,
  and a safety margin. Host RAM is not counted as GPU/NPU memory. The current
  group API does not enable CPU offload. NPU layer sharding requires explicit
  operator validation; a detected NPU is not proof of RPC support.

### Create a shard group from the dashboard

1. Start the orchestrator and worker nodes and pair them. For `llamacpp-rpc`,
   have a compatible GGUF model installed in Ollama or LM Studio on at least
   one selected node; the wizard installs the runtime and configures its head.
   Existing operator-configured `llama-server` heads are also supported.
   For `vllm-mp`, configure vLLM on every selected node. See
   [managed engine configuration](./configuration.md#managed-engine-processes).
2. Ctrl-click a healthy node in the cluster view to open **Create shard group**.
   Select the worker nodes and sharding mode. Every selected node must have the
   mode's managed worker engine. For llama.cpp RPC, selecting a node
   automatically downloads and verifies a missing worker; idle, automatically
   installed workers check for newer releases. Choose official llama.cpp or
   the custom XDNA release source. Unsupported platforms, missing releases,
   and failed verification are shown as errors, not skipped.
3. Choose the **Model head node**, a model, and its requested context length.
   For llama.cpp RPC, the picker reads existing managed heads and local Ollama
   or LM Studio GGUF files on that node. It configures the bundled matching
   `llama-server` when creating the group, without downloading or copying weights.
   For vLLM, it inspects configured local/cached Hugging Face metadata and weights.
   Models are offered only when their architecture and weight size can be read.
4. Choose **Create and start group**. The orchestrator calculates the placement
   from each device's current free memory, holds back a 2 GiB safety margin per
   device, and refuses the plan if the model weights and KV cache do not fit.
   For `llamacpp-rpc`, layer ranges are weighted by device capacity; vLLM
   requires homogeneous node resources and uses the configured parallel layout.
   The installed runtime refreshes actual free accelerator memory, including
   Vulkan devices. Unknown device memory is not replaced with host RAM and
   cannot receive layers.
5. The dialog reports the ready group and its assigned layers. The **Shard
   groups** panel under Job routing shows the group state and placement details.

The same create operation is available through the API; see the [shard-group API examples](./api.md#shard-group-api-examples).

### Use a shard group

If task planning or assignment reports **no compatible model**, mAIstr0 first
checks ready cluster models, then tries readable local GGUF weights on an idle
node. If none fit, it automatically downloads a recommended public Hugging Face
GGUF, verifies its size and SHA-256, installs a missing official llama.cpp
runtime, and starts a managed single-node group before retrying assignment.
This also works when no model is available for the initial planning step.
Active groups and operator-configured heads are never replaced.

The initial download recommendations are Qwen2.5 Instruct 3B and 7B Q4_K_M.
Downloads require internet access and roughly 2.1 GB or 4.7 GB of disk space
respectively; temporary download space is required until verification finishes.
The source repositories' model licenses still apply.
Only single-file public GGUF models are supported, not gated repositories,
arbitrary URLs, model scripts, or other weight formats. Placement still requires
known free GPU memory with the normal safety reserve and KV-cache allowance;
host RAM is not substituted for GPU memory. If every node is busy or the task
requires unsupported capabilities (for example vision), recovery returns an
explicit error rather than replacing a model or weakening the requirements.
See [model acquisition](./api.md#compatible-model-acquisition) for manual
download and local-model selection.

1. Open **Shard groups** under **Job routing** and check that the group is
   **ready**. A failed or degraded group cannot answer prompts.
2. In dashboard **mAIstr0** chat, submit your request normally. Ready shard
   models are eligible for automatic planning, subtask execution, and final
   answer synthesis, alongside ordinary cluster models. Chat does not pin a
   specific group; scheduler selection still applies.
3. To explicitly select the group instead of using automatic task planning,
   send an OpenAI-compatible request to the orchestrator's
   `POST /v1/chat/completions` with the exact model name shown on its card.
   For example, save this as `shard-chat.json`, replacing `model` with your
   ready group's model:

   ```json
   {
     "model": "Llama-3.2-3B-Instruct-Q4_K_M",
     "messages": [
       {"role": "user", "content": "Rank ten classical composers and explain each ranking with examples."}
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

The group is **one logical model**. Send prompts to the orchestrator, not to
individual shard workers. All requests share the head and are serialized,
including planning and synthesis; more shard members increase usable model
memory, not the number of independently running copies.

If chat reports `select planning model: scheduler: no compatible model for
subtasks: plan`, the planner could not find an eligible model; this is not an
error in the composer prompt. Update/restart the orchestrator to a build with
shard-task routing, verify that a group is **ready**, and submit again. Older
builds could route direct OpenAI requests to a shard head but could not use it
for task planning. If no group is ready, resolve its startup error or make a
normal cluster model available first.

If a working head times out with `Client.Timeout exceeded while awaiting headers`,
older task routing requested a non-streaming response: the head could be busy
generating without returning the completed answer before the fixed ten-minute
limit. Current builds consume streaming responses internally and flush proxy
chunks promptly. For a slow distributed model, set `dispatch_timeout_seconds`
to `1800` in the orchestrator configuration and restart with
`-orchestrator-config <path>` to allow thirty minutes. This also applies to
planning and synthesis. See [generation timeouts and progress](./configuration.md#capacity-timeouts-and-synchronization).
The application log reports elapsed time and output/reasoning byte counts,
not private generated text. Low GPU utilization alone does not identify the
bottleneck: inspect the head's prompt-evaluation and generation timings and
worker/network behavior before assuming the model has hung.

Failed cards offer **Delete failed group**. This stops any remaining group
processes, releases its reservations, and removes the saved group. Errors are
displayed and the card is retained if cleanup fails. Deletion does not remove
downloaded runtimes or original model weights. Fix the reported cause and
Ctrl-click eligible nodes to create a replacement group.

### Stop, start, or restart a shard group

Use the buttons on its card under **Job routing → Shard groups**:

- **Stop** (ready/degraded): wait for active inference to finish, stop the head
  followed by the workers, and release member capacity. The group becomes
  **stopped**; its model, placement history, pinned nodes, and ID remain saved.
- **Start** (stopped/failed): retry the saved group with the same model and ID.
  Placement is recalculated using current device memory, preserving pinned
  nodes and the previous head selection when available. Workers start before
  the head. A ready group is eligible for inference and automatic tasks again.
- **Restart**: drain and stop the group, then replan and start it. It does not
  restart just the head or create a second group. A failed group can also be
  retried this way.

Controls are disabled while an operation is in progress. Startup may take up
to ten minutes for a large head. A stopped group is unavailable for new prompts
and remains stopped across application restarts. Stop/restart waits rather
than aborting active inference; queued/new requests may receive an unavailable
error once stopping begins. Cleanup/startup failures are shown on the card;
resolve the cause and retry. Engines in another group cannot be taken over.

The equivalent API calls use an empty request body:

```powershell
curl.exe --fail-with-body -X POST "http://127.0.0.1:7450/api/shards/GROUP_ID/stop"
curl.exe --fail-with-body -X POST "http://127.0.0.1:7450/api/shards/GROUP_ID/start"
curl.exe --fail-with-body -X POST "http://127.0.0.1:7450/api/shards/GROUP_ID/restart"
```

Each successful call returns `200` and the updated group. Start on a ready
group and stop on a stopped group are idempotent. Concurrent lifecycle
operations return `409`.

The Go process integration tests prove control-plane lifecycle and routing with
fake engines, not tensor correctness, hardware compatibility, or throughput.
Record real-machine evidence in the
[hardware validation checklist](./hardware-validation.md). See
[managed process configuration](./configuration.md#managed-engine-processes),
the [planner](./shard-planner.md), and
[troubleshooting](./sharding-troubleshooting.md).
### Use a shard group

If task planning or assignment reports **no compatible model**, mAIstr0 first
checks ready cluster models, then tries readable local GGUF weights on an idle
node. If none fit, it automatically downloads a recommended public Hugging Face
GGUF, verifies its size and SHA-256, installs a missing official llama.cpp
runtime, and starts a managed single-node group before retrying assignment.
This also works when no model is available for the initial planning step.
Active groups and operator-configured heads are never replaced.

The initial download recommendations are Qwen2.5 Instruct 3B and 7B Q4_K_M.
Downloads require internet access and roughly 2.1 GB or 4.7 GB of disk space
respectively; temporary download space is required until verification finishes.
The source repositories' model licenses still apply.
Only single-file public GGUF models are supported, not gated repositories,
arbitrary URLs, model scripts, or other weight formats. Placement still requires
known free GPU memory with the normal safety reserve and KV-cache allowance;
host RAM is not substituted for GPU memory. If every node is busy or the task
requires unsupported capabilities (for example vision), recovery returns an
explicit error rather than replacing a model or weakening the requirements.
See [model acquisition](./api.md#compatible-model-acquisition) for manual
download and local-model selection.

1. Open **Shard groups** under **Job routing** and check that the group is
   **ready**. A failed or degraded group cannot answer prompts.
2. In dashboard **mAIstr0** chat, submit your request normally. Ready shard
   models are eligible for automatic planning, subtask execution, and final
   answer synthesis, alongside ordinary cluster models. Chat does not pin a
   specific group; scheduler selection still applies.
3. To explicitly select a ready group, use the [OpenAI-compatible API example](./api.md#explicit-shard-chat-request).

The group is **one logical model**. Send prompts to the orchestrator, not to
individual shard workers. All requests share the head and are serialized,
including planning and synthesis; more shard members increase usable model
memory, not the number of independently running copies.

If chat reports `select planning model: scheduler: no compatible model for
subtasks: plan`, the planner could not find an eligible model; this is not an
error in the composer prompt. Update/restart the orchestrator to a build with
shard-task routing, verify that a group is **ready**, and submit again. Older
builds could route direct OpenAI requests to a shard head but could not use it
for task planning. If no group is ready, resolve its startup error or make a
normal cluster model available first.

If a working head times out with `Client.Timeout exceeded while awaiting headers`,
older task routing requested a non-streaming response: the head could be busy
generating without returning the completed answer before the fixed ten-minute
limit. Current builds consume streaming responses internally and flush proxy
chunks promptly. For a slow distributed model, set `dispatch_timeout_seconds`
to `1800` in the orchestrator configuration and restart with
`-orchestrator-config <path>` to allow thirty minutes. This also applies to
planning and synthesis. See [generation timeouts and progress](./configuration.md#capacity-timeouts-and-synchronization).
The application log reports elapsed time and output/reasoning byte counts,
not private generated text. Low GPU utilization alone does not identify the
bottleneck: inspect the head's prompt-evaluation and generation timings and
worker/network behavior before assuming the model has hung.

Failed cards offer **Delete failed group**. This stops any remaining group
processes, releases its reservations, and removes the saved group. Errors are
displayed and the card is retained if cleanup fails. Deletion does not remove
downloaded runtimes or original model weights. Fix the reported cause and
Ctrl-click eligible nodes to create a replacement group.

### Stop, start, or restart a shard group

Use the buttons on its card under **Job routing → Shard groups**:

- **Stop** (ready/degraded): wait for active inference to finish, stop the head
  followed by the workers, and release member capacity. The group becomes
  **stopped**; its model, placement history, pinned nodes, and ID remain saved.
- **Start** (stopped/failed): retry the saved group with the same model and ID.
  Placement is recalculated using current device memory, preserving pinned
  nodes and the previous head selection when available. Workers start before
  the head. A ready group is eligible for inference and automatic tasks again.
- **Restart**: drain and stop the group, then replan and start it. It does not
  restart just the head or create a second group. A failed group can also be
  retried this way.

Controls are disabled while an operation is in progress. Startup may take up
to ten minutes for a large head. A stopped group is unavailable for new prompts
and remains stopped across application restarts. Stop/restart waits rather
than aborting active inference; queued/new requests may receive an unavailable
error once stopping begins. Cleanup/startup failures are shown on the card;
resolve the cause and retry. Engines in another group cannot be taken over.

Equivalent API lifecycle calls are in the [shard-group API examples](./api.md#shard-group-api-examples).

The Go process integration tests prove control-plane lifecycle and routing with
fake engines, not tensor correctness, hardware compatibility, or throughput.
Record real-machine evidence in the
[hardware validation checklist](./hardware-validation.md). See
[managed process configuration](./configuration.md#managed-engine-processes),
the [planner](./shard-planner.md), and
[troubleshooting](./sharding-troubleshooting.md).


## Sharding troubleshooting

Start with group `state`, `error`, and `rationale`, then inspect managed-engine
status on the member nodes. Common causes are missing RPC builds or relays,
untrusted vLLM interfaces, insufficient free device memory, incompatible
models/backends, and native Windows vLLM launches. A `degraded` group is not a
reduced-capacity service: wait for teardown, fix the cause, and rebuild.
The [troubleshooting guide](./sharding-troubleshooting.md) includes
safe diagnostic steps and evidence to record before retrying.


## Asking mAIstr0 about cluster status

The dashboard's **Ask mAIstr0** action answers read-only cluster-status
questions directly from the node inventory, health/capacity, recent-task,
shard-group, and version APIs. It does not call an LLM or create a task for
read-only questions about model availability, node health, or running engines. The answer includes reported model
names per node and their intersection across all online nodes. An offline
node or unavailable engine inventory makes availability on **all registered
nodes** unknown, not a successful match. Identical advertised model names are
not proof of identical weights, quantization, or revisions.

This is a snapshot report, not a new engine probe or a diagnosis of an
unreported failure. Missing telemetry and failed API reads are marked
explicitly. It also works when no nodes are healthy. Requests that change
state, such as installing models or restarting workers, remain on the task
path; ambiguous/general requests still use the workflow planner.

For work requiring automatic planning, malformed JSON or invalid task graphs
get one corrective retry on the selected model before trying another eligible
node/model. If every attempt fails, mAIstr0 reports the planning error rather
than silently substituting a heuristic plan. Model output is not echoed into
corrective prompts or logged.
