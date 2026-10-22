# mAIstr0

mAIstr0 is a local-first orchestrator for distributing LLM work across machines
on a LAN. The orchestrator discovers worker nodes, routes prompts to models
available on those nodes, and provides a dashboard and OpenAI-compatible API.

## Quick start

Run `maistr0.exe` on Windows, or the `maistr0` binary on Linux. By default,
instances first look for an active orchestrator and join it as a node. If no
orchestrator is active, instances announce candidacy for 15 seconds (one full
LAN discovery lease); the candidate with the highest capability score wins,
with the lowest stable ID breaking ties. The winner starts both the
orchestrator and a local worker node; other simultaneous starters join as
nodes. This lets the orchestrator host contribute its own compute resources to
cluster work.

LAN election is based only on announcements currently visible to each instance.
During a network partition, each side may therefore elect and serve its own
local winner; there is no quorum. A node that loses four consecutive
five-second health checks to its current orchestrator starts a background
failover election. The winning trusted cluster node can promote itself using
its existing TLS identity and paired-member roster, so surviving members can
re-register without repeating pairing. Untrusted discovery peers are not
eligible to take over, and new cluster members still require authorization. See
[LAN election and partitions](docs/lan-election.md) for the visibility and
failover behavior.

`--role orchestrator` and `--role both` use the same election. LAN discovery
must be available to start an orchestrator; coordinator startup fails closed
when discovery is disabled. `--role node` always starts a worker. To pair a
node, open the
orchestrator dashboard's Setup panel to view its one-time PIN, then open the
node dashboard's Setup panel and enter the orchestrator's HTTPS address and
PIN. The command-line pairing option is also available:

```powershell
.\maistr0.exe --role orchestrator
.\maistr0.exe --role node --orchestrator-addr https://192.168.1.10:7450 --pairing-pin "<six-digit-pin>"
```

```sh
./maistr0 --role orchestrator
./maistr0 --role node --orchestrator-addr https://192.168.1.10:7450 --pairing-pin "<six-digit-pin>"
```

The default ports are `7450` for the orchestrator and `7451` for a node. Use
`--no-discovery` to disable LAN discovery for node-only instances or
`--no-browser` to prevent opening the dashboard automatically. Orchestrator
election requires discovery. Node and orchestrator JSON configuration paths
can be set with `--node-config` and `--orchestrator-config`.
Use `--print-config` with the same role and config flags to print normalized
JSON and exit without starting listeners or the dashboard.

Concurrency can be tuned in those JSON files. The orchestrator's
`max_concurrent_dispatches` setting defaults to `16` and caps subtask execution
across the cluster. Its `dispatch_timeout_seconds` setting defaults to `600`
(10 minutes) and limits each subtask's dispatch and retry attempts after its
dependencies are complete. A node's `max_concurrent_tasks` setting defaults to `2`;
the node advertises its active and remaining capacity in status/heartbeat
updates, and both the orchestrator and node enforce that limit. These settings
only affect new in-memory scheduling; they do not change task or memory storage
formats.

Cluster traffic uses mutual TLS 1.3 or later with persistent per-instance
certificates. The orchestrator displays a six-digit, one-use pairing PIN that
expires after 30 minutes; after each successful pairing it creates the next
PIN. PIN values are not written to logs. Use **Generate new PIN** in the Setup
panel to replace an expired or consumed PIN. The dashboard provides a direct
node pairing form and can send TLS-identified invitations to LAN-discovered
nodes; a node must confirm locally
before the invitation's PIN is used to join. Alternatively, pass the PIN to a
new node with `--pairing-pin` or the node config's `pairing_pin`
setting. Paired nodes exchange certificate pins over the authenticated
connection. Orchestrator-to-orchestrator pairing is disabled to keep a single
coordinator per LAN cluster. Plain HTTP is restricted to loopback for the
local dashboard; use HTTPS for configured cluster addresses. The combined
orchestrator and local worker are paired automatically. A machine that loses
the election joins as a node; provide the elected orchestrator's one-use PIN
with `--pairing-pin` for automatic secure enrollment, or pair it later from
the node dashboard's Setup panel.
Local HTTP dashboard requests are restricted to localhost/loopback Host values
and same-origin browser requests. Expired local certificates rotate
automatically, but fingerprint pinning means peers must be paired again after
an identity certificate changes. See the [security model](./docs/security.md)
for the trust boundary, certificate lifecycle, request limits, and operational
guidance.
If a running orchestrator yields, its local dashboard redirects to the worker
dashboard so pairing and invitation acceptance remain available. Existing
trust in the old orchestrator does not authorize joining the new one; use the
new leader's PIN or accept its invitation locally.
The **Local network** tab lists LAN-discovered instances regardless of pairing,
plus online cluster nodes and the local worker. Membership is labeled
separately from visibility; an associated orchestrator is shown only when known,
not inferred from whichever orchestrator happens to be visible first.
From a node dashboard, use the **Join [orchestrator]'s cluster** button on a
discovered orchestrator in **Local network**, or right-click its card and
choose the same action, to pair this node directly with that specific
coordinator. Enter its one-use PIN when joining across subnets; same-subnet
pairing can proceed without a PIN. The selected orchestrator identity is
checked against its TLS certificate before pairing. The node can initiate this
join even if the orchestrator has not discovered the node yet.
Application logs are appended to `maistr0.log` in the user's configuration
directory (`%APPDATA%\maistr0\maistr0.log` on Windows). Choose **View Logs**
from the tray icon's right-click menu to inspect pairing and TLS diagnostics,
including detailed task-history and memory-sync diagnostics, even when the
console is hidden. Routine sync messages are kept in the log file rather than
printed to the console. Logs roll over daily; dated archives are kept for
seven calendar days and are available through **Open Logs Folder** in the same
menu.

`GET /api/status` reports whether persistent memory is available. If its
database cannot be opened, the dashboard shows a warning that conversation
history and learned data will not survive restarts; the process continues
without persistent memory.

## API contracts and porting

The [OpenAPI specification](./contracts/openapi.yaml), [JSON Schemas](./contracts/schemas/),
and [porting guide](./contracts/PORTING.md) describe the language-neutral HTTP,
configuration, task, scheduler, node, discovery, and memory contracts.

## Architecture and concurrency

Scheduler assignments use the deterministic, lock-free scorer in
[`internal/scheduler/score`](./internal/scheduler/score/score.go); eligibility
and capacity prefilters run before scoring. The lock-free guarantee applies to
the score calculations, which consume an already-resolved learned bias; it does
not claim that every surrounding scheduler or dispatch operation is lock-free.
Workflow dispatch and direct
OpenAI-compatible routing share the [`CapacityGate`](./internal/workpool/pool.go),
which reserves per-node and global slots until released. The
[`ClusterHealth`](./internal/orchestrator/cluster_health.go) view model is the
single source of truth for capacity and readiness in both the API and SSE
dashboard snapshots.

Keep critical sections narrow and avoid calling other components or doing
network work while holding state locks. The task-manager write hierarchy is
`persistGate` → per-task `persistMu` → `Manager.mu`; release `Manager.mu`
before waiting for a task lock. Node-agent engine scans acquire
`engineScanMu` before `enginesMu`, while inventory refresh follows
`refreshMu` → `modelMu` → agent state. Pass request and task contexts through
dispatch and capacity waits. Workpool `Options` controls whether submitter
cancellation continues to propagate after a job is accepted; pool shutdown
always cancels running work.

## Orchestrator agent backends

The interactive orchestrator uses [Pi](https://github.com/earendil-works/pi)
by default. Pi and the built-in DeepSeek harness share the Go agent-loop
control flow while retaining their own model adapters and message formats.
Pi selects coordinator models from healthy nodes in the local mAIstr0 cluster,
and does not send prompts to OpenAI or another cloud provider. No API key,
Node.js, or Pi CLI installation is required. Choose a specific local
coordinator model in the dashboard or leave it on Auto for cluster routing.
It does not include Pi's standalone coding CLI/TUI, filesystem or shell tools,
or extension compatibility. Both harnesses share mAIstr0's cluster, model,
task, web, math, and persistent-memory tools. To use DeepSeek instead,
start mAIstr0
with `--harness deepseek`. The Interactive Orchestrator sidebar can switch the
default harness at runtime; this applies to new conversations, while existing
conversations stay on their original harness. Runtime changes last until the
orchestrator restarts; use the `harness` setting in the orchestrator
configuration to choose the startup default. The default `full` mode enables
interactive harnesses and persistent learning. Set `"mode": "minimal"` in the
orchestrator configuration to omit harness and memory/learning endpoints and
their storage; cluster task history and the `/router/...` and `/v1/...`
routing APIs remain available.

```json
{
  "mode": "full",
  "harness": "pi"
}
```
Interactive session history for both backends is stored in the orchestrator's
memory database and restored when it restarts. Deleting a conversation from
the dashboard also removes its persisted history.

## Dashboard

The dashboard provides three views:

- **Interactive Orchestrator** for conversational requests, tool activity,
  cluster-aware model selection, runtime harness selection, and the shared
  tool catalog available to both Pi and DeepSeek. In-flight agent turns can be
  interrupted at any time via the **Interrupt** button or the API
  (`POST /api/agent/sessions/{id}/interrupt`), cleanly halting reasoning loops,
  cancelling downstream node model dispatches, and preserving session state.
  Both harnesses can hand multi-step work to the same persistent `execute_workflow`
  engine instead of coordinating worker calls only in chat history; workflows
  triggered by an agent are cancelled if the parent session is interrupted.
- **Cluster & Workload** for node health, discovered LAN peers, model lists,
  active tasks, and routing. Its Overview has **Available nodes** and
  **Recent activity** tabs for node status and current/recent task activity.
  Selecting a subtask assignment switches to Available nodes and highlights
  its target node; known members remain visible when offline. Its Setup page
  is organized into **Pairing access**, **Available nodes**, **Inference readiness**,
  and **Model catalog** tabs. Workspace, Overview, and Setup tabs support arrow
  keys, Home, and End. Content sections expand with their content and scroll
  cleanly without compressing or overlapping.
  Pending, running, completed, failed, cancelled, and unknown subtasks are
  counted separately. Tasks awaiting human approval remain visible as active
  work. Detailed Nodes, Workload, Local network, Tasks, Setup,
  and Batch submit panels remain available as workspace tabs. Node details
  include GPU, NPU, CPU, and memory utilization history when the host supports
  those metrics. On Windows, NPU utilization is collected from the NPU's
  DXCore-identified GPU Engine counters when available; unavailable telemetry
  is identified rather than treated as zero.
- **Memory & Learning** for cluster experience, recommendations, and learned
  facts.

Joined nodes synchronize experiences, facts, dialogues, and routing history
with the orchestrator every 30 seconds. Standalone peer clusters use the
elected node to relay memory between members every 15 seconds. Memory records
carry optional origin and sequence metadata; version 1 and newer peers exchange
only records newer than their persisted per-peer watermarks. Protocol version 2
adds cursor-based paging for large snapshots while keeping the upper watermark
fixed for the duration of a transfer. Intermediate pages do not advance the
receiver's peer watermark. Older peers retain the unpaged compatibility path,
and a peer whose watermark is ahead of the sender triggers a full snapshot.
Legacy memory databases are backfilled when opened without changing their
existing record fields.

Task history is synchronized separately every 15 seconds; the orchestrator
starts its task-history loop after a short startup delay. Version 1 and newer
task sync sends only records newer than each peer's watermark, while older
peers receive full snapshots. Task records also carry optional origin and
sequence metadata, and legacy task databases are backfilled automatically.
Memory and task-history requests default to a 64 MiB per-page limit; larger
histories transfer in multiple bounded pages. A single record larger than the
configured limit is rejected explicitly. Sync transfers use a dedicated HTTP
timeout, configurable with the defaults described in
[Configuration validation](#configuration-validation). Task-history and memory-sync diagnostics are retained in the rolling
log without being printed to the console. Dispatch and sync log records include
request/attempt IDs, node/model/engine, elapsed time, and a failure class where
applicable.
The scheduler uses successful and failed node/model/task outcomes to improve
future assignments.

`GET /metrics` exposes dependency-free Prometheus text metrics for active tasks,
dispatch queue depth, reserved capacity, per-peer sync watermark lag, engine
inventory freshness, successful pairing events, scheduler assignment latency,
dispatch attempts/retries, sync pages, transparent-router queue depth, attempts,
failures by bounded class, request and time-to-first-byte histograms, and active
router streams. `GET /api/metrics` remains as a compatible alias. The endpoint
uses the existing HTTP mux, so it follows the same loopback-only plaintext and
trusted-peer mTLS rules as the rest of the API.
Sync lag is the maximum missing source-sequence count for a paired peer; `-1`
means the value is unavailable. Inventory age is the age of the oldest
successful report; `-1` means no successful inventory has been reported.
Metrics have no per-node labels, keeping their cardinality bounded.

### Reverse proxies and service managers

For dashboard access, a reverse proxy on the orchestrator host can forward to
the loopback-only HTTP listener. Restrict the proxy with your organization’s
authentication or a private network; the local listener assumes requests come
from the same host. Disable response buffering for `/api/events` so SSE updates
arrive promptly. Cluster nodes must continue to connect directly to the
orchestrator’s HTTPS/mTLS listener; do not terminate or replace the client
certificate identity used for cluster traffic.

Example Nginx location for an authenticated dashboard proxy:

```nginx
location / {
    auth_basic "mAIstr0";
    auth_basic_user_file /etc/nginx/maistr0.htpasswd;
    proxy_pass http://127.0.0.1:7450;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_buffering off;
    proxy_read_timeout 660s;
}
```

Prometheus on the orchestrator host can scrape `http://127.0.0.1:7450/metrics`.
Remote scrapers should use a trusted client certificate and the HTTPS listener,
or a separately authenticated, network-restricted proxy.

The systemd example can be installed as a user service in
`~/.config/systemd/user/maistr0.service`; edit `ExecStart` for the installed
binary path, then run `systemctl --user daemon-reload` and
`systemctl --user enable --now maistr0`. For launchd, edit the binary path in
[`com.maistr0.agent.plist`](./packaging/launchd/com.maistr0.agent.plist), copy it
to `~/Library/LaunchAgents/`, and load it with:

```sh
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.maistr0.agent.plist
```

The Cluster & Workload **Setup** tab checks each node's engine and model
readiness, refreshes an empty model registry, and links to node settings for
engine configuration and default-model selection.
On worker-only nodes, local model inspection and updates run directly without
an HTTPS request back to the worker itself. Remote node requests still require
paired certificate pins.
Its model catalog is a curated, bundled list of pull-ready Ollama names with
descriptions and searchable task/capability tags for chat, coding, reasoning,
vision, and other common workloads; it does not fetch catalog metadata or send
prompts to a remote catalog service. Choose an online target node and its
download-capable engine before installing. Installed models are shown as
either usable or disabled for scheduling.
When a submitted workflow has a subtask with no compatible model anywhere in
the cluster, task assignment can automatically pull a matching model from the
bundled Ollama catalog. It chooses only healthy nodes reporting Ollama and
enough estimated available system RAM (using fresh memory telemetry when
available) or fresh GPU memory for the model footprint, refreshes that node's
inventory, and retries assignment. A task request may therefore wait for a
model download; if no catalog model fits or a pull fails, the task returns an
unavailable error. Automatic provisioning does not pull custom model names or
use non-Ollama engines.
Task details show the scheduler's score components and whether accelerator
telemetry was fresh enough to influence the assignment; telemetry older than
30 seconds is ignored for routing. Scheduler load includes active node work and
workflow subtasks waiting on dependencies, with node-reported utilization
penalties applied only when the telemetry sample timestamp is current.

Nodes scan for local LLM servers at startup and refresh their model lists
automatically. Use **Rescan engines & models** on an online node card to detect
servers started or installed later, or **Rescan all nodes** to scan every online
node. Configured engines are retained alongside locally discovered engines.
Node cards report engine inventory status and model counts. Open **Configure
models** for per-engine model lists, the latest check and last successful check,
and any probe error. A successful empty inventory is distinct from an untested
or failed check; a failed check retains the last successful model list and marks
it as stale. Inventory checks query the engine's model list and do not guarantee
that a model can load or generate.
To edit a node's properties, right-click its card in the Nodes section. The properties dialog
can change its model engine and URL, default and disabled models, advertised
and orchestrator addresses, discovery, tray and browser behavior, and memory
path. Node identity and listen address are shown as read-only settings.

## Model engines

Nodes detect local Ollama, LM Studio, llama.cpp, and FastFlowLM services by
default. LM Studio is discovered at `http://127.0.0.1:1234`; llama.cpp is
discovered at `http://127.0.0.1:8080` or PAIR's managed-server port
`http://127.0.0.1:8081`. Both can also be configured at another
OpenAI-compatible server URL. vLLM, Hugging Face Text Generation Inference
(TGI), LocalAI, MLX-LM, SGLang, and other OpenAI-compatible servers can be configured
explicitly in a node configuration file. They are not auto-detected because
their default ports overlap and may not identify a unique server:

```json
{
  "listen_addr": ":7451",
  "max_concurrent_tasks": 2,
  "supported_shard_models": ["large-source-model"],
  "max_layers_it_can_host": 32,
  "sync_max_bytes": 67108864,
  "sync_timeout_seconds": 600,
  "orchestrator_addr": "https://192.168.1.10:7450",
  "pairing_pin": "<six-digit-pin>",
  "engines": [
    { "name": "ollama", "url": "http://127.0.0.1:11434" },
    { "name": "lmstudio", "url": "http://127.0.0.1:1234" },
    { "name": "llamacpp", "url": "http://127.0.0.1:8080" },
    { "name": "vllm", "url": "http://127.0.0.1:8000" },
    { "name": "tgi", "url": "http://127.0.0.1:8082" },
    { "name": "localai", "url": "http://127.0.0.1:8083" },
    { "name": "mlx", "url": "http://127.0.0.1:8084" },
    { "name": "sglang", "url": "http://127.0.0.1:30000" },
    { "name": "openai", "url": "http://127.0.0.1:8085" }
  ]
}
```

An orchestrator config can set the cluster-wide cap:

```json
{
  "max_concurrent_dispatches": 16,
  "dispatch_timeout_seconds": 600,
  "sync_max_bytes": 67108864,
  "sync_timeout_seconds": 600,
  "sharded_models": [
    {
      "logical_id": "large-assistant",
      "source_model_id": "large-source-model",
      "revision": "revision-1",
      "tokenizer_id": "tokenizer-1",
      "model_config_id": "model-config-1",
      "total_layers": 80
    }
  ]
}
```

The node shard-readiness settings are an allow-list and an operator-configured
layer ceiling, not proof of partial-layer execution. A node is eligible only
when its selected engine also returns a valid protocol-v2 capability document.
The orchestrator publishes only the logical model ID to OpenAI clients, places
complete layer plans on healthy ready nodes, and persists plans beside its
memory database for reuse.

## Distributed / sharded models

Declare a sharded model in the orchestrator's `sharded_models` configuration,
and configure each eligible node with the matching source model in
`supported_shard_models` plus its `max_layers_it_can_host` limit (examples are
shown above). These node settings are eligibility limits, not proof of runtime
support: an engine must pass the protocol-v2 `/stage/capabilities` probe and
advertise the requested model and layer capacity. The orchestrator places
contiguous layer ranges across healthy, ready nodes and exposes the configured
`logical_id` as one model to API clients. Layer ranges in a plan are half-open:
`[start_layer, end_layer)`.

Every selected node needs a runtime implementing the protocol-v2 stage
capability, load, request-open, prefill, decode, and close endpoints. The
protocol transfers typed activation tensors; request-local KV state stays on
the stage that owns it. The simulated stage engine is available for development
and tests. The ordinary llama.cpp and vLLM OpenAI-compatible adapters do not
yet provide production partial-layer backends, and standard llama.cpp RPC is a
different distributed-inference mode rather than a stage backend.

The current design targets trusted LAN clusters and Pipeline Parallelism (PP)
only. Tensor Parallelism (TP), training, and Mixture-of-Experts expert
parallelism are not supported. A stage failure cancels the request; it does not
migrate live KV-cache state or transparently resume generation on replacement
nodes.

## Configuration validation

Configuration is normalized before startup. Empty harness settings default to
`pi` (harness names are trimmed and lowercased), empty tray modes default to
`taskbar`, and zero or omitted concurrency settings use their documented
defaults. Negative concurrency and dispatch timeout values are rejected.
Memory and task-history transfers default to a 64 MiB page/request limit and a
10-minute timeout. Both node and orchestrator configs may override
`sync_max_bytes` (1 byte through 256 MiB) and `sync_timeout_seconds` (1 through
3600 seconds); omitted or zero values retain the defaults.
Orchestrator configs must keep LAN discovery enabled. The optional orchestrator `mode` setting accepts
`full` or `minimal`; omitted or empty values normalize to `full`.

Listen addresses must use `host:port` or `:port` syntax. A node's
`orchestrator_addr` and `advertise_addr` must be empty or HTTPS URLs. Configured
engine names may be `ollama`, `vllm`, `lmstudio`, `llamacpp`, `fastflowlm`,
`tgi`, `localai`, `mlx`, `sglang`, or `openai` (the generic
OpenAI-compatible adapter);
their `url` values must be HTTP or HTTPS URLs. OpenAI-compatible engines must
implement `GET /v1/models` and `POST /v1/chat/completions`. A configured
`pairing_pin` must contain exactly six digits. When `memory_path` is set, its
parent directory must be creatable; missing parent directories are created as
needed.

Invalid settings are reported individually and prevent startup with exit code
`2`.
The `--print-config` dry-run applies the same normalization and validation and
prints the effective node and/or orchestrator JSON without starting services.

The SSE cluster snapshot includes dispatch queue depth, active global slots,
dependency waiters, cumulative dispatch/dependency-wait duration, goroutine
count, and each node's active/max/remaining task capacity. Increase the
orchestrator cap to use more cluster-wide parallelism; increase a node's cap
only when its local engine and hardware can safely serve more concurrent
requests.

LM Studio, vLLM, llama.cpp, and FastFlowLM use the OpenAI-compatible
chat-completions API. mAIstr0 connects to an already-running `llama-server`;
installing the server and downloading/managing its model files remain the
responsibility of llama.cpp or another model manager. The scheduler adds a GPU preference to models advertised by GPU-equipped
nodes; this is a routing preference and does not guarantee that the engine
will use the GPU. On nodes with a detected NPU, FastFlowLM is preferred when
the same model is available from multiple local engines. Ollama and
OpenAI-compatible engines support token streaming. Other engines can still
serve requests, but their streamed responses may arrive as a single completed
chunk. Temperature and maximum-token settings are passed to engines that
support those options. Acceleration and model-format support remain properties
of the configured server; an engine adapter does not imply that a given
hardware device is supported.

### Distributed llama.cpp with RPC

llama.cpp RPC lets one `llama-server` use CPU/GPU devices on other hosts for
one distributed inference endpoint. Build `ggml-rpc-server` on each worker
and build the main `llama-server` with the RPC backend enabled. The worker and
main host must use compatible llama.cpp builds; select the accelerator backend
per machine (for example CPU, CUDA, HIP/ROCm, SYCL, Vulkan, OpenVINO, or MUSA).
Model support and accelerator coverage are determined by the llama.cpp build
and selected model format; mAIstr0 does not install drivers, compile
inference runtimes, or claim that an arbitrary NPU is supported.

For a native install, build llama.cpp with `-DGGML_RPC=ON` on both worker and
main hosts. Add one hardware backend on each host as needed, such as
`-DGGML_CUDA=ON` for NVIDIA or `-DGGML_HIP=ON` for AMD ROCm. Start
`ggml-rpc-server` on each worker and start `llama-server` on the main host with
`--rpc host1:50052,host2:50052`. The server exposes one OpenAI-compatible
endpoint. Configure only that endpoint as a `llamacpp` engine in the mAIstr0
node configuration; RPC worker hosts are compute workers, not separate model
replicas. Requests routed to that server use the remote devices through
llama.cpp.

For Linux Docker deployments, [packaging/llamacpp/compose.yaml](./packaging/llamacpp/compose.yaml)
provides separate `rpc-worker` and `inference` profiles. Use images built with
`GGML_RPC=ON`; choose CPU/CUDA/ROCm/SYCL/Vulkan/OpenVINO/MUSA build options and
GPU container access per host, then set the image and model/RPC environment
variables described alongside the file. For macOS and Windows, install and
run the native llama.cpp build with its supported backends and configure the
same `llamacpp` HTTP endpoint.

**Security:** llama.cpp RPC is experimental and insecure. Do not expose its
worker port to the public internet or untrusted networks, and do not use it in
sensitive environments. Restrict worker ports to trusted hosts with host
firewalls or an appropriately isolated network. The Compose worker requires
an explicit private-interface bind address; its HTTP inference endpoint binds
to loopback by default.

The `tgi`, `localai`, `mlx`, `sglang`, and generic `openai` adapters connect to their
already-running `/v1/models` and `/v1/chat/completions` endpoints. They work
with the hardware and model formats supported by those runtimes; mAIstr0 routes
to a served model but does not turn independent full-model copies into one
distributed model. For multi-host model splitting, use a backend such as
llama.cpp RPC or another server's supported distributed mode.

## API

The orchestrator API remains available while the mAIstr0 process is running,
including when its console is hidden. Local third-party programs can use
`http://127.0.0.1:7450` by default. Remote clients must use HTTPS and a paired
cluster identity (mutual TLS); plain HTTP is restricted to loopback.

Submit work through `POST /api/tasks` with a JSON body such as
`{"description":"Summarize this report"}`. The recommended path is
`"plan_mode":"auto"`, which asks a local coordinator model for a structured
plan with agent roles and dependencies. Successful plans are cached in a
bounded in-memory cache keyed by normalized task description, so repeated
requests can reuse the plan without another planner call. Invalid plans are
rejected rather than silently run. If the selected coordinator model fails,
the planner retries with another advertised node/model; if no candidate
succeeds, the failure is returned. Omitting `plan_mode` uses the improved
local heuristic; `"plan_mode":"heuristic_v1"` explicitly selects the legacy
heuristic. Work is rejected with a service unavailable response if any planned
subtask has no compatible model, rather than accepting only part of the
request. Query task status and results through
`GET /api/tasks` or `GET /api/tasks/{id}`. Cancel a running task with
`POST /api/tasks/{id}/cancel`. Retrieve a task's workflow nodes and dependency
edges for visualization through `GET /api/tasks/{id}/dependencies`.

For precise multi-step workflows, submit an explicit plan. Dependencies name
other subtask IDs; dependent work waits for its prerequisites and receives
their outputs as context:

```json
{
  "description": "Research and summarize a topic",
  "subtasks": [
    {
      "id": "research",
      "description": "Research the topic",
      "role": "researcher",
      "task_type": "general",
      "expected_output": "Evidence and key findings"
    },
    {
      "id": "summary",
      "description": "Write a concise summary",
      "role": "summarizer",
      "depends_on": ["research"],
      "expected_output": "A concise, evidence-grounded summary"
    }
  ]
}
```

Plans are checked for missing dependencies and cycles. Heuristic decomposition
recognizes enumerations, “compare A and B”, “for each” lists, and clear
sequential cues such as “then” and “after that”. Fan-out plans include a final
synthesis subtask that depends on their leaf tasks. Other independent subtasks
remain parallel. Set `"routing_preference":"latency"` to favor faster candidate
estimates for all subtasks; the default `"quality"` preference retains model
quality bonuses. Completion estimates use model size, queued work, and measured
historical throughput when available; throughput itself is an estimate based
on stored output length, not tokenizer-specific telemetry.

Speculative hedging is disabled by default. When enabled with
`"speculative_hedging_enabled":true` in orchestrator configuration, a second
eligible node may receive the same subtask after the model/task-type historical
p95 delay is reached. The first successful result is persisted and the losing
request is cancelled. Only one extra concurrent hedge is allowed by default;
`"max_concurrent_hedges"` may be raised up to four, subject to dispatch
concurrency. P95 hedging waits for at least five successful history samples.

Supported roles include researcher, analyst, coder, writer, reviewer,
summarizer, translator, and general. Subtasks may specify
`expected_output`, `output_format:"json"`, `required_fields`, and
`max_retries` (0–2); JSON contracts are checked before dependent work starts.
Set `"require_review":true` to add a reviewer task that must return
`{"approved":true,...}` before synthesis, or `"require_approval":true` to
pause completed work for a human. Approve through
`POST /api/tasks/{id}/approve`; the approval state and outputs survive
restarts. Multi-task results are synthesized by a cluster model only after
all required work succeeds. Work that was still running when the orchestrator
stopped is restored as failed with an interruption reason rather than being
reported as running.

`POST /v1/chat/completions` accepts OpenAI-style messages and supports streamed
responses using `"stream": true`. The orchestrator preserves system, user,
assistant, and tool message roles in its model prompt.

For provider-compatible inference without task planning or payload translation,
use the separate transparent router paths:
`POST /router/v1/chat/completions`, `/router/v1/completions`,
`/router/v1/messages`, `/router/api/chat`, and `/router/api/generate`.
The router selects a healthy node/model whose ready engine advertises the
request protocol, preserves provider fields and headers, and supplies a model
when the request omits one. It retries only bounded transient failures, and
streaming failover is allowed only before the first response byte is sent.
Responses include an `X-Request-ID`; recent prompt-free request/attempt events
and per-node/model circuit-breaker states are available through
`GET /api/router/requests` and SSE cluster snapshots. A breaker opens after
three transient failures within 30 seconds, admits one half-open probe after an
exponentially backed-off delay with jitter, and resets after a healthy response.
The default first-byte timeout is 60 seconds and the idle-stream timeout is 30
seconds; both are bounded by the existing 120-second total router deadline.
Configure `router_time_to_first_byte_timeout_seconds` and
`router_idle_stream_timeout_seconds` in the orchestrator JSON to override those
defaults.

Per-client limits are opt-in: set `router_requests_per_minute` and/or
`router_max_concurrent_per_client` in the orchestrator JSON; zero leaves each
limit disabled. Client identity uses a hash of `X-API-Key` or `Authorization`
when present, otherwise the direct remote IP address. Forwarded-address headers
are not trusted. Credentials and prompt content are not included in router
events, circuit-breaker state, or metrics.
Production HTTP listeners cap request bodies at 256 MiB in addition to any
smaller route-specific limits. Their request logs contain only the request ID,
method, status, and duration; they omit URLs, request bodies, and model content.
The Anthropic-compatible route is accepted only when a ready node engine
advertises Anthropic protocol support; the current built-in engines advertise
OpenAI and/or Ollama protocols.

Task dispatch also retries transient failures across up to five distinct
node/model pairs within a bounded deadline. Each execution attempt, including
its selected node/model/engine, status, and failure class, is persisted with
the task and returned by the task APIs.

The Ollama-compatible surface includes `GET /api/tags`, `POST /api/chat`, and
`POST /api/generate` (use the Ollama request fields such as `stream` and
`options`). Anthropic Messages clients can use `POST /v1/messages` with
`max_tokens`; text messages and text content blocks are supported. Streaming
is supported by both compatibility APIs. Existing mAIstr0 generation requests
and the OpenAI-compatible routes remain available.
The bundled model catalog is available at `GET /api/models/catalog`, with
optional `engine` and `q` filters. Model pulls can specify both `model` and
`engine` in `POST /api/nodes/{id}/models/pull`.

Automatic decomposition remains heuristic; callers that need reliable
dependencies should provide an explicit plan. GPU utilization and free-memory
telemetry influence scheduling when supported by NVIDIA tooling. CPU and
memory utilization samples are retained for display when reported by the
node's operating system.

## Development

See [CONTRIBUTING.md](./CONTRIBUTING.md) for branch, review, and contract-change
guidance, and [AGENTS.md](./AGENTS.md) for repository architecture and coding
conventions. Current cluster sync flows and their idle traffic baseline are
documented in [docs/sync-traffic.md](./docs/sync-traffic.md).

Run the test suite with:

```sh
go test ./...
go vet ./...
node --test web/app_test.js
```

The Go suite includes a process-level integration test for pairing, authenticated
cluster requests, streaming inference, and session restoration after restart.
Regenerate characterization contract fixtures with
`go test ./internal/orchestrator ./internal/scheduler ./internal/taskmgr ./internal/config -args -update`;
review the resulting golden-file changes before committing.
Contract fixtures use LF line endings enforced by `.gitattributes` so
byte-for-byte comparisons remain consistent on Windows and Linux.
The GitHub Actions workflow runs these checks and builds the application on
Windows and Linux.

### Race tests

Run the race-enabled Go suite with:

```sh
go test -race ./...
```

GitHub Actions runs this command on Ubuntu. The full suite passed in Ubuntu
24.04 WSL with Go 1.27.2. The node-agent capacity test uses an injected
no-op engine detector so its start signal does not depend on host-local engine
discovery. On the native Windows host, ThreadSanitizer cannot allocate its
shadow memory (error code 87); use Linux or CI for race validation.

On Windows, cross-compile the release binaries for Windows and Linux with:

```powershell
.\build\build.ps1
```

On Linux or macOS, use `./build/build.sh`. Both scripts write versioned
binaries to `dist/windows-amd64/`, `dist/linux-amd64/`, and
`dist/linux-ubuntu-amd64/`, and record the release number in `dist/version.txt`.
The release number comes from `internal/version/version.go` and is shown by
running `maistr0 --version`. Tests use isolated temporary data stores; normal
runs store persistent memory in the per-user data directory.
On Windows, verify the release build with
`.\dist\windows-amd64\maistr0.exe --version`; `dist/version.txt` records the
same version for all cross-compiled artifacts.
