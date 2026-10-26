# Configuration

mAIstr0 accepts command-line options and JSON overlays for node and
orchestrator settings. Configuration is normalized and validated before
listeners start. Use `--print-config` with the same role and config options to
inspect normalized JSON without starting services.

## Command-line options

The main options include:

- `--role auto|node|orchestrator|both`
- `--node-config <path>` and `--orchestrator-config <path>`
- `--node-listen-addr <address>` and `--orchestrator-listen-addr <address>`
- `--orchestrator-addr <https-url>` for a node
- `--pairing-pin <six-digit-pin>` for unattended node enrollment
- `--orchestrator-fingerprint <64-hex-SHA256>` for verified first-contact
  enrollment; also `orchestrator_fingerprint` in node JSON. Stored pins take
  precedence when reconnecting to a known selected identity, preserving failover.
  For certificate rotation, supply the verified new fingerprint explicitly in
  the local re-pairing UI/API.
- `--no-discovery` for node-only configurations
- `--no-browser` to suppress automatic dashboard launch
- `--harness pi|deepseek` to select the default interactive harness
- `--print-config` to print normalized configuration and exit
- `--diagnostics` to print a prompt-free local health summary
- `--allow-peer-updates` to opt in to executable updates from paired peers
- `--no-peer-updates` to prohibit those updates, overriding the allow flag

Run `maistr0 --help` for the complete option list. Orchestrator election
requires LAN discovery; disabling discovery is supported only for
node-oriented startup.

## Automatic peer executable updates

Peer executable updates are **disabled by default**. Opt in with
`--allow-peer-updates` only if every paired publisher is trusted to execute
code as the application account. `--no-peer-updates` overrides that opt-in.
Existing launch scripts must add the allow flag if this authority is intended;
ordinary pairing, configuration, and trust-store formats remain compatible.

Version 0.0.24.1 introduced updates from **paired peers**, not GitHub.
Install this version manually once on existing nodes: earlier executables do
not contain the updater and cannot bootstrap themselves. When opted in, a node
checks healthy registered peers and LAN-discovered paired identities every
30 seconds, using pinned mutual TLS and the peer's executable manifest.
Only strictly newer numeric `major.minor.patch` or
`major.minor.patch.iteration` releases are accepted;
rebuilding the same version does not trigger an update. Use a new version
number for each rollout.

Rapid iterations use `0.0.24.1`, `0.0.24.2`, and so on; the fourth component
has no single-digit limit (`0.0.24.10` follows `0.0.24.9`). A three-component
release behaves as iteration zero. `0.0.25` sorts after all `0.0.24.x`
versions. Any node running the earlier local 0.0.25 development build must be
manually replaced with 0.0.24.1; automatic downgrade remains prohibited.

A compatible image is downloaded beside the installed executable, with
bounded size, SHA-256, Go platform metadata, and reported-version checks.
The executable directory must be writable by the application account.
Downloads can be staged while busy, but installation waits until the entire
process is idle: no active/pending tasks (including synthesis/approval), no
in-flight work requests, no loaded pipeline stages, no managed worker/head
processes, and no active shard groups or membership operations on a local
coordinator. A combined-role process and a promoted coordinator use the same
admission fence. **Stop running shard groups explicitly to allow updates.**
The updater does not force-stop them, even if inference is currently idle.
Failed managed engines must also be explicitly stopped, so a pending crash
restart cannot race executable replacement.
New work is briefly rejected with 503 once replacement commits.

The helper waits for graceful parent exit, replaces the image, and restarts
with the original arguments and working directory. Configuration, identity,
model caches and databases are unchanged. The prior executable is retained
as `<executable>.previous`; replacement/launch errors are written to
`<executable>.peer-update.log`. Immediate launch failures attempt rollback;
this is not a post-start health supervisor. For a later startup failure,
close the process, restore the `.previous` executable, then restart manually
with `--no-peer-updates` until the faulty release is withdrawn.

**Opt-in security risk / deferred security work:** enabling peer updates grants
paired publishers the ability to supply executable code that runs as the local
application account. A compromised paired node can supply a malicious binary and matching
checksum/version. Mutual TLS authenticates the publisher and SHA-256 detects
transfer corruption; neither is an independent release signature. Signed
releases, publisher authorization and revocation policy remain future work.
Use `--no-peer-updates` on every receiving process to disable this behavior.
Never pair an untrusted machine. Existing pairing and current-coordinator
write checks are not bypassed by the updater.

### Cross-platform bundles

A node always serves its own native running image. Windows cannot manufacture
a Linux executable (or vice versa) from its running image; publish both
builds in sibling `windows-amd64` and `linux-amd64` directories with generated
`.peer.json` metadata. A native seed on each platform also works.
Ubuntu uses the Linux amd64 image.

For locally built bundles, build every platform with the **same explicit**
`-X github.com/maistr0/maistr0/internal/version.Value=0.0.24.1` linker setting,
then run:

```powershell
go run .\cmd\updatebundle -root .\dist -version 0.0.24.1
```

The tool requires the existing `windows-amd64`, `linux-amd64`, and
`linux-ubuntu-amd64` build outputs; it
does not compile binaries or download anything. Native images are probed with
`-version`; the publisher asserts the foreign image's version with `-version`
because Go trimpath builds do not retain linker settings. Receiving nodes
verify that image's reported version before installation. When a running Windows image
is locked, build `dist\windows-amd64\maistr0-update.exe` and pass
`-windows-binary maistr0-update.exe`. Preserve the sibling platform folders
and metadata when copying a multi-platform publishing bundle; a standalone
executable can only publish its native platform. Ordinary GitHub release
archives are single-platform native seeds.

## Node configuration

The node listener defaults to port `7451`; a configured orchestrator commonly
uses port `7450`. A node's `orchestrator_addr` must be an HTTPS URL. Pairing
PINs are optional for manual setup and should be supplied out of band.

```json
{
  "listen_addr": ":7451",
  "orchestrator_addr": "https://192.168.1.10:7450",
  "max_concurrent_tasks": 2,
  "sync_max_bytes": 67108864,
  "sync_timeout_seconds": 600,
  "log_level": "info",
  "engines": [
    { "name": "ollama", "url": "http://127.0.0.1:11434" },
    { "name": "lmstudio", "url": "http://127.0.0.1:1234" },
    { "name": "llamacpp", "url": "http://127.0.0.1:8080", "api_key": "optional-token" }
  ]
}
```

`api_key` is optional and is sent as a bearer token by the `vllm`, `llamacpp`,
`lmstudio`, and `fastflowlm` adapters. It is never logged or returned by node
or orchestrator APIs, and `--print-config` redacts it.

For `llamacpp-rpc` shard groups, the node's `rpc_relay_listen_addr` defaults
to `:7452`. Permit this port only between cluster nodes in the host firewall.
The worker's `ggml-rpc-server` remains bound to loopback; remote traffic is
forwarded through the pinned mutual-TLS relay, and the head's llama-server is
given loopback relay endpoints.

vLLM distributed traffic is unencrypted and can use dynamic ports. By default,
each node must have an active private IP address on an interface named in
`trusted_network_interfaces`; the optional `host_ip` engine setting selects
which assigned address is exported as `VLLM_HOST_IP`. Use a WireGuard network
or isolated VLAN for distributed vLLM traffic. Setting
`allow_insecure_shard_network` explicitly permits an untrusted interface and
logs a prominent warning; it does not add encryption or authentication.

The node also supports `supported_shard_models` and
`max_layers_it_can_host` as eligibility limits for configured stage-capable
engines. They do not make ordinary OpenAI-compatible engines support
partial-layer inference. `sync_max_bytes` and `sync_timeout_seconds` tune
bounded memory and task-history transfers; see
[Memory synchronization](./memory-sync.md).

## Orchestrator configuration

```json
{
  "listen_addr": ":7450",
  "mode": "full",
  "harness": "pi",
  "log_level": "info",
  "max_concurrent_dispatches": 16,
  "dispatch_timeout_seconds": 600,
  "sync_max_bytes": 67108864,
  "sync_timeout_seconds": 600
}
```

`mode` accepts `full` or `minimal`. Full mode enables interactive harnesses
and persistent learning; minimal mode omits those endpoints and storage while
retaining cluster task history and inference routing. Harness names are `pi`
and `deepseek`. A process running both node and orchestrator roles uses the
orchestrator's `log_level`.

### Capacity, timeouts, and synchronization

- `max_concurrent_dispatches` defaults to `16` and limits cluster-wide
  subtask execution.
- `dispatch_timeout_seconds` defaults to `600` seconds and bounds a subtask's
  dispatch and retry attempts. The inference HTTP client also uses this setting
  for planning and final synthesis rather than a separate fixed ten-minute
  limit. Managed-shard generation includes waiting for the group's request slot
  in this budget. Earlier caller deadlines and cancellation still win.
- `max_concurrent_tasks` defaults to `2` on a node; both the orchestrator and
  node enforce node capacity.
- `sync_max_bytes` defaults to 64 MiB and may be configured from 1 byte through
  256 MiB.
- `sync_timeout_seconds` defaults to 600 seconds and may be configured from 1
  through 3600 seconds.
- `log_level` accepts `debug`, `info`, `warn`, or `error` and defaults to
  `info`.
  Orchestrator debug logs include privacy-safe scheduler candidate rejection,
  score, telemetry, and capacity-wait explanations; see
  [scheduler diagnostics](./scheduler-evaluation.md#structured-diagnostics).

For a slow distributed model, use a thirty-minute budget without changing the
global default:

```json
{
  "dispatch_timeout_seconds": 1800
}
```

Save this as an orchestrator JSON configuration and launch with
`-orchestrator-config <path>`, retaining your usual role/node options. This is
not an automatically discovered configuration file; it must be supplied on the
command line. A node-served dashboard no longer imposes an independent
ten-minute timeout on task submission while the orchestrator is planning.
Closing/canceling the incoming request still cancels planning.

Shard task generation requests OpenAI SSE internally. The node proxy flushes
headers and small chunks immediately; the orchestrator assembles assistant
content and logs safe progress counts (`chunks`, `output_bytes`,
`reasoning_bytes`, and `elapsed_ms`) without logging prompts or generated text.
The dashboard receives the completed plan/output, not these internal token
chunks. A total timeout still applies even if the head is producing tokens.
Increasing it gives a working but slow head more time; it does not improve
throughput or repair a stalled model.

### Router controls

Router first-byte and idle-stream timeouts default to 60 and 30 seconds,
respectively, bounded by the 120-second total deadline. Override them with
`router_time_to_first_byte_timeout_seconds` and
`router_idle_stream_timeout_seconds`.

Per-client limits are disabled by default. Set
`router_requests_per_minute` and/or `router_max_concurrent_per_client` to
enable them. Identity is derived from a hash of `X-API-Key` or `Authorization`,
or the direct remote address; forwarded-address headers are not trusted.

Speculative hedging is also disabled by default. Set
`speculative_hedging_enabled` to opt in and use `max_concurrent_hedges` to
bound the extra load (maximum four). See the [API reference](./api.md) for the
request-level `routing_preference`.

## Model engines

Nodes discover local Ollama, LM Studio, llama.cpp, and FastFlowLM services by
default. Other supported configured engines include vLLM, TGI, LocalAI, MLX-LM,
SGLang, and generic OpenAI-compatible servers. OpenAI-compatible engines
include `vllm`, `llamacpp`, `lmstudio`, and `fastflowlm`; they implement
`GET /v1/models` and `POST /v1/chat/completions`.

LM Studio is discovered at `http://127.0.0.1:1234`; llama.cpp is discovered at
`http://127.0.0.1:8080` or PAIR's managed-server port
`http://127.0.0.1:8081`. Other OpenAI-compatible engines are configured
explicitly because their default ports may overlap and do not uniquely identify
an engine. Use the Setup view to rescan engines and inspect inventory freshness.

Configured engine URLs may point to local or operator-managed services; no
cloud inference endpoint is required. The selected server remains responsible
for model files, hardware acceleration, and model-format support. Inventory
checks list models but do not guarantee that a model can load or generate.

### NPU engine profiles

The typed profiles `llamacpp-openvino-npu`, `llamacpp-hexagon-npu`,
`llamacpp-xdna2-npu`, and `fastflowlm-xdna2` are OpenAI-compatible configured
engine names. Configure
only the profile that matches the service and hardware on that node:

```json
{
  "engines": [
    { "name": "llamacpp-openvino-npu", "url": "http://127.0.0.1:8080" }
  ]
}
```

Use `llamacpp-hexagon-npu` for a Hexagon-backed llama-server, or
`fastflowlm-xdna2` for the FastFlowLM service (default port `52625`).

For managed llama-server processes, set `managed_engines[].template` to
`llamacpp-openvino-npu` or `llamacpp-hexagon-npu`. The OpenVINO profile fixes
`GGML_OPENVINO_DEVICE=NPU`, passes `--parallel 1`, and requires an explicit
`options.llama_server.context_size`; its reported capabilities are Intel NPU,
text-only, Q4_0 primary quantization, and one parallel sequence. The Hexagon
profile selects `HTP0` and reports only its documented Snapdragon/Hexagon NPU
backend and Q4_0 example. These fixed settings use typed argv/environment
values, not shell fragments. They require llama.cpp binaries built with the
corresponding backend. See the upstream
[OpenVINO backend](https://github.com/ggml-org/llama.cpp/blob/master/docs/backend/OPENVINO.md)
and [Snapdragon backend](https://github.com/ggml-org/llama.cpp/blob/master/docs/backend/snapdragon/README.md)
documentation.

#### AMD XDNA 2 llama-cpp

`llamacpp-xdna2-npu` is a separate, opt-in profile for the enhanced
llama.cpp build at source revision `52aa47c00f`. It requires an XRT SDK at
build time and a compatible XRT runtime, NPU driver, and firmware on the
machine that runs mAIstr0. No llama.cpp executable, XRT library, model, or
kernel asset is downloaded or bundled by mAIstr0. This source pin is in the
local enhanced llama.cpp checkout; make the commit available to any separate
build host before following the commands below.

Build the optional `llama-server` locally from that pinned source revision.
Enable RPC as well only if this host will participate in a distributed RPC
deployment:

```powershell
$env:XILINX_XRT = "C:\path\to\xrt\sdk"
git -C C:\src\llama.cpp checkout 52aa47c00f
cmake -S C:\src\llama.cpp -B C:\build\llama-xdna -DGGML_XDNA=ON -DGGML_RPC=ON -DLLAMA_BUILD_SERVER=ON
cmake --build C:\build\llama-xdna --config Release --target llama-server ggml-rpc-server
```

```sh
export XILINX_XRT=/path/to/xrt/sdk
git -C /src/llama.cpp checkout 52aa47c00f
cmake -S /src/llama.cpp -B /build/llama-xdna -DGGML_XDNA=ON -DGGML_RPC=ON -DLLAMA_BUILD_SERVER=ON
cmake --build /build/llama-xdna --config Release --target llama-server ggml-rpc-server
```

For a local-only deployment, `GGML_RPC=ON` and the `ggml-rpc-server` target
are unnecessary. Keep the XRT shared libraries on the service process's
runtime library path (`PATH` on Windows), and keep the `xdna-kernels` runtime
assets produced by the build alongside the backend module. Before registering
the process with mAIstr0, confirm `llama-server --list-devices` reports
`XDNA0`.

Configure the engine name and managed process together. `model_path` may be
the Q4NX package directory (containing `model.q4nx`, `config.json`,
`tokenizer.json`, and `tokenizer_config.json`) or a GGUF model file:

```json
{
  "engines": [
    { "name": "llamacpp-xdna2-npu", "url": "http://127.0.0.1:8080" }
  ],
  "managed_engine_binary_allowlist": ["/opt/maistr0/llama-xdna/bin"],
  "managed_engine_model_allowlist": ["/models"],
  "managed_engines": [
    {
      "id": "llama-xdna",
      "kind": "llama-server",
      "template": "llamacpp-xdna2-npu",
      "binary_path": "/opt/maistr0/llama-xdna/bin/llama-server",
      "model_path": "/models/qwen2.5-3b-instruct-npu2",
      "options": {
        "llama_server": {
          "host": "127.0.0.1",
          "port": 8080,
          "context_size": 4096
        }
      }
    }
  ]
}
```

The profile fixes `--device XDNA0`, `--parallel 1`, and `-nkvo`, and requires
an explicit `context_size`. It does not claim a model-wide NPU execution
device: supported `Q4_0` matrix-vector operations can execute on XDNA while
unsupported operations, shapes, and batches fall back to CPU. Q4NX support is
for loading supported Qwen2 packages and unpacking their matrices to Q4_0; it
does not add native Q4NX kernels. In particular, the supplied Qwen2.5-3B
projection shapes do not match the current XDNA kernels. Model inventory
therefore remains routable through the ordinary engine path; individual models
are not tagged as NPU-only, so mAIstr0 does not assume that the whole model
runs there.

Treat this profile as experimental: the recorded end-to-end XDNA-requested
benchmark was slower than CPU-only, and XRT did not report NPU power. Keep
CPU fallback enabled and compare throughput, latency, and power on the
specific workload before using it as a preferred engine.

The FastFlowLM profile matches AMD XDNA 2 NPUs and reports its documented
maximum context length of 256k; it does not infer model size or quantization
when the server omits them. See the
[FastFlowLM documentation](https://github.com/ROCm/FastFlowLM). NPU models are
eligible only when their reported size fits the node's reported NPU memory and
any template quantization/context requirements are known to pass. Missing or
incompatible metadata is explained in the scheduler rejection.

NPU participation in a shard group is disabled by default. Set
`engines[].validated_rpc` to `true` only after the operator has tested that
engine's RPC behavior; a server's self-reported `validated_rpc` value does not
enable it. Whole-model NPU inference does not require this opt-in.

For XDNA workers, build compatible XDNA-enabled `llama-server` and
`ggml-rpc-server` binaries. Use mAIstr0's loopback-only RPC server and
authenticated relay; do not expose the unauthenticated llama.cpp RPC port
directly. Keep `validated_rpc` false until the operation-shape checks and a
separate-host inference test have passed for the exact client/server builds.
Local or loopback RPC success alone is not evidence of cross-host validation.

When a workflow subtask has no compatible model, automatic provisioning may
pull a fitting model from the bundled Ollama catalog. It considers healthy
Ollama nodes and available memory estimates; it does not pull custom model
names or use non-Ollama engines. Choose an online target and download-capable
engine for manual model pulls.

## Distributed llama.cpp RPC

llama.cpp RPC lets one `llama-server` use CPU/GPU devices on other hosts for a
single distributed inference endpoint. Build compatible `ggml-rpc-server` and
`llama-server` binaries with the RPC backend enabled, start the workers, and
configure only the main host's OpenAI-compatible endpoint as a `llamacpp`
engine. RPC worker hosts are compute workers, not separate mAIstr0 model
replicas.

The example Linux Docker profiles are in
[`packaging/llamacpp/compose.yaml`](../packaging/llamacpp/compose.yaml).
llama.cpp RPC is experimental and insecure: do not expose its worker port to
the public internet or untrusted networks. Restrict it to trusted hosts with
firewalls or an isolated network. Hardware backends and model-format support
are determined by the llama.cpp build, not by mAIstr0.

## Managed engine processes

### Automatic RPC workers

Selecting a worker in the shard-group wizard automatically installs a missing
`ggml-rpc-server` on Windows amd64 or Linux amd64. The official source is
[`ggml-org/llama.cpp`](https://github.com/ggml-org/llama.cpp/releases);
the optional XDNA source is
[`dbhaigh/llama-xdna.cpp`](https://github.com/dbhaigh/llama-xdna.cpp/releases).
No administrator privilege, package manager, or manual configuration edit is
required. The installer retains the archive's shared libraries alongside the
worker, verifies its GitHub SHA-256 digest and size, rejects unsafe archives,
and registers a loopback-only worker behind the existing authenticated relay.

The release source picker defaults to official Vulkan builds. It expects
`-bin-win-vulkan-x64.zip` or `-bin-ubuntu-vulkan-x64.tar.gz` assets; the XDNA
source expects `-bin-win-xdna-x64.zip` or `-bin-ubuntu-xdna-x64.tar.gz`.
Archives must include `ggml-rpc-server` and all runtime libraries. If the
custom source has no supported release, installation reports unavailable;
it never silently substitutes a CPU or unrelated build. XRT, device drivers,
Vulkan drivers, and other system prerequisites remain operator-installed.
Official numbered `b<build>` releases are eligible even though upstream marks
them as prereleases; unrelated prereleases and drafts are excluded. Official
build numbers are never automatically downgraded, and an archive whose digest
changes without a version change is rejected.

Installer-managed workers are stored under the user configuration directory
at `maistr0/workers/<node-key>`, or beside an explicit `memory_path` in
`<memory_path>.workers`. A durable manifest restores them after restart.
Selecting them again checks for updates; activation requires every managed
engine to be stopped, and active workers retain their installed version with
an explicit update-deferred message. Operator-configured workers are never
overwritten. Older installation directories are retained.

The wizard can also configure a matching `llama-server` from this runtime.
Choose the model-head node, then an existing local Ollama or LM Studio GGUF.
It reuses weights in place (no model downloads or copies), persists the selected
head and model alias, and restores them on restart and runtime updates. All
managed engines must be idle when changing the head. Operator-configured heads
are never replaced. An official Vulkan worker is not proof of XDNA support;
vLLM installation remains manual.

Discovery uses `OLLAMA_MODELS` or `~/.ollama/models`, and LM Studio's
`~/.lmstudio/models` and `~/.cache/lm-studio/models`. Set `LMSTUDIO_MODELS` for
a custom LM Studio model directory. Run the node as an account with permission
to read these models. Ollama manifests identify the actual GGUF weight blobs;
non-GGUF weights, split GGUF files, and inaccessible models produce explicit warnings.
Automatic head setup currently supports single-file GGUF models. A model
need not be loaded in Ollama or LM Studio. Stop competing model loads to free
accelerator memory before creating a group.

The node probes the installed `llama-server --list-devices` for measured total
and free GPU/NPU memory after installation and during telemetry refresh.
Invalid readings are rejected and failed probes clear their previous free-memory
readings. Host RAM is never substituted for unknown device memory. Devices need
free memory above the 2 GiB reserve plus their assigned weights and KV cache.

`POST /api/nodes/{id}/engines/managed/install-worker` accepts
`{"source":"official"}` or `{"source":"xdna"}`. It proxies to the node-local
`POST /api/engines/managed/install-worker`, which requires the current paired
orchestrator's pinned mTLS identity. Request bodies cannot supply executables,
arbitrary repositories, download URLs, installation paths, or shell commands.

### Operator-configured processes

Model-loading engines (`llama-server` and vLLM) have a ten-minute readiness
window for reading weights, transferring RPC shards, and warming up. RPC
workers retain a thirty-second readiness window. These limits apply to initial
starts and automatic/explicit restarts; caller cancellation still aborts startup
and cleans up the process. Managed start requests allow eleven minutes, and
node-served dashboard shard operations allow fifteen minutes so proxy deadlines
do not cut off model loading. A running process is not reported ready until its
readiness probe succeeds. If startup still times out, inspect the head node's
`managed_engine` entries in its mAIstr0 application log for loading progress,
device allocation errors, or RPC connectivity failures.

Nodes can manage explicitly configured `llama-server`, `ggml-rpc-server`, and
`vllm` processes. Managed engines are not started automatically; the paired
current orchestrator controls their lifecycle through the node's authenticated
mTLS API:

- `GET /api/engines/managed`
- `POST /api/engines/managed/{id}/start`
- `POST /api/engines/managed/{id}/stop`
- `POST /api/engines/managed/{id}/restart`

Configure absolute allow-list directories for executable binaries and local
model files/directories. Paths are resolved before validation, so a symlink
cannot escape an approved directory. Managed commands are built from typed
options as argv arrays; command strings and shell scripts are not accepted.
The current paired orchestrator can read a managed engine's model profile with
`GET /api/engines/managed/{id}/profile`. This parses an allow-listed GGUF file
or Hugging Face `config.json` and measures model weight files without returning
local paths. For a configured `model_id`, the model must already be in the
standard Hugging Face hub cache or under `HF_HUB_CACHE`/`HF_HOME`; this read
does not download model files. Profiles without measurable weights are
rejected so the shard planner does not guess at memory requirements. Supply a
positive `requested_context` query value; it is used to estimate KV-cache
memory for the selected group.

```json
{
  "managed_engine_binary_allowlist": ["/opt/maistr0/bin"],
  "managed_engine_model_allowlist": ["/models"],
  "managed_engines": [
    {
      "id": "llama-main",
      "kind": "llama-server",
      "binary_path": "/opt/maistr0/bin/llama-server",
      "model_path": "/models/model.gguf",
      "options": {
        "llama_server": {
          "host": "127.0.0.1",
          "port": 8080,
          "context_size": 4096,
          "gpu_layers": 32
        }
      }
    },
    {
      "id": "rpc-worker",
      "kind": "ggml-rpc-server",
      "binary_path": "/opt/maistr0/bin/ggml-rpc-server",
      "options": {
        "ggml_rpc_server": {"port": 50052}
      }
    },
    {
      "id": "vllm-worker",
      "kind": "vllm",
      "binary_path": "/opt/maistr0/bin/vllm",
      "model_id": "organization/model",
      "options": {"vllm": {"port": 8000}}
    }
  ]
}
```

`llama-server` and vLLM require exactly one of `model_path` or `model_id`;
local paths must be inside `managed_engine_model_allowlist`. The RPC server
defaults to `127.0.0.1`; non-loopback `bind_address` values are rejected even
if the legacy `allow_remote_bind` field is set. That field remains accepted
for config compatibility but cannot expose the RPC server.

Starting a vLLM worker requires a shard plan in the start/restart request:

```json
{
  "shard_plan": {
    "nnodes": 2,
    "node_rank": 1,
    "master_addr": "192.0.2.10"
  }
}
```

The supervisor starts nonzero ranks headlessly and supplies the distributed
flags from that plan; rank zero runs the HTTP inference service. Headless ranks
are considered ready while their processes remain alive, and the shard group is
ready only after rank zero passes its HTTP readiness probe. Native Windows vLLM
launches are rejected. Readiness is checked over HTTP for llama-server/rank-zero
vLLM and TCP for the RPC server; unexpected exits use bounded exponential
restart backoff. Shutdown terminates the process tree using Windows Job Objects
or Unix process groups.

For vLLM, configure the trusted interface names on each node, for example
`"trusted_network_interfaces": ["wg0"]`, and optionally set
`"host_ip": "10.8.0.12"` under that engine's `options.vllm`. The selected
address must be assigned to an active private interface; public or loopback
addresses are refused. Prefer WireGuard or an isolated VLAN because the
vLLM/NCCL distributed data plane is not protected by mAIstr0's control-plane
mutual TLS.

## Validation and compatibility

Listen addresses must use `host:port` or `:port` syntax. Node
`orchestrator_addr` and `advertise_addr` values must be empty or HTTPS URLs;
engine URLs must use HTTP or HTTPS. Pairing PINs, when configured, must contain
exactly six digits. Invalid settings are reported before startup, with exit
code `2`.

Configuration keys and defaults are compatibility contracts. See the
[node schema](../contracts/schemas/node-config.schema.json),
[orchestrator schema](../contracts/schemas/orchestrator-config.schema.json),
and [porting guide](../contracts/PORTING.md) before changing them.

For pipeline/sharded-model configuration and its current runtime limitations,
see [Distributed models](./api.md#distributed-models).
