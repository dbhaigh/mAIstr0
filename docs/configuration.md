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
- `--no-discovery` for node-only configurations
- `--no-browser` to suppress automatic dashboard launch
- `--harness pi|deepseek` to select the default interactive harness
- `--print-config` to print normalized configuration and exit
- `--diagnostics` to print a prompt-free local health summary

Run `maistr0 --help` for the complete option list. Orchestrator election
requires LAN discovery; disabling discovery is supported only for
node-oriented startup.

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
  dispatch and retry attempts.
- `max_concurrent_tasks` defaults to `2` on a node; both the orchestrator and
  node enforce node capacity.
- `sync_max_bytes` defaults to 64 MiB and may be configured from 1 byte through
  256 MiB.
- `sync_timeout_seconds` defaults to 600 seconds and may be configured from 1
  through 3600 seconds.
- `log_level` accepts `debug`, `info`, `warn`, or `error` and defaults to
  `info`.

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
