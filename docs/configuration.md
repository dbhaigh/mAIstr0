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
    { "name": "llamacpp", "url": "http://127.0.0.1:8080" }
  ]
}
```

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
implement `GET /v1/models` and `POST /v1/chat/completions`.

LM Studio is discovered at `http://127.0.0.1:1234`; llama.cpp is discovered at
`http://127.0.0.1:8080` or PAIR's managed-server port
`http://127.0.0.1:8081`. Other OpenAI-compatible engines are configured
explicitly because their default ports may overlap and do not uniquely identify
an engine. Use the Setup view to rescan engines and inspect inventory freshness.

Configured engine URLs may point to local or operator-managed services; no
cloud inference endpoint is required. The selected server remains responsible
for model files, hardware acceleration, and model-format support. Inventory
checks list models but do not guarantee that a model can load or generate.

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
