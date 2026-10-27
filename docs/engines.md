# Engine support and network guidance

## Support matrix

These are runtime/build requirements, **not hardware certification**. All
real-machine combinations in the new validation checklist start untested.
An HTTP adapter can run on a different OS from its engine server.

- **llama.cpp (`llamacpp`), Linux / Windows / macOS**:
  CPU whole-model inference depends on a compatible build and model. GPU
  execution needs a matching backend: NVIDIA CUDA on Linux/Windows, AMD
  ROCm/HIP or Vulkan where supported, Intel Arc/iGPU Vulkan or SYCL where
  supported, or Metal on Apple silicon/macOS. RPC needs compatible RPC-enabled
  head/worker builds in addition to those device backends. CPU-only managed
  group placement is not currently enabled.
- **vLLM (`vllm`), Linux**:
  managed `vllm-mp` requires homogeneous GPUs and an appropriate upstream
  build (for example NVIDIA CUDA or AMD ROCm). CPU-only and NPU shard groups
  are not supported by this mode. Other standalone upstream backends are not
  evidence of mAIstr0 distributed support. **vLLM is not native on Windows**;
  a Windows mAIstr0 client may use a Linux server or a separately configured
  Linux/WSL environment. macOS managed vLLM is not supported.
- **Intel NPU (`llamacpp-openvino-npu`)**:
  experimental OpenVINO-enabled llama.cpp on an upstream-supported Intel
  Linux/Windows environment; requires compatible drivers, model quantization,
  and an explicit context cap. It is not an Apple/AMD/Qualcomm NPU backend.
  CPU/GPU use should select an appropriate ordinary engine profile instead.
- **AMD NPU (`llamacpp-xdna2-npu`, `fastflowlm-xdna2`)**:
  experimental, build/driver-specific Ryzen AI XDNA 2 paths. The enhanced
  llama.cpp/XRT profile is not a generic GPU or Windows NPU guarantee;
  FastFlowLM's Windows deployment has its own runtime requirements. Supported
  shapes, models, CPU fallback, and context limits differ. No macOS support
  is claimed. Neither profile enables NPU RPC sharding by default.
- **Snapdragon NPU (`llamacpp-hexagon-npu`)**:
  experimental Hexagon/HTP-enabled llama.cpp on the upstream-supported
  Snapdragon OS/SDK combination. This is not a claim of native Windows or
  macOS NPU support; validate the exact Android/Linux/Windows target against
  its build documentation. CPU/GPU and NPU execution are separate backends.
- **MLX (`mlx`), Apple silicon / macOS**:
  whole-model CPU/unified-memory GPU runtime via a compatible server; no
  mAIstr0 managed multi-node mode or NPU support is claimed.
- **Ollama, LM Studio, FastFlowLM, TGI, LocalAI, SGLang, and OpenAI-compatible
  servers**: mAIstr0 provides API adapters, not bundled runtimes or a common
  CPU/GPU/NPU support guarantee. Linux/Windows/macOS and accelerator support
  follow each server's build. These adapters do not imply managed shard-group
  or protocol-v2 partial-layer support.


## Engine configuration

Valid configured engine names include `ollama`, `vllm`, `llamacpp`,
`lmstudio`, `fastflowlm`, `tgi`, `localai`, `mlx`, `sglang`, `openai`,
`llamacpp-openvino-npu`, `llamacpp-hexagon-npu`, `llamacpp-xdna2-npu`, and
`fastflowlm-xdna2`.
These llama.cpp and FastFlowLM profile names use the OpenAI-compatible API.
The `vllm`, `llamacpp`, `lmstudio`, and `fastflowlm` engines also use that API.
Add an optional `api_key` to an engine configuration to send bearer
authentication; it is redacted from config dumps and never returned by node
or orchestrator APIs. The typed NPU profiles only advertise documented
vendor/backend capabilities; see the
[configuration guide](./configuration.md#model-engines) for examples.

The `llamacpp-xdna2-npu` profile is an opt-in, experimental managed
llama-server build for AMD Ryzen AI XDNA 2 systems. It does not replace the
default llama.cpp engine; CPU fallback remains enabled for unsupported
operations. The current XDNA kernels cover only exact Q4_0 matrix-vector
shapes, and the measured XDNA-requested Qwen run was slower than CPU-only.
See the [XDNA deployment notes](./configuration.md#amd-xdna-2-llama-cpp).


## Distributed shard network security

The control plane authenticates paired nodes with mutual TLS and pinned
certificates. The data planes used by distributed inference have different
security properties and must not be exposed to untrusted networks:

- llama.cpp RPC is an unauthenticated, unencrypted proof-of-concept protocol.
  mAIstr0 keeps each `ggml-rpc-server` on loopback and forwards worker traffic
  through a separate mutual-TLS relay authenticated with the paired nodes'
  pinned certificates. The head engine receives only local loopback relay
  addresses; configure the relay listener (`rpc_relay_listen_addr`, default
  `:7452`) so selected workers can reach it.
- vLLM's distributed traffic is unencrypted and can use dynamic ports. By
  default, each node requires `VLLM_HOST_IP` to resolve to a private address on
  an operator-trusted interface (`trusted_network_interfaces`). Run distributed
  vLLM over WireGuard or an isolated VLAN. Setting
  `allow_insecure_shard_network` is an explicit opt-out: it permits an
  otherwise untrusted interface and emits a prominent warning, but does not
  encrypt or authenticate vLLM traffic.

Mutual TLS on the llama.cpp relay does not make the underlying RPC protocol
safe to expose directly; firewall the RPC backend so only its local relay can
reach it. See the [network configuration details](./configuration.md#managed-engine-processes).


## Threat model

The protected assets are model inputs/outputs and activations in transit,
cluster control authority, local model files, and the ability to launch
allow-listed engine processes. The remote attacker model includes an unpaired
LAN client, intercepted discovery traffic, and attempts to control processes
or send traffic directly to a shard backend.

- Pairing establishes pinned mutual-TLS identities, not trust in every machine
  on the LAN. Managed lifecycle calls additionally require the currently
  elected, paired orchestrator. Discovery/election does not authenticate peers
  or provide partition-safe consensus.
- llama.cpp RPC backends stay loopback-only behind the pinned mTLS relay.
  Do not publish their raw ports. vLLM distributed traffic is not protected by
  the HTTP control-plane TLS; use WireGuard or an isolated trusted VLAN and
  firewall its distributed ports. The insecure override is not a security fix.
- The local dashboard trusts the host user and loopback boundary. Do not expose
  it with an unauthenticated reverse proxy, shared port forward, or public
  tunnel. Keep PINs, certificates/private keys, and API keys out of logs.
- Binary/model allow-lists and typed argv constrain process selection; they
  do not sandbox an engine. Operators must trust the executable, model
  artifacts, driver stack, and every paired worker. A compromised paired host
  can observe its shard's data or return malicious results.
- Denial of service, malicious engine/model code, compromised administrator
  accounts, side channels, disk encryption, and cryptographic verification of
  model output are outside the current guarantees. Health checks and capacity
  limits improve availability but are not fault-tolerant inference.

See [pairing and security](./pairing-and-security.md) for the complete
local/remote trust boundaries and [SECURITY.md](../SECURITY.md) for reporting.
