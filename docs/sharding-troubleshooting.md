# Sharding troubleshooting

## Gather evidence first

1. On the local orchestrator, inspect `GET /api/shards` and
   `GET /api/shards/{id}`. Save the mode, state, model dimensions/context,
   placement rationale, and error. A failed create can leave a failed record.
2. Inspect each node's managed status from the paired current orchestrator:
   `GET /api/engines/managed`. Record state, PID, restart count, last error,
   endpoint, relay endpoint, and warnings. These routes are not anonymous LAN
   APIs. The local operator's dashboard/diagnostics can help collect evidence.
3. Compare hardware devices, **free device memory**, engine capabilities,
   model metadata, and telemetry freshness. Detection alone is not execution
   support. Check exact driver and engine revisions on every host.
4. Use `--diagnostics` and local engine logs. Do not attach prompts, generated
   text, PINs, API keys, private keys, or full unredacted configurations to bug
   reports. Record real-machine results in
   [hardware-validation.md](./hardware-validation.md).

## Creation is refused

- **No compatible nodes / insufficient memory**: check model size,
  quantization, layer dimensions, requested context, and KV-cache head count.
  Free memory must cover weights, KV cache, and safety margins. Host total
  RAM is not accelerator memory; an unknown free-memory value is not a fit.
  The group API currently leaves CPU offload disabled.
- **llama.cpp relay unavailable / untrusted network**: configure a managed RPC
  worker on each selected node and a head on a selected node. Confirm paired
  node certificate pins and reachable advertised relay addresses/ports
  (default `7452`). Keep the raw RPC backend loopback-only. Never fix this by
  opening its unauthenticated port to the LAN.
- **vLLM interface rejected**: assign a private, non-loopback IP on an active
  interface named in `trusted_network_interfaces`; set `options.vllm.host_ip`
  if selection is ambiguous. Allow distributed traffic only over a trusted
  VLAN or WireGuard. Control-plane mTLS does not protect dynamic vLLM ports.
- **Homogeneity / unsupported OS**: match GPU signature, device count, runtime
  builds, and model availability. Managed vLLM requires Linux; Windows needs
  a separate Linux server/environment, not a renamed native executable.
- **NPU rejected**: use the correct vendor/backend profile, supported
  quantization, known model memory requirement, and context cap. Leave
  `validated_rpc` false until separate-host validation passes for those exact
  builds. Whole-model NPU support is not multi-node RPC support.

## Worker or head will not become ready

- Verify executable/model paths exist inside their configured allow-list
  directories, including resolved symlink targets. The engine command is typed
  argv, not a shell command. Do not insert shell fragments in paths/options.
- Match llama.cpp head/worker RPC and device build options; ensure model
  files/revisions and tokenizer agree. Check port conflicts and permissions.
- The head's HTTP health endpoint must respond successfully. A vLLM headless
  worker has no chat server; process liveness is not proof that collective
  initialization or GPU execution succeeded. Inspect upstream runtime logs.
- NPU startup may require specific drivers/firmware and supported operation
  shapes; CPU fallback can hide lack of acceleration. Measure it rather than
  interpreting a ready HTTP endpoint as an NPU validation.

## Group becomes degraded, then failed

Health sweeps run every five seconds. A lost managed process or unavailable
member first marks the group `degraded`; the next sweep stops the head and
remaining workers and marks it `failed`. All inference is refused while not
ready. Node heartbeats alone do not prove managed processes are healthy.
Automatic process restart/backoff can race with health observation; do not
assume the five-second sweep is a guaranteed outage-detection deadline.

Fix the failure, verify fresh node telemetry, and create a new group with the
same model name. The router selects the replacement over failed history.
Delete obsolete records only after recording diagnostics. If teardown fails,
inspect the reported error and remaining PIDs before retrying deletion; do
not kill processes by executable name.

## Router returns 503 or no output

Use `POST /router/v1/chat/completions` with the exact group model name. Only a
`ready` group accepts inference; requests within a group share one serialized
request slot. Check router request events and request IDs, head managed status,
firewalls, engine API compatibility, and upstream timeouts. Streaming must end
with the engine's expected terminal event; a partial stream is not success.
Fix/rebuild the group rather than routing directly to a headless worker.

The fake process test (`go test ./internal/integration -run
TestManagedShardProcessLifecycle -count=1 -v`) checks lifecycle, routing, loss,
and rebuild without GPUs. It cannot diagnose real tensor/backend failures.
