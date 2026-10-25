# Real-machine hardware validation

This is an evidence checklist, not a support claim. **Every result field starts
untested.** Fake-engine CI does not qualify a device, backend, model, or OS.
Do not infer a pass from device detection, process startup, or HTTP readiness.
Keep failed attempts, including CPU fallback and unsupported OS/build results.

For each combination below, replace `untested` only after a named tester runs
the exact configuration. Add separate records for OS, driver, engine, model,
quantization, or topology changes. Never copy throughput from another machine.
No real-machine results have been added as part of this checklist.

## Procedure for each record

- [ ] Record date/tester, host CPU, accelerator model/count, RAM/VRAM or shared
      memory, OS/version/architecture, driver/firmware, and mAIstr0 revision.
- [ ] Record engine repository/revision, compiler, **all build flags**, runtime
      options/environment (redact secrets), model ID/revision/hash, format,
      quantization, context size, prompt token count, and output token count.
- [ ] Run a whole-model smoke test and confirm the intended device actually
      executes work; record fallback, memory use, startup failures, and logs.
- [ ] Measure prompt processing and decode **tokens per second separately**,
      time to first token, repetitions, and cold/warm conditions. Record
      failures/timeouts as failures, not zero-throughput passes.
- [ ] If testing sharding, record node count, device inventory, network
      interface/speed, engine revision on every host, plan and split/topology.
      Verify the protected relay/trusted-network setup before inference.
- [ ] Create a group, serve non-streaming and streaming chat through the router,
      kill a specific worker PID, observe degraded then failed, verify teardown,
      rebuild, and serve again. Verify model output, not just HTTP status.
- [ ] Test incompatible quantization/context and insufficient memory refusals.
      For NPUs, test supported operation shapes and distinguish CPU fallback.
- [ ] Attach sanitized evidence and record limitations. Only then consider
      operator `validated_rpc` opt-in for the exact NPU RPC combination.

## NVIDIA GPU

- [ ] Validation completed: untested
- Machine / OS / driver / tester / date: **untested**
- Engine revision / CUDA or other backend / build flags: **untested**
- Model / revision / quantization / context / runtime flags: **untested**
- Whole-model output / device verification: **untested**
- Prompt tokens/s / decode tokens/s / first-token latency / repetitions: **untested**
- Shard topology / network / lifecycle / rebuild evidence: **untested**
- Failures / fallback / evidence links: **untested**

## AMD GPU

- [ ] Validation completed: untested
- Machine / OS / driver / tester / date: **untested**
- Engine revision / ROCm-HIP or Vulkan backend / build flags: **untested**
- Model / revision / quantization / context / runtime flags: **untested**
- Whole-model output / device verification: **untested**
- Prompt tokens/s / decode tokens/s / first-token latency / repetitions: **untested**
- Shard topology / network / lifecycle / rebuild evidence: **untested**
- Failures / fallback / evidence links: **untested**

## Intel Arc or integrated GPU

- [ ] Validation completed: untested
- Machine / discrete or shared memory / OS / driver / tester / date: **untested**
- Engine revision / SYCL or Vulkan backend / build flags: **untested**
- Model / revision / quantization / context / runtime flags: **untested**
- Whole-model output / device verification: **untested**
- Prompt tokens/s / decode tokens/s / first-token latency / repetitions: **untested**
- Shard topology / network / lifecycle / rebuild evidence: **untested**
- Failures / fallback / evidence links: **untested**

## Apple silicon

- [ ] Validation completed: untested
- SoC / unified memory / macOS version / tester / date: **untested**
- Engine revision / Metal or MLX backend / build flags: **untested**
- Model / revision / quantization / context / runtime flags: **untested**
- Whole-model output / device verification: **untested**
- Prompt tokens/s / decode tokens/s / first-token latency / repetitions: **untested**
- llama.cpp RPC topology / network / lifecycle / rebuild evidence: **untested**
- Failures / unsupported modes / fallback / evidence links: **untested**

## CPU-only

- [ ] Validation completed: untested
- CPU / instruction set / RAM / OS / tester / date: **untested**
- Engine revision / CPU backend / build flags / thread count: **untested**
- Model / revision / quantization / context / runtime flags: **untested**
- Whole-model output / device verification: **untested**
- Prompt tokens/s / decode tokens/s / first-token latency / repetitions: **untested**
- Managed CPU shard-placement refusal / standalone RPC experiments: **untested**
- Failures / unsupported modes / evidence links: **untested**

## Intel NPU

- [ ] Validation completed: untested
- NPU / memory accounting / OS / driver / firmware / tester / date: **untested**
- Engine revision / OpenVINO backend / build flags: **untested**
- Model / revision / quantization / context cap / runtime flags: **untested**
- Whole-model output / operation shapes / actual NPU use: **untested**
- Prompt tokens/s / decode tokens/s / first-token latency / repetitions: **untested**
- Separate-host RPC / topology / lifecycle / rebuild evidence: **untested**
- Failures / CPU fallback / evidence links: **untested**

## AMD NPU

- [ ] Validation completed: untested
- XDNA generation / OS / driver / XRT or runtime / tester / date: **untested**
- Engine revision / XDNA or FastFlowLM backend / build flags: **untested**
- Model / revision / quantization / context cap / runtime flags: **untested**
- Whole-model output / operation shapes / actual NPU use: **untested**
- Prompt tokens/s / decode tokens/s / first-token latency / repetitions: **untested**
- Separate-host RPC / topology / lifecycle / rebuild evidence: **untested**
- Failures / CPU fallback comparison / evidence links: **untested**

## Snapdragon

- [ ] Validation completed: untested
- SoC / HTP generation / OS / SDK / driver / tester / date: **untested**
- Engine revision / Hexagon backend / build flags: **untested**
- Model / revision / quantization / context / runtime flags: **untested**
- Whole-model output / operation shapes / actual NPU use: **untested**
- Prompt tokens/s / decode tokens/s / first-token latency / repetitions: **untested**
- Separate-host RPC / topology / lifecycle / rebuild evidence: **untested**
- Failures / unsupported OS / CPU fallback / evidence links: **untested**
