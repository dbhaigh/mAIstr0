# Deterministic shard planner

`internal/shard` builds a proposed placement from a `ModelProfile`, candidate
node snapshots, and a `Policy`. `Plan` is deterministic and performs no
network, filesystem, or process I/O. It does not start engines or change the
orchestrator's current request path.

`ParseGGUF` reads the GGUF header and metadata only; it does not read tensor
descriptors or tensor data. `ParseHuggingFaceConfig` reads the architecture
fields from `config.json`. Both parsers use the model's declared context as the
default request context; callers can override it with the `WithContext`
variants. GGUF file size can be supplied separately when it is not present in
metadata. When no weight/file size is available, parameter count and a
recognized quantization estimate weight bytes as
`ceil(parameters * quantization_bits / 8)`. Unknown quantization is not guessed.

Memory checks use each device's `FreeMemoryMB`, not host total RAM. A
`SafetyMarginMB` is reserved on each selected device. CPU devices are excluded
unless `AllowCPUOffload` is true and the candidate explicitly supplies free
memory for that CPU device. The KV-cache estimate is
`2 * layers * kv_heads * head_dim * requested_context * bytes_per_element`;
the element size defaults to two bytes and can be set in policy.
Managed shard-group creation currently reserves a fixed 2048 MiB per device;
the dashboard reports this margin and the planner refuses a placement that
cannot fit weights, KV cache, and the reserve together.

## Modes

- `llamacpp-rpc` accepts heterogeneous devices and allocates contiguous layer
  ranges in proportion to free device memory, subject to each device's memory
  limit and safety margin. A non-validated NPU can only be selected as a
  single-device whole-model placement; using an NPU as a layer shard requires
  `Capabilities.ValidatedRPC`. The plan's `RPC` and `TensorSplit` fields are
  the comma-separated values for `--rpc` and `--tensor-split`. Tensor-split
  weights reflect the final per-node layer allocation after memory limits.
- `vllm-mp` is Linux-only and requires homogeneous GPU nodes with the same
  device count and GPU signature. The default topology keeps tensor parallelism
  within each node (`tensor_parallel_size = devices_per_node`) and uses
  pipeline parallelism across nodes (`pipeline_parallel_size = node_count`).
  `VLLMParallelismAuto` selects cross-node tensor parallelism only when every
  node advertises a link meeting `MinimumLinkSpeedMbps`; otherwise it selects
  pipeline parallelism. An explicit cross-node tensor-parallel request returns
  a typed `RefusalLinkTooSlow` when the minimum is not met.

Every successful plan carries a `Rationale` list with selected and rejected
nodes/devices, memory allocations, and topology decisions. A failure returns a
typed `*Refusal` with a stable code, a readable message, and rationale.
