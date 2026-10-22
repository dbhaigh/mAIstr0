# llama.cpp RPC deployment

This Compose file is an optional Linux deployment helper. It does not build or
bundle llama.cpp; provide compatible images that contain `ggml-rpc-server` and
`llama-server`, both compiled with `GGML_RPC=ON`. Use the same llama.cpp source
revision on the coordinator and workers.

## Build the runtime

Build llama.cpp on each host for its CPU or accelerator backend. For example:

```sh
cmake -S . -B build-rpc-cpu -DCMAKE_BUILD_TYPE=Release -DGGML_RPC=ON
cmake --build build-rpc-cpu --config Release --parallel

cmake -S . -B build-rpc-cuda -DCMAKE_BUILD_TYPE=Release -DGGML_RPC=ON -DGGML_CUDA=ON
cmake --build build-rpc-cuda --config Release --parallel

cmake -S . -B build-rpc-rocm -DCMAKE_BUILD_TYPE=Release -DGGML_RPC=ON -DGGML_HIP=ON
cmake --build build-rpc-rocm --config Release --parallel
```

Package the resulting `ggml-rpc-server` and `llama-server` executables in
images available to Compose. The image must put the selected executable on
`PATH`; provide `LLAMACPP_RPC_IMAGE` and `LLAMACPP_SERVER_IMAGE` as image
references. Both may point to the same image. For CUDA, install and configure
NVIDIA Container Toolkit. The ROCm profiles pass through `/dev/kfd` and
`/dev/dri`; install the matching AMD host drivers and runtime. Other backends
such as SYCL, Vulkan, OpenVINO, MUSA, Metal, and vendor NPUs need their own
compatible builds and host-device setup.

## Configure and start

Create a `.env` file in this directory. Use a separate file on each machine:

```dotenv
LLAMACPP_RPC_IMAGE=local/llama.cpp:rpc-cuda
LLAMACPP_SERVER_IMAGE=local/llama.cpp:rpc-cuda
RPC_BIND_IP=192.168.1.21
LLAMACPP_MODEL_DIR=/srv/llama/models
LLAMACPP_MODEL_FILE=model.gguf
LLAMACPP_RPC_SERVERS=192.168.1.21:50052,192.168.1.22:50052
LLAMACPP_GPU_LAYERS=99
```

Set `LLAMACPP_GPU_LAYERS` high enough for the target model; a zero value
disables layer offload and defeats the intended remote compute use.

On each worker, set `RPC_BIND_IP` to that worker's trusted private-interface
address and start exactly one worker profile matching its build:

```sh
docker compose --profile rpc-worker-cuda up -d
```

On the coordinator, point `LLAMACPP_RPC_SERVERS` at every worker and set
`LLAMACPP_MODEL_DIR` to a directory containing the GGUF file. Start one
inference profile matching the coordinator image:

```sh
docker compose --profile inference-cuda up -d
```

The HTTP API binds to `127.0.0.1:8080`; configure the mAIstr0 node running on
the coordinator host with:

```json
{
  "engines": [
    { "name": "llamacpp", "url": "http://127.0.0.1:8080" }
  ]
}
```

Use `rpc-worker-cpu` / `inference-cpu` or `rpc-worker-rocm` /
`inference-rocm` for those builds. Keep the RPC worker port reachable only by
the selected coordinator. llama.cpp RPC is experimental and insecure; never
expose it to the public internet or an untrusted network, or use it in a
sensitive environment. The HTTP endpoint is loopback-bound by default; put
any remotely accessible API behind an authenticated, TLS-terminating proxy.
