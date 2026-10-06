# mAIstr0

mAIstr0 is a local-first orchestrator for distributing LLM work across machines
on a LAN. The orchestrator discovers worker nodes, routes prompts to models
available on those nodes, and provides a dashboard and OpenAI-compatible API.

## Quick start

Run `maistr0.exe` on Windows, or the `maistr0` binary on Linux. By default,
the application auto-selects a role: it joins a discovered orchestrator as a
node, or starts a combined orchestrator and node if none is found. The
dashboard opens on the local machine.

Run only an orchestrator or node explicitly:

```powershell
.\maistr0.exe --role orchestrator
.\maistr0.exe --role node --orchestrator-addr http://192.168.1.10:7450
```

```sh
./maistr0 --role orchestrator
./maistr0 --role node --orchestrator-addr http://192.168.1.10:7450
```

The default ports are `7450` for the orchestrator and `7451` for a node. Use
`--no-discovery` to disable LAN discovery or `--no-browser` to prevent opening
the dashboard automatically. Node and orchestrator JSON configuration paths
can be set with `--node-config` and `--orchestrator-config`.

## Dashboard

The dashboard provides three views:

- **Interactive Orchestrator** for conversational requests, tool activity, and
  cluster-aware model selection.
- **Cluster & Workload** for node health, discovered LAN peers, model lists,
  active tasks, and routing. Open a node's terminal to chat with its models.
- **Memory & Learning** for cluster experience, recommendations, and learned
  facts.

Use **Refresh all model registries** in the Nodes section to refresh model
lists across the cluster, or refresh an individual node. Registries are also
refreshed automatically by nodes. To edit a node's properties, right-click its
node in the topology or its card in the Nodes section. The properties dialog
can change its model engine and URL, default and disabled models, advertised
and orchestrator addresses, discovery, tray and browser behavior, and memory
path. Node identity and listen address are shown as read-only settings.

## Model engines

Nodes detect local Ollama and FastFlowLM services by default. Configure
additional or remote engines in a node configuration file:

```json
{
  "listen_addr": ":7451",
  "orchestrator_addr": "http://192.168.1.10:7450",
  "engines": [
    { "name": "ollama", "url": "http://127.0.0.1:11434" },
    { "name": "vllm", "url": "http://127.0.0.1:8000" }
  ]
}
```

Ollama and vLLM/FastFlowLM support token streaming for generation. Other
engines can still serve requests, but their streamed responses may arrive as
a single completed chunk. Temperature and maximum-token settings are passed
to engines that support those options.

## API

Submit work through `POST /api/tasks` with a JSON body such as
`{"description":"Summarize this report"}`. Work is rejected with a service
unavailable response if any planned subtask has no compatible model, rather
than accepting only part of the request. Query task status and results through
`GET /api/tasks` or `GET /api/tasks/{id}`. Cancel a running task with
`POST /api/tasks/{id}/cancel`.

`POST /v1/chat/completions` accepts OpenAI-style messages and supports streamed
responses using `"stream": true`. The orchestrator preserves system, user,
assistant, and tool message roles in its model prompt.

Task decomposition is currently heuristic. Results are combined in subtask
order into a readable task result; they are not yet synthesized into a new
model-generated answer. Dependency-aware planning, retry/reassignment after
node loss, and persistent task history remain possible future extensions.

## Development

Run the test suite with:

```sh
go test ./...
```

On Windows, cross-compile the release binaries for Windows and Linux with:

```powershell
.\build\build.ps1
```

On Linux or macOS, use `./build/build.sh`. Both scripts write versioned
binaries to `dist/windows-amd64/`, `dist/linux-amd64/`, and
`dist/linux-ubuntu-amd64/`. Tests use isolated temporary data stores; normal
runs store persistent memory in the per-user data directory.
