# mAIstr0

mAIstr0 is a local-first orchestrator for distributing LLM work across machines
on a LAN. The orchestrator discovers worker nodes, routes prompts to models
available on those nodes, and provides a dashboard and OpenAI-compatible API.

## Quick start

Run `maistr0.exe` on Windows, or the `maistr0` binary on Linux. By default,
the application auto-selects a role: it joins a discovered orchestrator as a
node, or starts a combined orchestrator and node if none is found. The
dashboard opens on the local machine.

Run only an orchestrator or node explicitly. To pair a node, open the
orchestrator dashboard's Setup panel to view its one-time PIN, then open the
node dashboard's Setup panel and enter the orchestrator's HTTPS address and
PIN. The existing command-line pairing options are also available.
To federate two orchestrators, start the joining orchestrator with the
receiving orchestrator's HTTPS address and active PIN:

```powershell
.\maistr0.exe --role orchestrator
.\maistr0.exe --role orchestrator --harness pi
.\maistr0.exe --role node --orchestrator-addr https://192.168.1.10:7450 --pairing-pin "<six-digit-pin>"
.\maistr0.exe --role orchestrator --pair-to https://192.168.1.10:7450 --pairing-pin "<six-digit-pin>"
```

```sh
./maistr0 --role orchestrator
./maistr0 --role orchestrator --harness pi
./maistr0 --role node --orchestrator-addr https://192.168.1.10:7450 --pairing-pin "<six-digit-pin>"
./maistr0 --role orchestrator --pair-to https://192.168.1.10:7450 --pairing-pin "<six-digit-pin>"
```

The default ports are `7450` for the orchestrator and `7451` for a node. Use
`--no-discovery` to disable LAN discovery or `--no-browser` to prevent opening
the dashboard automatically. Node and orchestrator JSON configuration paths
can be set with `--node-config` and `--orchestrator-config`.

Cluster traffic uses mutual TLS with persistent per-instance certificates.
The orchestrator displays and logs a six-digit, one-use pairing PIN that
expires after 30 minutes; after each successful pairing it creates the next
PIN. The dashboard provides a direct node pairing form. Alternatively, pass
the PIN to a new node with `--pairing-pin` or the node config's `pairing_pin`
setting. Paired peers exchange certificate pins over the authenticated
connection. Plain HTTP is restricted to loopback for the local dashboard;
use HTTPS for configured cluster addresses. The combined `--role both` mode
pairs its local worker automatically.
Orchestrators that share a paired trust roster securely exchange cluster
membership over mutual TLS. Pairing initiated from one side establishes trust
in both directions; use the receiving orchestrator's current PIN.

## Orchestrator agent backends

The interactive orchestrator uses [Pi](https://github.com/earendil-works/pi)
by default, with a native Go implementation of the agent loop. Pi selects
coordinator models from healthy nodes in the local mAIstr0 cluster, and does
not send prompts to OpenAI or another cloud provider. No API key, Node.js, or
Pi CLI installation is required. Choose a specific local coordinator model in
the dashboard or leave it on Auto for cluster routing. It does not include
Pi's standalone coding CLI/TUI, filesystem or shell tools, or extension
compatibility. Pi and the built-in DeepSeek harness share mAIstr0's cluster,
model, task, web, math, and persistent-memory tools. To use DeepSeek instead,
start mAIstr0
with `--harness deepseek`. The Interactive Orchestrator sidebar can switch the
default harness at runtime; this applies to new conversations, while existing
conversations stay on their original harness. Runtime changes last until the
orchestrator restarts; use the `harness` setting in the orchestrator
configuration to choose the startup default.

```json
{
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
  tool catalog available to both Pi and DeepSeek.
- **Cluster & Workload** for node health, discovered LAN peers, model lists,
  active tasks, and routing. Its Overview pairs current and recent task
  activity with the nodes assigned to each subtask. Selecting an assignment
  highlights its target node; known members remain visible when offline.
  Pending, running, completed, failed, cancelled, and unknown subtasks are
  counted separately. Detailed Nodes, Workload, Local network, Tasks, Setup,
  and Batch submit panels remain available as workspace tabs. Node details
  include GPU, CPU, and memory utilization history when the host supports
  those metrics; unavailable telemetry is identified rather than treated as
  zero.
- **Memory & Learning** for cluster experience, recommendations, and learned
  facts.

The Cluster & Workload **Setup** tab checks each node's engine and model
readiness, refreshes an empty model registry, and links to node settings for
engine configuration and default-model selection.

Use **Refresh all model registries** in the Nodes section to refresh model
lists across the cluster, or refresh an individual node. Registries are also
refreshed automatically by nodes. To edit a node's properties, right-click its
card in the Nodes section. The properties dialog
can change its model engine and URL, default and disabled models, advertised
and orchestrator addresses, discovery, tray and browser behavior, and memory
path. Node identity and listen address are shown as read-only settings.

## Model engines

Nodes detect local Ollama and FastFlowLM services by default. Configure
additional or remote engines in a node configuration file:

```json
{
  "listen_addr": ":7451",
  "orchestrator_addr": "https://192.168.1.10:7450",
  "pairing_pin": "<six-digit-pin>",
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

For precise multi-step workflows, submit an explicit plan. Dependencies name
other subtask IDs; dependent work waits for its prerequisites and receives
their outputs as context:

```json
{
  "description": "Research and summarize a topic",
  "subtasks": [
    { "id": "research", "description": "Research the topic", "task_type": "general" },
    { "id": "summary", "description": "Write a concise summary", "depends_on": ["research"] }
  ]
}
```

Plans are checked for missing dependencies and cycles. Heuristic decomposition
also recognizes clear sequential cues such as “then” and “after that”; other
subtasks remain parallel. Multi-subtask results are synthesized by a cluster
model after all subtasks succeed. Task and subtask results persist across
restarts; work that was still active when the orchestrator stopped is restored
as failed with an interruption reason rather than being reported as running.

`POST /v1/chat/completions` accepts OpenAI-style messages and supports streamed
responses using `"stream": true`. The orchestrator preserves system, user,
assistant, and tool message roles in its model prompt.

The Ollama-compatible surface includes `GET /api/tags`, `POST /api/chat`, and
`POST /api/generate` (use the Ollama request fields such as `stream` and
`options`). Anthropic Messages clients can use `POST /v1/messages` with
`max_tokens`; text messages and text content blocks are supported. Streaming
is supported by both compatibility APIs. Existing mAIstr0 generation requests
and the OpenAI-compatible routes remain available.

Automatic decomposition remains heuristic; callers that need reliable
dependencies should provide an explicit plan. GPU utilization and free-memory
telemetry influence scheduling when supported by NVIDIA tooling. CPU and
memory utilization samples are retained for display when reported by the
node's operating system.

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
`dist/linux-ubuntu-amd64/`, and record the release number in `dist/version.txt`.
The release number comes from `internal/version/version.go` and is shown by
running `maistr0 --version`. Tests use isolated temporary data stores; normal
runs store persistent memory in the per-user data directory.
