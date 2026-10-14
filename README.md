# mAIstr0

mAIstr0 is a local-first orchestrator for distributing LLM work across machines
on a LAN. The orchestrator discovers worker nodes, routes prompts to models
available on those nodes, and provides a dashboard and OpenAI-compatible API.

## Quick start

Run `maistr0.exe` on Windows, or the `maistr0` binary on Linux. By default,
instances announce an orchestrator candidacy for six seconds. The candidate
with the highest capability score wins, with the lowest stable ID breaking
ties. The winner starts both the orchestrator and a local worker node; all
other instances join as nodes. This lets the orchestrator host contribute its
own compute resources to cluster work.

`--role orchestrator` and `--role both` use the same election. LAN discovery
must be available to start an orchestrator; coordinator startup fails closed
when discovery is disabled. `--role node` always starts a worker. To pair a
node, open the
orchestrator dashboard's Setup panel to view its one-time PIN, then open the
node dashboard's Setup panel and enter the orchestrator's HTTPS address and
PIN. The command-line pairing option is also available:

```powershell
.\maistr0.exe --role orchestrator
.\maistr0.exe --role node --orchestrator-addr https://192.168.1.10:7450 --pairing-pin "<six-digit-pin>"
```

```sh
./maistr0 --role orchestrator
./maistr0 --role node --orchestrator-addr https://192.168.1.10:7450 --pairing-pin "<six-digit-pin>"
```

The default ports are `7450` for the orchestrator and `7451` for a node. Use
`--no-discovery` to disable LAN discovery for node-only instances or
`--no-browser` to prevent opening the dashboard automatically. Orchestrator
election requires discovery. Node and orchestrator JSON configuration paths
can be set with `--node-config` and `--orchestrator-config`.

Concurrency can be tuned in those JSON files. The orchestrator's
`max_concurrent_dispatches` setting defaults to `16` and caps subtask execution
across the cluster. A node's `max_concurrent_tasks` setting defaults to `2`;
the node advertises its active and remaining capacity in status/heartbeat
updates, and both the orchestrator and node enforce that limit. These settings
only affect new in-memory scheduling; they do not change task or memory storage
formats.

Cluster traffic uses mutual TLS with persistent per-instance certificates.
The orchestrator displays and logs a six-digit, one-use pairing PIN that
expires after 30 minutes; after each successful pairing it creates the next
PIN. Use **Generate new PIN** in the Setup panel to replace an expired or
consumed PIN. The dashboard provides a direct node pairing form and can send
TLS-identified invitations to LAN-discovered nodes; a node must confirm locally
before the invitation's PIN is used to join. Alternatively, pass the PIN to a
new node with `--pairing-pin` or the node config's `pairing_pin`
setting. Paired nodes exchange certificate pins over the authenticated
connection. Orchestrator-to-orchestrator pairing is disabled to keep a single
coordinator per LAN cluster. Plain HTTP is restricted to loopback for the
local dashboard; use HTTPS for configured cluster addresses. The combined
orchestrator and local worker are paired automatically. A machine that loses
the election joins as a node; provide the elected orchestrator's one-use PIN
with `--pairing-pin` for automatic secure enrollment, or pair it later from
the node dashboard's Setup panel.
If a running orchestrator yields, its local dashboard redirects to the worker
dashboard so pairing and invitation acceptance remain available. Existing
trust in the old orchestrator does not authorize joining the new one; use the
new leader's PIN or accept its invitation locally.
The **Local network** tab lists LAN-discovered instances regardless of pairing,
plus online cluster nodes and the local worker. Membership is labeled
separately from visibility; an associated orchestrator is shown only when known,
not inferred from whichever orchestrator happens to be visible first.
Application logs are appended to `maistr0.log` in the user's configuration
directory (`%APPDATA%\maistr0\maistr0.log` on Windows). Choose **View Logs**
from the tray icon's right-click menu to inspect pairing and TLS diagnostics,
including when the console is hidden. Logs roll over daily; dated archives are
kept for seven calendar days and are available through **Open Logs Folder** in
the same menu.

`GET /api/status` reports whether persistent memory is available. If its
database cannot be opened, the dashboard shows a warning that conversation
history and learned data will not survive restarts; the process continues
without persistent memory.

## API contracts and porting

The [OpenAPI specification](./contracts/openapi.yaml), [JSON Schemas](./contracts/schemas/),
and [porting guide](./contracts/PORTING.md) describe the language-neutral HTTP,
configuration, task, scheduler, node, discovery, and memory contracts.

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
  tool catalog available to both Pi and DeepSeek. In-flight agent turns can be
  interrupted at any time via the **Interrupt** button or the API
  (`POST /api/agent/sessions/{id}/interrupt`), cleanly halting reasoning loops,
  cancelling downstream node model dispatches, and preserving session state.
  Both harnesses can hand multi-step work to the same persistent `execute_workflow`
  engine instead of coordinating worker calls only in chat history; workflows
  triggered by an agent are cancelled if the parent session is interrupted.
- **Cluster & Workload** for node health, discovered LAN peers, model lists,
  active tasks, and routing. Its Overview has **Available nodes** and
  **Current activity** tabs for node status and current/recent task activity.
  Selecting a subtask assignment switches to Available nodes and highlights
  its target node; known members remain visible when offline. Its Setup page
  is organized into **Pairing access**, **Available nodes**, **Inference readiness**,
  and **Model catalog** tabs. Workspace, Overview, and Setup tabs support arrow
  keys, Home, and End. Content sections expand with their content and scroll
  cleanly without compressing or overlapping.
  Pending, running, completed, failed, cancelled, and unknown subtasks are
  counted separately. Tasks awaiting human approval remain visible as active
  work. Detailed Nodes, Workload, Local network, Tasks, Setup,
  and Batch submit panels remain available as workspace tabs. Node details
  include GPU, NPU, CPU, and memory utilization history when the host supports
  those metrics. On Windows, NPU utilization is collected from the NPU's
  DXCore-identified GPU Engine counters when available; unavailable telemetry
  is identified rather than treated as zero.
- **Memory & Learning** for cluster experience, recommendations, and learned
  facts.

Joined nodes synchronize experiences, facts, dialogues, and routing history
with the orchestrator every 30 seconds. Standalone peer clusters use the
elected node to relay memory between members every 15 seconds. Memory records
carry optional origin and sequence metadata; version 1 and newer peers exchange
only records newer than their persisted per-peer watermarks. Protocol version 2
adds cursor-based paging for large snapshots while keeping the upper watermark
fixed for the duration of a transfer. Intermediate pages do not advance the
receiver's peer watermark. Older peers retain the unpaged compatibility path,
and a peer whose watermark is ahead of the sender triggers a full snapshot.
Legacy memory databases are backfilled when opened without changing their
existing record fields.

Task history is synchronized separately every 15 seconds; the orchestrator
starts its task-history loop after a short startup delay. Version 1 and newer
task sync sends only records newer than each peer's watermark, while older
peers receive full snapshots. Task records also carry optional origin and
sequence metadata, and legacy task databases are backfilled automatically.
Memory and task-history requests remain limited to 64 MiB each; larger
histories transfer in multiple bounded pages. A single record larger than the
limit is rejected explicitly. The scheduler uses successful and failed
node/model/task outcomes to improve future assignments.

The Cluster & Workload **Setup** tab checks each node's engine and model
readiness, refreshes an empty model registry, and links to node settings for
engine configuration and default-model selection.
On worker-only nodes, local model inspection and updates run directly without
an HTTPS request back to the worker itself. Remote node requests still require
paired certificate pins.
Its model catalog is a curated, bundled list of pull-ready Ollama names; it
does not fetch catalog metadata or send prompts to a remote catalog service.
Choose an online target node and its download-capable engine before installing.
Installed models are shown as either usable or disabled for scheduling.
Task details show the scheduler's score components and whether accelerator
telemetry was fresh enough to influence the assignment; telemetry older than
30 seconds is ignored for routing. Scheduler load includes active node work and
workflow subtasks waiting on dependencies, with node-reported utilization
penalties applied only when the telemetry sample timestamp is current.

Nodes scan for local LLM servers at startup and refresh their model lists
automatically. Use **Rescan engines & models** on an online node card to detect
servers started or installed later, or **Rescan all nodes** to scan every online
node. Configured engines are retained alongside locally discovered engines.
To edit a node's properties, right-click its card in the Nodes section. The properties dialog
can change its model engine and URL, default and disabled models, advertised
and orchestrator addresses, discovery, tray and browser behavior, and memory
path. Node identity and listen address are shown as read-only settings.

## Model engines

Nodes detect local Ollama, LM Studio, and FastFlowLM services by default. LM
Studio is discovered at `http://127.0.0.1:1234` and can also be configured at
another OpenAI-compatible server URL. Configure
additional or remote engines in a node configuration file:

```json
{
  "listen_addr": ":7451",
  "max_concurrent_tasks": 2,
  "orchestrator_addr": "https://192.168.1.10:7450",
  "pairing_pin": "<six-digit-pin>",
  "engines": [
    { "name": "ollama", "url": "http://127.0.0.1:11434" },
    { "name": "lmstudio", "url": "http://127.0.0.1:1234" },
    { "name": "vllm", "url": "http://127.0.0.1:8000" }
  ]
}
```

An orchestrator config can set the cluster-wide cap:

```json
{
  "max_concurrent_dispatches": 16
}
```

## Configuration validation

Configuration is normalized before startup. Empty harness settings default to
`pi` (harness names are trimmed and lowercased), empty tray modes default to
`taskbar`, and zero or omitted concurrency settings use their documented
defaults. Negative concurrency values are rejected. Orchestrator configs must
keep LAN discovery enabled.

Listen addresses must use `host:port` or `:port` syntax. A node's
`orchestrator_addr` and `advertise_addr` must be empty or HTTPS URLs. Configured
engine names must be `ollama`, `vllm`, `lmstudio`, or `fastflowlm`, and their
`url` values must be HTTP or HTTPS URLs. A configured `pairing_pin` must contain
exactly six digits. When `memory_path` is set, its parent directory must be
creatable; missing parent directories are created as needed.

Invalid settings are reported individually and prevent startup with exit code
`2`.

The SSE cluster snapshot includes dispatch queue depth, active global slots,
dependency waiters, cumulative dispatch/dependency-wait duration, goroutine
count, and each node's active/max/remaining task capacity. Increase the
orchestrator cap to use more cluster-wide parallelism; increase a node's cap
only when its local engine and hardware can safely serve more concurrent
requests.

LM Studio, vLLM, and FastFlowLM use the OpenAI-compatible chat-completions API.
The scheduler adds a GPU preference to models advertised by GPU-equipped
nodes; this is a routing preference and does not guarantee that the engine
will use the GPU. Ollama and OpenAI-compatible engines support token streaming. Other
engines can still serve requests, but their streamed responses may arrive as
a single completed chunk. Temperature and maximum-token settings are passed
to engines that support those options.

## API

Submit work through `POST /api/tasks` with a JSON body such as
`{"description":"Summarize this report"}`. By default, mAIstr0 preserves its
heuristic decomposition. Set `"plan_mode":"auto"` to have a local coordinator
model produce a structured plan with agent roles and dependencies; invalid
plans are rejected rather than silently run. If the selected coordinator
model fails, the planner retries with another advertised node/model; if no
candidate succeeds, the failure is returned. Work is rejected with a service
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
    {
      "id": "research",
      "description": "Research the topic",
      "role": "researcher",
      "task_type": "general",
      "expected_output": "Evidence and key findings"
    },
    {
      "id": "summary",
      "description": "Write a concise summary",
      "role": "summarizer",
      "depends_on": ["research"],
      "expected_output": "A concise, evidence-grounded summary"
    }
  ]
}
```

Plans are checked for missing dependencies and cycles. Heuristic decomposition
also recognizes clear sequential cues such as “then” and “after that”; other
subtasks remain parallel. Supported roles include researcher, analyst, coder,
writer, reviewer, summarizer, translator, and general. Subtasks may specify
`expected_output`, `output_format:"json"`, `required_fields`, and
`max_retries` (0–2); JSON contracts are checked before dependent work starts.
Set `"require_review":true` to add a reviewer task that must return
`{"approved":true,...}` before synthesis, or `"require_approval":true` to
pause completed work for a human. Approve through
`POST /api/tasks/{id}/approve`; the approval state and outputs survive
restarts. Multi-task results are synthesized by a cluster model only after
all required work succeeds. Work that was still running when the orchestrator
stopped is restored as failed with an interruption reason rather than being
reported as running.

`POST /v1/chat/completions` accepts OpenAI-style messages and supports streamed
responses using `"stream": true`. The orchestrator preserves system, user,
assistant, and tool message roles in its model prompt.

The Ollama-compatible surface includes `GET /api/tags`, `POST /api/chat`, and
`POST /api/generate` (use the Ollama request fields such as `stream` and
`options`). Anthropic Messages clients can use `POST /v1/messages` with
`max_tokens`; text messages and text content blocks are supported. Streaming
is supported by both compatibility APIs. Existing mAIstr0 generation requests
and the OpenAI-compatible routes remain available.
The bundled model catalog is available at `GET /api/models/catalog`, with
optional `engine` and `q` filters. Model pulls can specify both `model` and
`engine` in `POST /api/nodes/{id}/models/pull`.

Automatic decomposition remains heuristic; callers that need reliable
dependencies should provide an explicit plan. GPU utilization and free-memory
telemetry influence scheduling when supported by NVIDIA tooling. CPU and
memory utilization samples are retained for display when reported by the
node's operating system.

## Development

Run the test suite with:

```sh
go test ./...
go vet ./...
node --test web/app_test.js
```

The Go suite includes a process-level integration test for pairing, authenticated
cluster requests, streaming inference, and session restoration after restart.
Regenerate characterization contract fixtures with
`go test ./internal/orchestrator ./internal/scheduler ./internal/taskmgr ./internal/config -args -update`;
review the resulting golden-file changes before committing.
The GitHub Actions workflow runs these checks and builds the application on
Windows and Linux.

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
