# mAIstr0

mAIstr0 is a local-first Go application for planning and routing LLM work
across trusted LAN machines. It includes a web dashboard, worker nodes,
interactive agents, and synchronized cluster memory; a cloud service is not
required.

Current version: **0.1.1.15**. See the [changelog](./changelog.md#unreleased)
and the [documentation index](./docs/README.md).

## Quick start

Download and extract the matching archive from
[GitHub Releases](https://github.com/dbhaigh/mAIstr0/releases).
macOS releases include Apple Silicon and Intel builds, a menu-bar app ZIP, and
a command-line archive. The app is currently unsigned and unnotarized; macOS
will show a Gatekeeper warning on first launch. Control-click the app, choose
**Open**, and confirm if you trust this download. See the
[macOS installation guide](./docs/node-service-controller.md#macos-launchagent)
for the optional background service.

On the first machine, start automatic election. If no active orchestrator is
visible on the LAN, this instance becomes the orchestrator and starts its
local worker; otherwise it joins the visible orchestrator as a node.

PowerShell:

```powershell
.\maistr0.exe --role auto
```

bash:

```sh
./maistr0 --role auto
```

To start a node on another machine, use the orchestrator's HTTPS address and
the orchestrator certificate fingerprint that you verified out of band:

PowerShell:

```powershell
.\maistr0.exe --role node `
  --orchestrator-addr https://192.168.1.10:7450 `
  --orchestrator-fingerprint "PASTE_VERIFIED_SHA256_FINGERPRINT"
```

bash:

```sh
./maistr0 --role node \
  --orchestrator-addr https://192.168.1.10:7450 \
  --orchestrator-fingerprint "PASTE_VERIFIED_SHA256_FINGERPRINT"
```

When both machines share a connected local interface prefix, a node can join
automatically if the coordinator's **Pairing access** panel has strict
verification disabled. For other connections, pair in the **Setup** panels
using the one-time PIN and independently verified certificate fingerprint.
Enable strict verification in **Pairing access** to require both checks for
every new join or re-pair. Discovery by itself never establishes trust. For
unattended enrollment, provide the PIN securely with `--pairing-pin`; do not
store or reuse it.

Default ports (configurable):

- Orchestrator API/dashboard listener: TCP `7450`
- Node API/dashboard listener: TCP `7451`
- Host service controller (paired mutual TLS): TCP `7453`
- LAN discovery: UDP `7999`
- Optional managed llama.cpp RPC relay: TCP `7452`; do not expose the raw RPC
  backend

The dashboard opens automatically by default; the orchestrator dashboard is
available locally at `http://127.0.0.1:7450`. Use `--no-browser` to suppress
browser launch, `--print-config` to inspect redacted normalized settings, and
`--diagnostics` for a prompt-free local health summary.

Persistent runtime options, including `--allow-peer-updates`, can be edited in
the dashboard and take effect after restart. Install the companion controller
service to manage stopped nodes from the dashboard; see
[node service controller](./docs/node-service-controller.md).

## Security essentials

- Pair only machines you trust. Verify the orchestrator's 64-hex-character
  SHA-256 certificate fingerprint through an authenticated out-of-band channel
  and supply it for first-contact pairing, together with the one-time PIN.
- Discovery and election do not authenticate peers or provide partition-safe
  consensus. Mutating cluster requests still require current authority and
  authenticated trust.
- Keep the dashboard on loopback; do not expose it through an unauthenticated
  proxy or public tunnel. Keep certificates, PINs, API credentials, and model
  data private.
- Peer executable updates are disabled by default. Enabling
  `--allow-peer-updates` grants paired publishers code-execution authority.
- Shard data planes have separate trust requirements; follow the network
  guidance before enabling distributed engines.

See [pairing and security](./docs/pairing-and-security.md) before enrolling
nodes.

## Development

Run from the repository root:

```sh
go build ./...
go vet ./...
go test ./...
node --test web/app_test.js
```

Run `go test -race ./...` on Linux where the race detector and its compiler
prerequisites are available. See the [development guide](./docs/development.md)
for platform-specific helpers, integration tests, contracts, benchmarks, and
release checks.

## Documentation

Start with the [documentation index](./docs/README.md). Key references:

- [API](./docs/api.md)
- [Dashboard](./docs/dashboard.md)
- [Configuration](./docs/configuration.md)
- [Engines and deployment support](./docs/engines.md)
- [Memory and task synchronization](./docs/sync.md)
- [Pairing and security](./docs/pairing-and-security.md)
- [Architecture](./docs/architecture.md)
- [Development](./docs/development.md)
- [OpenAPI, schemas, and porting guide](./contracts/PORTING.md)
- [Contributing](./CONTRIBUTING.md), [security reporting](./SECURITY.md), and
  [license](./LICENSE)
