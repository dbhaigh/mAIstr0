# Agent backends

The orchestrator uses a capability-aware registry for interactive agent
backends. Pi remains the default, and the built-in Pi and DeepSeek adapters
retain their existing session and tool behavior. An integration can provide
additional `agent.BackendRegistration` values to
`orchestrator.NewWithConfigAndBackends`; this is a Go registration point, not a
runtime plugin loader or a promise that an external harness is installed.
Registrations are used only in full mode; minimal mode remains harness-free.

Backend names are lowercase identifiers: an ASCII letter followed by up to
63 lowercase letters, digits, hyphens, or underscores. The existing
`harness` configuration key and `--harness` option are unchanged. Omitting the
setting still selects Pi. A valid name that is not registered does not prevent
startup and is never replaced with another backend.

The evidence-based qualification procedure for managed-agent and
model-endpoint-client configurations is in
[harness integration qualification](./harness-qualification.md). Fake adapter
tests and opt-in real smoke tests are distinct evidence; the presence of a
smoke test does not mean it has been run.

## Capabilities

Registrations explicitly declare these capabilities:

- `session_resume`: saved sessions can be restored for further turns. A
  backend that advertises this must implement `agent.SessionRestorer`.
- `streaming`: the backend can produce streaming events for the agent message
  endpoint.
- `cancellation`: the backend honors turn cancellation and session interrupts.
- `approvals`: the backend has an approval interaction.
- `workspace_isolation`: the backend provides isolated workspaces.
- `model_selection`: the backend accepts a selected coordinator model.
- `tool_access`: the backend exposes tools to the agent.

The built-in Pi and DeepSeek adapters currently declare resume, streaming,
cancellation, model selection, and tool access. They do not declare approval
interactions or workspace isolation. External integrations are responsible for
declaring only behavior their implementation actually provides. Unsupported
streaming, model selection, or tool listing is reported rather than silently
discarded.

The dashboard lists every registered backend with its reported availability
and capability flags. `GET /api/agent/backend` also reports an additive
`backends[].execution` profile: whether the harness runtime is local, remote,
or unknown; its configured workspace when applicable; the stated isolation
mode; and approval limitations. A missing registration has unknown execution
details and no capability claims. Execution location describes the harness or
server, not where a configured model provider runs. Workspace paths are
returned by this status API for the dashboard; protect access to the
orchestrator accordingly. Credentials, prompts, and tool output are not part
of these profiles.

The dashboard offers a stop request only when the selected backend declares
cancellation support. An accepted interrupt request is not treated as proof
that execution stopped. If the harness reports an active session, refuses the
request, or returns an unknown outcome, the dashboard warns that work may
still be running; reconcile the existing conversation rather than retrying it
elsewhere.

The opt-in Claude Code adapter declares session resume, streaming, and
cancellation only. It does not expose mAIstr0 tools or cluster models, accept
coordinator-model/session tuning options, provide approvals, or claim
workspace isolation. Its CLI tool list is explicitly empty, so this adapter
currently provides Claude-backed conversation rather than codebase operations.

Enable the adapter for the normal full-mode composition root by setting
`MAISTR0_CLAUDE_CODE_EXECUTABLE` to an absolute, trusted CLI executable path,
`MAISTR0_CLAUDE_CODE_WORKSPACE` to an existing absolute workspace directory,
and `ANTHROPIC_API_KEY` in the local mAIstr0 process environment. Setting either
Claude-specific variable opts in and requires both; the backend performs a
CLI-version check at startup. Pi remains selected unless `harness` or
`--harness` is set to `claude-code`. Minimal mode does not start the adapter.

The supported Claude Code CLI range is `>=2.1.286, <2.2.0` (2.1.x beginning
with 2.1.286). The lower bound includes the documented complete `--bare`
behavior; unsupported versions fail closed. Contract tests exercise both
range boundaries and the CLI stream contract. An opt-in real smoke test is
available on a host with an installed CLI, the configured workspace, and API
access:

```powershell
$env:MAISTR0_CLAUDE_CODE_REAL_SMOKE = "1"
go test ./internal/agent/claudecode -run TestRealClaudeCodeSmoke -count=1 -v
```

The adapter uses the [documented headless CLI
interface](https://code.claude.com/docs/en/headless) and
[CLI reference](https://code.claude.com/docs/en/cli-reference):
`claude -p --output-format stream-json --verbose --include-partial-messages`.
It creates a UUID session
with `--session-id`, resumes it with `--resume`, sends prompts over stdin,
forwards text deltas, and uses the terminal `result` event as the final
assistant message. Child errors and malformed output are mapped to generic
errors; stderr, prompts, and result/error payloads are not included in logs or
process errors. Cancellation terminates the owned process tree and records an
interrupted turn; it is not a Claude SDK-level interrupt acknowledgment.

Configuration boundaries are explicit: `--restricted` excludes project/user
settings and confines any built-in file tools to the fixed working directory;
`--bare` disables hook/plugin/CLAUDE.md discovery; `--tools ""`,
`--strict-mcp-config` with an empty MCP configuration, and `--disallowedTools
mcp__*` disable built-in and MCP tools; `--permission-mode plan` with
`--permission-prompts none` disables approval interactions. Managed machine or
organization policy may still apply. The adapter does not enable any tools, so
the configured workspace is only the CLI working directory, not an accessible
project sandbox. The process runs as the mAIstr0 OS user and can access
resources allowed to that user; `--bare` is not a sandbox.

Bare mode does not use a Claude subscription login. The adapter supports
Anthropic API-key authentication only and explicitly forwards only
`ANTHROPIC_API_KEY` plus the current OS user's home-directory variable
(`HOME` or `USERPROFILE`; Windows also gets `SystemRoot`) to the local CLI
child. The API key is not put in argv, files, logs, or mAIstr0 session records.
The CLI writes its own session data using its local Claude Code data location.
This adapter makes requests to Anthropic; it
does not route Claude through mAIstr0 inference or expose cluster inference
tools.

The opt-in Codex CLI adapter declares session resume, streaming, and
cancellation only. It exposes no mAIstr0 tools or cluster models, does not
accept session tuning options, and does not claim approvals or workspace
isolation. It uses Codex's own read-only command sandbox with approvals set to
`never`; actions outside that sandbox are denied rather than prompting or
falling back to broader access.

Enable the adapter in full mode by setting `MAISTR0_CODEX_EXECUTABLE` to an
absolute trusted CLI executable path, `MAISTR0_CODEX_WORKSPACE` to an existing
absolute workspace directory, and `MAISTR0_CODEX_HOME` to an existing local
Codex data directory outside that workspace. Setting any of these variables
opts in and requires all three. Pi remains selected unless `harness` or
`--harness` is set to `codex`. Minimal mode does not start the adapter.

The supported CLI range is `>=0.171.0, <0.172.0`, matching the documented
`rust-v0.171.0` release line. Startup checks the version and the CLI help for
the required JSONL, read-only sandbox, approval, configuration, and
strict-configuration flags; unsupported versions or missing policy flags fail
closed. `codex exec --json` events map `thread.started` IDs to mAIstr0 sessions,
`agent_message` item updates to text deltas, completed agent messages to the
final result, and terminal `turn.failed`/`error` events to generic failures.
Resume invokes `codex exec resume` only with the recorded native thread ID.
The mapping is stored as a mode-0600 file in
`CODEX_HOME/maistr0-sessions/`; the existing mAIstr0 session record format is
unchanged. Deleting the mAIstr0 session removes its mapping but does not delete
Codex's own local transcript.

The CLI receives explicit `--sandbox read-only` and `--ask-for-approval never`
flags, strict configuration overrides `approval_policy="never"` and
`sandbox_mode="read-only"`, `--ignore-user-config`, an empty MCP-server config,
and the operator-approved working directory. Codex's OS sandbox is the
enforcement boundary for model-generated commands; the runner's fixed working
directory alone is not a sandbox. Unsupported versions or policy flags fail at
startup, and turns never retry with broader access. Read-only mode allows
inspection but does not let the agent edit the workspace. The process runs as
the mAIstr0 OS user, so this is not a substitute for OS account separation.

Authentication is handled by Codex CLI using the configured `CODEX_HOME`
login (ChatGPT sign-in or an OpenAI API key configured through `codex login`).
`--ignore-user-config` still permits authentication from `CODEX_HOME`; it
prevents loading the user's Codex config. The backend does not inherit
credentials from the mAIstr0 environment, does not place them in argv, and
does not log raw CLI output. Codex may store credentials in its local auth file
or OS credential store and persists its own transcripts in `CODEX_HOME`;
restrict access to that directory. This backend calls Codex/OpenAI directly.
It does not use mAIstr0 inference: mAIstr0's translated Chat Completions
endpoint is not Codex's documented Responses protocol, and `/v1/responses` is
not implemented.

An opt-in real smoke test logs the exact CLI version it exercised. Run it only
with a supported CLI version, a configured `CODEX_HOME` login, and a
disposable workspace:

```powershell
$env:MAISTR0_CODEX_REAL_SMOKE = "1"
go test ./internal/agent/codexcli -run TestRealCodexSmoke -count=1 -v
```

See the [Codex CLI reference](https://developers.openai.com/codex/developer-commands),
[authentication guidance](https://developers.openai.com/codex/auth), and
[sandbox and approval guidance](https://developers.openai.com/codex/agent-approvals-security)
for upstream behavior. The smoke test is optional; fake-process contract
coverage does not establish real-harness compatibility.

The opt-in OpenCode backend supports the OpenCode server API, not the CLI's
shared-server default. It can either own a private foreground server
(`managed`) or connect to an explicitly configured server (`connect`); it
never stops a connected server. Pi remains selected unless `harness` or
`--harness` is set to `opencode`, and minimal mode does not start this backend.

Enable managed mode in full mode by setting
`MAISTR0_OPENCODE_MODE=managed`,
`MAISTR0_OPENCODE_EXECUTABLE` to an absolute trusted OpenCode executable,
`MAISTR0_OPENCODE_WORKSPACE` to an existing absolute workspace, and
`MAISTR0_OPENCODE_HOME` to an existing OpenCode home outside that workspace.
Restrict filesystem access to that home because it may contain provider
credentials. The backend starts `opencode serve` bound to loopback with
isolated home/XDG directories and a generated server password. Configure the
provider authentication in that OpenCode home before use; credentials are not
copied from the mAIstr0 process environment.

For connect mode set `MAISTR0_OPENCODE_MODE=connect`,
`MAISTR0_OPENCODE_WORKSPACE`, `MAISTR0_OPENCODE_SERVER_URL`,
`MAISTR0_OPENCODE_SERVER_PASSWORD`, and
`MAISTR0_OPENCODE_SESSION_DIR`. The session directory must be absolute and
outside the workspace; restrict its filesystem access. Mapping files use
mode 0600 where supported and store only local/native session IDs and message
cursors, not prompts. The URL must use HTTPS except for loopback addresses.
Connect mode authenticates to the OpenCode server with HTTP Basic
authentication; provider credentials remain configured on that server.
Mode-specific executable/home or server URL/password variables are rejected
when they do not belong to the selected mode.

The supported OpenCode server API range is `>=2.3.2, <2.4.0`; startup checks
`/api/info` and fails closed outside that range. The opt-in smoke test records
the exact server version and sends a real prompt through the configured
provider:

```powershell
$env:MAISTR0_OPENCODE_REAL_SMOKE = "1"
go test ./internal/agent/opencode -run TestRealOpenCodeSmoke -count=1 -v
```

The backend maps mAIstr0 session IDs to native OpenCode session IDs and resumes
only sessions with a valid local mapping and matching workspace. It waits for
the server's idle confirmation and reconciles the native transcript before
starting a new turn, including after a restart. It currently returns only a
final assistant message; streaming events are not implemented. Terminal
failures are generic and raw server bodies are discarded.

The backend denies all permissions by default, then allows OpenCode read,
edit, glob, and grep actions within the configured workspace. Shell and
external-directory access are denied; loaded plugins or MCP servers cause
startup and pre-turn policy checks to fail closed. These OpenCode permission
rules are not OS sandboxing: the server process runs as the mAIstr0 OS user.
The backend exposes no mAIstr0 tools, approval flow, model selection, or
workspace-isolation capability. A cancellation or deletion is successful
only after the server reports idle, or confirms the native session is absent.
If shutdown cannot confirm idle, `Close` returns an error and does not stop a
managed server; a caller may retry while the backend remains alive. Do not
treat stopping the client or server process as evidence that remote work
stopped.

**mAIstr0 as an OpenCode model provider is separate and not currently tested
end to end.** The server-backend smoke test uses the provider configured in
OpenCode; it does not point OpenCode at mAIstr0. The
[model endpoint matrix](./client-compatibility.md) describes the documented
OpenCode custom-provider route and mAIstr0's text-only Chat Completions
translation, but does not certify an OpenCode provider configuration or
coding-agent tool loop. No mAIstr0 provider setup is supported by this
backend.

See the [OpenCode server documentation](https://opencode.ai/docs/server/),
[provider documentation](https://opencode.ai/docs/providers/), and
[v2.3.2 release](https://github.com/anomalyco/opencode/releases/tag/v2.3.2)
for upstream behavior. Fake-server/process tests do not establish compatibility
with a real OpenCode server or provider.

The opt-in Gemini CLI adapter uses the CLI's headless `stream-json` JSONL
interface. It declares session resume, streaming, and process-tree cancellation;
it exposes no mAIstr0 tools or cluster models, does not accept session tuning
options, and does not provide an mAIstr0 approval interaction. Pi remains
selected unless `harness` or `--harness` is set to `gemini-cli`; minimal mode
does not start this backend.

Enable the adapter in full mode with
`MAISTR0_GEMINI_EXECUTABLE` set to an absolute trusted CLI executable,
`MAISTR0_GEMINI_WORKSPACE` set to an existing absolute workspace,
`MAISTR0_GEMINI_HOME` set to an existing local directory outside that workspace,
`MAISTR0_GEMINI_SANDBOX` set to an explicit supported sandbox provider, and
`MAISTR0_GEMINI_AUTH_MODE` set to one of `api-key`, `vertex-api-key`, or
`vertex-service-account`. Setting any of these five configuration variables
opts in and requires the complete configuration. The backend creates its isolated
CLI home and session-ID mapping directory under the configured home. Restrict
access to this directory because Gemini CLI stores its local session data there.

`api-key` forwards only `GEMINI_API_KEY`. `vertex-api-key` forwards only
`GOOGLE_API_KEY`, `GOOGLE_CLOUD_PROJECT`, and `GOOGLE_CLOUD_LOCATION`, and
selects Vertex AI explicitly. `vertex-service-account` forwards the absolute
`GOOGLE_APPLICATION_CREDENTIALS` file path plus the project and location; the
credentials file must be outside the workspace. These are the supported
non-interactive authentication modes. Cached interactive OAuth credentials and
user settings in the isolated CLI home are rejected; the adapter does not
perform browser sign-in. Credentials remain in the local process environment
and configured credential file, not mAIstr0 argv, logs, or session records.
Gemini CLI may forward the selected credentials into its local sandbox; with
Docker/Podman-backed sandboxes, the upstream launcher may expose API-key values
in the local container-runtime command line. The adapter cannot prevent that
upstream behavior, so restrict the mAIstr0 OS account and use only trusted
workspaces and prompts.

The supported Gemini CLI version range is `>=0.70.0, <0.71.0`. Startup checks
the exact CLI version and required headless, session, approval, and sandbox
flags. To run the opt-in real smoke test:

```powershell
$env:MAISTR0_GEMINI_REAL_SMOKE = "1"
go test ./internal/agent/geminicli -run TestRealGeminiCLISmoke -count=1 -v
```

The adapter passes prompts through stdin and uses only a fixed headless-mode
marker in argv. It requires `--approval-mode=plan`, `--sandbox`, and an explicit
`GEMINI_SANDBOX` provider. Its generated system settings disable hooks,
extensions, MCP, and skills; force plan approval; and configure no extra
filesystem paths. The system-settings override is used instead of the host's
system policy. Project instructions such as `GEMINI.md` may still be included
as model context. Prompts, selected workspace content, and tool results are sent
to the configured Google provider; use only a trusted workspace. The
selected sandbox provider must be installed and operational; a failed sandbox
tool execution is surfaced as a tool-result error, with no fallback to
unsandboxed tools. Supported provider names are `windows-native` on Windows,
`sandbox-exec` on macOS, `runsc` or `lxc` on Linux, and `docker` or `podman`
where supported by the installed Gemini CLI. The CLI runs with the configured
workspace as its working directory and no additional allowed filesystem paths.
Sandbox network access remains enabled so the CLI can reach its Google model
provider; sandboxed tools may also access the network, and this backend does not
provide network egress filtering. The process runner's working directory alone
is not a sandbox, and this backend does not declare `workspace_isolation`.
Cancellation kills the owned local process tree but does not receive a separate
acknowledgment from an external sandbox daemon.

The `init` event's native session ID must match the mAIstr0 session ID;
mode-restricted mapping files store IDs only, and `--resume` is used only when
a valid mapping exists. Assistant message deltas, tool-use/result events, error
events, and the terminal `result` are validated and mapped. A successful
process exit without a successful terminal result is an error. Raw child
stderr, prompts, credentials, and error payloads are not included in backend
errors or logs.

**mAIstr0 inference is not supported or tested for Gemini CLI.** The CLI uses
the selected Google AI Studio or Vertex AI authentication mode. Gemini's native
`generateContent` protocol is not implemented by mAIstr0; its translated
Chat Completions endpoint is not claimed to be a compatible provider.

See the [headless CLI documentation](https://geminicli.com/docs/cli/headless/),
[authentication guidance](https://geminicli.com/docs/get-started/authentication/),
[sandbox documentation](https://geminicli.com/docs/cli/sandbox/), and
[CLI reference](https://geminicli.com/docs/cli/cli-reference/) for upstream
behavior. Fake-process coverage does not establish compatibility with a real
Gemini CLI, sandbox provider, Google account, or model.

## OpenHands Agent Server

The opt-in OpenHands backend connects to an operator-managed Software Agent
SDK / Agent Server. It does not use or launch the legacy headless CLI and does
not start, stop, or otherwise own the configured server. Configure all of:

- `MAISTR0_OPENHANDS_SERVER_URL`: the Agent Server origin. Remote servers must
  use HTTPS; plain HTTP is accepted only for loopback.
- `MAISTR0_OPENHANDS_SESSION_API_KEY`: the server's session API key. It is sent
  in the `X-Session-API-Key` header and the session socket authentication frame.
- `MAISTR0_OPENHANDS_WORKSPACE`: the absolute path as seen by the server's
  `LocalWorkspace`. The created conversation is checked against this exact path.
- `MAISTR0_OPENHANDS_MODEL`: a model identifier configured for that server.
- `MAISTR0_OPENHANDS_SESSION_DIR`: an absolute local directory for
  mAIstr0-to-OpenHands session mapping metadata. This is not a provider
  credential store.

Startup requires the exact Agent Server version `1.63.1` from `/server_info`,
verifies that the API key is enforced, and rejects redirects. Conversation
creation explicitly selects `LocalWorkspace`, `conversation_runtime: local`,
`TerminalTool` and `FileEditorTool`, with empty MCP/plugin lists and no hooks.
The server's confirmation policy is `NeverConfirm`; mAIstr0 does not implement
OpenHands approval requests. Consequently, this backend can execute tools
without a per-action approval prompt.

**The configured workspace is not an OS security boundary.** A LocalWorkspace
selects the directory OpenHands operates in; it does not prove that the server
process or its tools are confined to that directory. Run Agent Server under a
dedicated, least-privilege account and an operator-verified isolation
deployment, restrict its network exposure, and protect the API key. The
backend does not claim workspace isolation and does not advertise that
capability. It will not fall back to a worktree or silently change the
requested workspace. mAIstr0 stores and sends the API key only in its
configured local process trust boundary and does not log prompts or raw server
responses.

Prompts use the server's resumable session WebSocket. Durable events are
replayed by sequence after disconnect, and a pending conversation is reconciled
before a new prompt after mAIstr0 restarts. Cancellation is reported only after
the server reports a stopped/paused or completed state; if that cannot be
confirmed, the session remains active with an unknown outcome. Session IDs
remain owned by OpenHands and are not reassigned to another backend.

The model provider and its credentials are configured on the Agent Server.
`MAISTR0_OPENHANDS_SESSION_API_KEY` authenticates mAIstr0 to that server; it is
not a model-provider key. **Using mAIstr0 as the OpenHands model provider is
not supported or tested.** In particular, this adapter does not claim that
mAIstr0's Chat Completions endpoint implements the protocol or request
capabilities expected by OpenHands.

Fake HTTP/WebSocket tests cover key enforcement, workspace selection, durable
event mapping, reconnect/replay, restored pending work, cancellation
confirmation, and malformed/sensitive server output. They do not establish
real-server compatibility or isolation. To run the opt-in smoke test, configure
the five variables above and set `MAISTR0_OPENHANDS_REAL_SMOKE=1`:

```text
go test ./internal/agent/openhands -run TestRealOpenHandsSmoke -count=1 -v
```

The smoke test logs the exact server version it exercised, creates a temporary
conversation, sends a prompt, and attempts to delete the conversation. Run it
only against a server and model configuration approved for that test.

The implementation is pinned to the Agent Server `v1.63.1` API:
[Software Agent SDK](https://github.com/OpenHands/software-agent-sdk) and
[v1.63.1 release](https://github.com/OpenHands/software-agent-sdk/releases/tag/v1.63.1).

## Hermes Agent API Server

The opt-in Hermes backend connects to an operator-managed API Server Runs
interface. It does not launch or own the gateway. Configure all of:

- `MAISTR0_HERMES_SERVER_URL`: API Server origin; remote connections must use
  HTTPS, while plain HTTP is accepted only for loopback. A `/p/<profile>`
  prefix is supported.
- `MAISTR0_HERMES_API_KEY`: bearer key required by the server.
- `MAISTR0_HERMES_SESSION_DIR`: absolute local directory for protected
  mAIstr0-to-Hermes session/run mappings.

Startup accepts exactly API Server `0.23.3` and verifies the server advertises
required bearer authentication and durable run idempotency, status, SSE event,
and stop operations. The adapter sends only input, session ID, and supported
instructions/model fields. It does not configure Hermes tools, skills, hooks,
MCP, memory, or approval policy. Those remain under gateway operator control;
use a dedicated, least-privilege Hermes deployment and protect its credentials.
This adapter does not provide an execution sandbox or claim workspace
isolation. API keys remain in the configured local process trust boundary;
prompts and raw tool output are not logged or persisted in the mapping sidecar.

Hermes owns its sessions and accepted runs. mAIstr0 creates explicit
`hermes-` session IDs and persists only the run ID, durable idempotency key,
and replay sequence in mode-0600 mapping files. After a disconnect, it resumes
the accepted run using SSE replay and status polling; it never resubmits an
ambiguous admission. Cancellation is confirmed only after terminal `cancelled`
status, not from a `stopping` response. Cancelling local reconciliation or
shutting down mAIstr0 detaches its watcher and does not stop gateway-owned work.
An interrupted or unavailable run remains unresolved and blocks a new prompt
until reconciled. The backend does not claim per-action approvals.

The model and provider credentials are configured on Hermes. Compatibility
with mAIstr0 inference is **not established**; the adapter does not assume
mAIstr0's Chat Completions endpoint implements the protocol or features Hermes
requires.

Fake API tests cover fragmented SSE, reconnect/replay, authentication,
malformed/failed responses, sensitive-output redaction, cancellation
acknowledgment, restart reconciliation, and shutdown detachment. They do not
establish real-server compatibility or isolation. To run the opt-in smoke test,
configure the three variables above and set `MAISTR0_HERMES_REAL_SMOKE=1`:

```text
go test ./internal/agent/hermes -run TestRealHermesSmoke -count=1 -v
```

The smoke test logs the exact API Server version, creates a temporary session,
sends a prompt, and attempts to delete the session. Run it only against a
server and model configuration approved for that test.

The implementation is pinned to Hermes Agent API Server `v0.23.3`:
[API Server documentation](https://hermes-agent.nousresearch.com/docs/user-guide/api-server)
and [v0.23.3 source](https://github.com/NousResearch/hermes-agent/tree/v0.23.3).

`GET /api/agent/backend` preserves its existing `name`, `harnesses`, and
`supports_coordinator_model` fields, and adds `available`, `capabilities`, and
`backends`. A missing backend has `available: false` and no capability claims.
Session list and get responses add `available` and, when needed,
`unavailable_reason`. The original session data and `harness` owner remain
unchanged. Missing or non-resumable session owners do not prevent startup:
their stored transcript remains readable and deletable, but sending a message
returns an unavailable error. No session is routed to another backend.

Capabilities describe the registered backend implementation. They do not
guarantee that a remote model provider, external executable, workspace, or
tool service is currently reachable. Registration and tests alone are not
evidence of real-harness compatibility.

## Optional stdio process foundation

`internal/agent/execution` provides an opt-in process runner for backend
adapters. Its zero/default configuration is disabled. Enabling it requires an
absolute executable path and an operator-approved, existing workspace
directory. `RunJSON` sends one JSON object on stdin and accepts one JSON
object on stdout; `RunLines` sends stdin and delivers bounded stdout lines to a
callback. Prompts are not placed in argv. Inherited environment variables are
not passed to the child; callers may explicitly supply selected variables
(Windows also receives `SystemRoot`). Stderr is drained but never returned or
logged.

The runner bounds input and combined stdout/stderr bytes, execution time, and
per-runner concurrency. The line callback supports protocol adapters but does
not itself interpret or guarantee an event protocol. `Close` cancels and joins
in-flight runs.
Missing executables, invalid JSON, limit violations, and process failures are
reported as generic errors without returning child output.
Defaults are two minutes, 1 MiB input, 1 MiB combined output, and one process;
the hard maxima are ten minutes, 8 MiB input, 8 MiB combined output, and 16
concurrent processes. Saturated calls fail immediately instead of building an
unbounded queue. Process-tree shutdown waits up to five seconds and returns an
error if descendants remain. Do not place prompts or credentials in configured
arguments; prompts belong on stdin, and this foundation provides no credential
copying or protected-file provisioning.

The fixed working directory is an operator approval constraint, not a sandbox:
the child runs as the mAIstr0 user and can access resources permitted to that
user. Requested isolation is rejected because this runner does not implement
an OS sandbox. It never installs a harness or provisions protected files.
Protocol adapters must explicitly define their own environment allowlist,
configuration, and protocol handling.

An external `agent.Backend` must own the runner and call `Runner.Close` from
its `Close` method; the orchestrator already closes each registered backend
during server shutdown.

On Unix-like supported systems, the runner starts a process group and kills
that group with `SIGKILL`; descendants that deliberately leave the group may
survive. On Windows, it assigns the process to a Job Object configured to kill
members when the job closes; startup fails if assignment is unavailable.
Other platforms reject enabled execution rather than claim descendant
termination. These mechanisms manage ordinary child trees; they are not
security isolation and cannot guarantee control over deliberately detached
processes on every platform.
