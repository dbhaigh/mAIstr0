# Harness integration qualification

This procedure qualifies two distinct paths for Claude Code, Codex CLI,
OpenCode, Gemini CLI, OpenHands, and Hermes Agent:

1. **Managed agent:** mAIstr0 invokes or connects to the harness as an agent
   backend.
2. **Model-endpoint client:** the harness uses mAIstr0 as its model provider.

A result applies only to the recorded operating system, exact harness/server
version, interface, model, authentication, and isolation setup. It does not
establish support for other versions, platforms, models, or hardware.

## Status definitions

- **Supported:** deterministic contract checks and the applicable real
  qualification steps passed for the exact recorded configuration.
- **Experimental:** the adapter has deterministic fake-process/server coverage,
  but the real configuration has not completed qualification.
- **Untested:** there is no end-to-end result for that integration path.
  Documentation or protocol inspection is not a test result.
- **Unsupported:** a required protocol, feature, or policy is absent on the
  specified path. This is path-specific, not a claim about every way to
  integrate the products.

Do not promote a row to Supported based on fakes, startup/version checks, a
plain text response, or a smoke test that did not exercise the row's listed
capabilities. Keep failed and blocked attempts as evidence; do not erase or
reinterpret them as passes.

## Current evidence register

No credentialed real-harness or real-client qualification result is recorded
in this repository. The managed-agent rows are Experimental because fake
contract tests exist; their opt-in smoke tests have not been run as part of
this qualification. All exact runtime OS, observed version, model, and
authentication values therefore remain **not run / not recorded**. Version
targets below are implementation requirements, not observed test versions. The
shared fake-suite evidence is `go test ./internal/agent/... -count=1`: **PASS**
on Windows 10.0.26300.0 x64 in this worktree. This result covers adapter
contracts only, not a real harness or model provider.

| Direction / harness | Status | OS; target version; interface | Model and authentication | Isolation | Streaming; tool use; cancellation; resume | Evidence and result |
| --- | --- | --- | --- | --- | --- | --- |
| Managed / Claude Code | Experimental | Real harness not run; `>=2.1.286, <2.2.0`; headless CLI `stream-json` | Anthropic model configured in Claude Code; local `ANTHROPIC_API_KEY` | No OS sandbox; built-in tools and MCP disabled | Text streaming; no tools; owned process-tree termination, not CLI acknowledgment; native session resume | Fake tests PASS: stream/resume, failure redaction, cancellation, unsupported tools. Real smoke exists; not run. |
| Managed / Codex CLI | Experimental | Real harness not run; `>=0.171.0, <0.172.0`; `codex exec --json` | Codex-selected model; local Codex authentication in `CODEX_HOME` | Codex read-only sandbox; no approval prompts; no write access | JSONL streaming; read-only agent tools; owned process cancellation; native thread resume | Fake tests PASS: fragmented JSONL, early/terminal failure, cancellation, resume, fail-closed policy. Real smoke exists; not run. |
| Managed / OpenCode | Experimental | Real harness not run; server API `>=2.3.2, <2.4.0`; owned or configured OpenCode server API | Model/provider configured on that server; server Basic authentication | Workspace-scoped permissions, not an OS sandbox | Final response only; workspace read/edit tools; cancellation requires server confirmation; native session mapping and reconciliation | Fake tests PASS: permissions, disconnect, unavailable server, restart reconciliation, cancellation confirmation. Real smoke exists; not run. |
| Managed / Gemini CLI | Experimental | Real harness not run; `>=0.70.0, <0.71.0`; headless `stream-json` | Google AI Studio or Vertex model; explicit API-key or service-account mode | Explicit Gemini CLI sandbox provider; not independently qualified on a real host | Structured streaming and CLI tool events; process-tree cancellation without sandbox acknowledgment; native session resume | Fake tests PASS: event mapping, malformed/failure cases, policy/auth validation, cancellation, resume. Real smoke exists; not run. |
| Managed / OpenHands | Experimental | Real harness not run; Agent Server `1.63.1`; authenticated session WebSocket/API | Model/provider configured on server; session API key | `LocalWorkspace` selection is not an OS boundary | Durable event streaming; Terminal/FileEditor tools; server-confirmed cancellation; native conversation resume/replay | Fake HTTP/WebSocket tests PASS: authentication, workspace, replay, pending work, cancellation, malformed/redacted output. Real smoke exists; not run. |
| Managed / Hermes Agent | Experimental | Real harness not run; API Server `0.23.3`; durable Runs API and SSE | Model/provider configured on gateway; bearer API key | Gateway/operator-controlled; no isolation claim | SSE run events; tools only as explicitly configured by gateway operator; terminal cancellation acknowledgment; gateway-owned run/session resume | Fake API tests PASS: authentication, replay, disconnect, restart, duplicate prevention, cancellation, shutdown. Real smoke exists; not run. |
| Model endpoint / Claude Code | Untested text path; unsupported for agent tool loop | Not run; research snapshot `2.1.330`; Anthropic Messages `POST /v1/messages` translation | No model tested; use an explicit returned model ID; only a non-secret placeholder if the client requires a key | Loopback-only test procedure; no harness workspace qualification | Text SSE is implemented, client interop untested; tool-use blocks unsupported; request-context cancellation not client-qualified; resume N/A | Protocol/API review only; no real client result. |
| Model endpoint / Codex CLI | Unsupported on documented Responses path | Not run; research snapshot `rust-v0.171.0`; mAIstr0 does not implement `POST /v1/responses` | No model/auth tested; Codex provider key configuration does not add the missing protocol | No client test performed | Chat Completions text SSE is not the documented Responses stream; tools/results unsupported; cancellation untested; resume N/A | Protocol mismatch documented; no real client result. |
| Model endpoint / OpenCode | Untested text path; unsupported for agent tool loop | Not run; research snapshot `1.19.6`; custom OpenAI provider using translated `POST /v1/chat/completions` | No model tested; configure an explicit model; use a non-secret placeholder | Loopback-only test procedure; no harness workspace qualification | Text SSE is implemented, client interop untested; tools/results unsupported by the translation; request-context cancellation untested; resume N/A | Documented provider setup and API review only; no real client result. |
| Model endpoint / Gemini CLI | Unsupported as a native Gemini endpoint | Not run; research snapshot `0.70.0`; native Gemini `generateContent` protocol is absent | Google credentials are not mAIstr0 provider credentials; no model/auth test | No client test performed | No Gemini-native stream, tool, or structured-output endpoint; cancellation untested; resume N/A | Protocol mismatch documented; no real client result. |
| Model endpoint / OpenHands | Untested text path; unsupported for agent tool loop | Not run; research snapshot `1.43.0`; custom OpenAI provider using translated `POST /v1/chat/completions` | No model tested; configure an explicit model; use a non-secret placeholder | Loopback-only test procedure; no harness workspace qualification | Text SSE is implemented, client interop untested; tools/results unsupported by the translation; request-context cancellation untested; resume N/A | Documented provider setup and API review only; no real client result. |
| Model endpoint / Hermes Agent | Untested text path; unsupported for agent tool loop | Not run; research snapshot `0.23.3`; custom `chat_completions` provider using translated `POST /v1/chat/completions` | No model tested; configure an explicit model; use a non-secret placeholder | Loopback-only test procedure; no harness workspace qualification | Text SSE is implemented, client interop untested; tools/results unsupported by the translation; request-context cancellation untested; resume N/A | Documented provider setup and API review only; no real client result. |

The client release numbers are research snapshots in
[client compatibility](./client-compatibility.md), not tested-version ranges.
The managed-agent version requirements and behavior are documented in
[agent backends](./agent-backends.md). Neither register implies real model,
hardware, or platform support.

## Repeatable procedure

### 1. Deterministic, secret-free baseline

Run from the repository root with real-smoke opt-in variables unset. The test
packages use fakes and do not require provider credentials:

```sh
go test ./internal/agent/... -count=1
```

In CI, do not set any `MAISTR0_*_REAL_SMOKE` variable. If running locally in a
shell that may inherit them, unset the six flags first.

PowerShell:

```powershell
$flags = @(
  "MAISTR0_CLAUDE_CODE_REAL_SMOKE", "MAISTR0_CODEX_REAL_SMOKE",
  "MAISTR0_OPENCODE_REAL_SMOKE", "MAISTR0_GEMINI_REAL_SMOKE",
  "MAISTR0_OPENHANDS_REAL_SMOKE", "MAISTR0_HERMES_REAL_SMOKE"
)
$saved = @{}
foreach ($flag in $flags) {
  $saved[$flag] = [Environment]::GetEnvironmentVariable($flag, "Process")
  [Environment]::SetEnvironmentVariable($flag, $null, "Process")
}
try {
  go test ./internal/agent/... -count=1
} finally {
  foreach ($flag in $flags) {
    [Environment]::SetEnvironmentVariable($flag, $saved[$flag], "Process")
  }
}
```

bash/zsh:

```sh
env -u MAISTR0_CLAUDE_CODE_REAL_SMOKE \
  -u MAISTR0_CODEX_REAL_SMOKE \
  -u MAISTR0_OPENCODE_REAL_SMOKE \
  -u MAISTR0_GEMINI_REAL_SMOKE \
  -u MAISTR0_OPENHANDS_REAL_SMOKE \
  -u MAISTR0_HERMES_REAL_SMOKE \
  go test ./internal/agent/... -count=1
```

Record the command and result. This baseline qualifies only the deterministic
adapter contracts, not an installed harness.

### 2. Managed-agent real qualification

Run each opt-in smoke test separately using the configuration and command in
the corresponding section of [agent backends](./agent-backends.md). The smoke
test must print or otherwise safely record the exact CLI/server version it
checked. Do not put credentials in command-line arguments, test output, or
evidence. Use a disposable service account/home and provider configuration;
credentialed or paid calls are opt-in and are never part of default CI.

For each exact OS/version/configuration:

1. Confirm startup rejects an unsupported harness/server version, absent
   required authentication, and unavailable or policy-incompatible server
   before submitting a prompt.
2. Confirm one synthetic plain-text turn, event ordering, terminal result,
   failure handling, and resume where the adapter declares resume. Record
   streaming as unsupported where the adapter returns only a final result.
3. **Safe workspace-edit smoke:** only run a write case for a backend that
   explicitly exposes edit tools. Use a newly created disposable workspace
   inside an OS-level VM/container or equivalently restricted account, with no
   repository or personal files mounted. Ask the harness to create one
   uniquely named text file with a fixed harmless marker, then verify only
   that file changed and remove the disposable environment. Do not infer
   isolation from a workspace path or application permission rule. Claude
   Code and Codex are configured without write tools; their expected result
   is no workspace change. Do not broaden their policies to make this step
   pass. Gemini must use its selected functioning sandbox. OpenCode,
   OpenHands, and Hermes require independently verified OS/deployment
   containment; their adapter workspace/gateway policy is not a sandbox.
4. **Negative authorization:** for remote servers, repeat startup or a
   harmless read with a missing/invalid server credential and verify rejection
   before prompt admission. Verify the server received no prompt. For provider
   authentication, use a test endpoint or a deliberately invalid test
   credential only; verify a generic failure and no credential in logs. Never
   use a production credential to test rejection.
5. **Unknown-outcome recovery:** use only a disposable session and a harmless,
   idempotent turn. After server acceptance, interrupt the client connection
   before the terminal event. Reconnect or restart mAIstr0 and verify it
   reconciles the same native session/run before accepting another turn. Verify
   that it does not resubmit the original prompt. Where the harness is a local
   child process and cannot distinguish termination from completion, record
   the outcome as unknown unless a terminal event or process termination is
   confirmed. Never retry by creating a second session to conceal ambiguity.
6. Record cancellation separately: a request being accepted is not proof that
   work stopped. Mark pass only after the backend's documented confirmation
   boundary is observed; otherwise record unknown/failed and retain the
   existing session for reconciliation.

The current fake tests include targeted authorization, safe policy, and
unknown-outcome cases for the server backends. Important examples are
`TestOpenCodeRestartReconcilesAcceptedTurnBeforeNewPrompt`,
`TestOpenHandsRestoredPendingTurnContinuesWithoutResubmission`,
`TestHermesUnconfirmedCancellationAndGatewayRestartRemainUnknown`,
`TestOpenHandsAuthenticationMustBeEnforcedAndWorkspaceMustMatch`, and
`TestHermesRequiresPinnedAuthenticatedDurableRunsAPI`. These are deterministic
contract evidence, not real-server results. Claude Code, Codex, and Gemini
tests cover their local-process cancellation and native resume behavior; they
do not establish remote-work continuation semantics.

### 3. Model-endpoint-client qualification

Only test provider configurations whose documented wire protocol is exposed
by mAIstr0. Keep mAIstr0 bound to loopback, use a disposable model instance,
and select an exact ID from `GET /v1/models`. Use a non-secret placeholder
only when the client requires an API-key field. Do not send third-party
credentials to mAIstr0.

Record plain text, streaming, tool-call/result, structured-output, and
cancellation outcomes separately. A text response or SSE text event is not a
pass for tool use. The direct compatibility endpoints translate limited text
requests and do not implement model-native tool calls; the `/router/` routes
depend on the selected engine and must be recorded as a separate configuration.
Codex's documented Responses path and Gemini's native API are absent and must
remain Unsupported unless those protocols are intentionally implemented and
qualified.

The direct compatibility routes do **not** validate bearer/API-key values.
Therefore an invalid bearer on loopback is not a meaningful negative
authorization test. For a remote-client security check, verify that an
unpaired client without a trusted mTLS identity cannot reach the HTTPS
listener and that no request is forwarded. Do not expose an unauthenticated
listener or proxy to perform this test.

### 4. Evidence record

Keep one record for each direction × harness × OS × exact version × model
configuration. Store sanitized output or an artifact reference; do not store
prompts, credentials, raw tool output, private paths, or unredacted session
IDs. For each item use `pass`, `fail`, `not run`, `not applicable`, or
`unknown`; explain failures without copying sensitive output.

```text
Date:
Direction: managed agent | model-endpoint client
Harness and exact version:
Operating system and version:
Interface and configuration mode:
Model and provider:
Authentication mode (credential value omitted):
Workspace and isolation boundary:
Streaming:
Tool use:
Negative authorization:
Cancellation and confirmation:
Resume / unknown-outcome recovery:
Safe workspace edit (or expected no-write result):
Pass/fail evidence reference:
Limitations:
```

## Platform and hardware limits

No cross-platform equivalence is implied. Record the OS build, architecture,
shell, sandbox provider/version, container runtime, and server deployment for
each real run. Gemini sandbox providers are platform-specific; OpenHands and
Hermes depend on their separately deployed servers; OpenCode server behavior
depends on the server host; Claude Code and Codex depend on local CLI/runtime
support. No GPU, accelerator, model-quality, throughput, or hardware claim is
part of this procedure.
