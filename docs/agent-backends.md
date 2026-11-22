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

`internal/agent/execution` provides an opt-in one-request process runner for
future backend adapters. Its zero/default configuration is disabled. Enabling
it requires an absolute executable path and an operator-approved, existing
workspace directory. The executable receives configured arguments as an argv
vector, one JSON object on stdin, and must return one JSON object on stdout.
Prompts are not placed in argv; inherited environment variables are not passed
to the child (Windows receives only `SystemRoot`). Stderr is drained but never
returned or logged.

The runner bounds input and combined stdout/stderr bytes, execution time, and
per-runner concurrency. It accepts one object response, not an event stream, so
streaming must remain unadvertised. `Close` cancels and joins in-flight runs.
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
an OS sandbox. It never installs a harness, copies credentials, or configures
MCP servers or hooks. No protocol-specific harness adapter or HTTP server
transport is included.

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
