# Coding-client model endpoint compatibility

This matrix describes the interfaces implemented by mAIstr0, not certification
of these clients as autonomous agents. A text response from a client is not
evidence that its tool loop, output constraints, or cancellation behavior works.

## Interface boundary

There are two different kinds of endpoint:

- The direct compatibility endpoints translate a limited request into
  mAIstr0's text-generation request. They return text, not model-native tool
  calls or schema-constrained output. Unsupported JSON fields are rejected
  instead of being silently discarded.
- The `/router/` endpoints select a model/node and forward the request body and
  response stream using the protocol path. mAIstr0 adds routing headers and
  applies routing, admission, and retry behavior; the selected node's engine
  must implement the requested protocol and features.

| Direct endpoint | Behavior |
| --- | --- |
| `GET /v1/models` | OpenAI-style model list (`data` entries with `id`, `object`, `created`, and `owned_by`). |
| `POST /v1/chat/completions` | Translates text messages to generation; text SSE is available when `stream: true`. |
| `POST /v1/completions` | Translates a prompt to generation; non-streaming only. |
| `POST /v1/messages` | Translates text-only Anthropic Messages requests; text SSE is available when `stream: true`. |
| `GET /api/tags` | Ollama-style model list. |
| `POST /api/chat`, `POST /api/generate` | Translate supported Ollama text requests; chat/generate streaming uses NDJSON. |
| `POST /router/v1/chat/completions`, `POST /router/v1/completions`, `POST /router/v1/messages`, `POST /router/api/chat`, `POST /router/api/generate` | Route and forward the corresponding protocol request to a node. These paths do not add support that the selected engine lacks. |

`/v1/responses` and Gemini's `generateContent` protocol are not implemented.
The direct translation paths accept a deliberately small set of fields; this
is not a promise that every field in the corresponding vendor API is supported.

## Client matrix

“Client version checked” is the latest stable release found in the official
release source on **2026-11-21**. It is a research snapshot, not an mAIstr0
tested-version range or compatibility guarantee.

| Client (version checked) | Model discovery and exact endpoint/protocol | Authentication | Streaming | Tool calls and results | Structured output | Cancellation | Known limits / who needs the feature |
| --- | --- | --- | --- | --- | --- | --- | --- |
| [Claude Code v2.1.330](https://github.com/anthropics/claude-code/releases/tag/v2.1.330) | Uses Anthropic Messages at `POST /v1/messages` when configured with `ANTHROPIC_BASE_URL`. mAIstr0's path is a **translated** text-only endpoint. Optional gateway discovery calls `GET /v1/models`; Claude Code expects Anthropic model entries (including `id` and optional `display_name`) in `data`. mAIstr0 returns its OpenAI-style entry shape, so use a configured model ID rather than relying on this picker. | Claude Code expects `ANTHROPIC_AUTH_TOKEN` or `ANTHROPIC_API_KEY`. The direct mAIstr0 handler does not validate these values. The actual service listener allows loopback HTTP; non-loopback HTTPS requests require a trusted paired mTLS identity. | Basic text SSE is translated to Anthropic text events. This does not cover the full Messages event/capability set (for example, tool-use blocks, usage fidelity, and all current beta fields). | **No on the translated path.** Claude Code needs tool definitions, tool-use blocks, and tool results to perform agent work; mAIstr0 rejects unsupported request fields/content instead of pretending to execute tools. `/router/v1/messages` forwards them only if the selected node engine supports them. | **No on the translated path.** `output_config` and other unsupported fields are rejected. Router forwarding is engine-dependent. | Request cancellation follows the HTTP request context; there is no separate per-inference cancel endpoint. Claude Code's agent loop needs tools and capabilities beyond the text translation. |
| [Codex CLI rust-v0.171.0](https://github.com/openai/codex/releases/tag/rust-v0.171.0) | Current Codex configuration documents custom providers and the Responses wire API. mAIstr0 has no `POST /v1/responses`; its `POST /v1/chat/completions` endpoint is a **translated** Chat Completions endpoint, not a Responses implementation. `GET /v1/models` is OpenAI-style; configure an explicit model/provider ID rather than assuming Codex will discover it. | Codex's provider config can obtain a key from its configured environment variable. The direct mAIstr0 handler does not validate a key. For remote listener access, mAIstr0's paired mTLS requirement still applies. | Chat Completions text SSE exists. It is not the Responses event protocol expected by the documented Responses provider. | **No on the translated path.** Codex needs tool/function interactions for agent execution. `/router/v1/chat/completions` only works when the engine can handle the exact request and response protocol. | **No on the translated path.** Unsupported request fields are rejected. Router behavior depends on the engine. | Request cancellation is propagated with the HTTP request context; no separate cancel route. | **Not compatible with Codex's documented Responses wire API.** Do not infer agent support from the OpenAI-compatible path name. A text-only result does not validate Codex's tools or Responses behavior. |
| [OpenCode v1.19.6](https://github.com/anomalyco/opencode/releases/tag/v1.19.6) | Its official custom-provider setup supports an OpenAI-compatible provider with a base URL and explicitly configured model map. Point that provider at `/v1`; its Chat Completions call reaches mAIstr0's **translated** `POST /v1/chat/completions`. `GET /v1/models` is available, but explicitly declaring the model follows the documented custom-provider configuration. | OpenCode can provide a configured provider credential/header. The direct mAIstr0 path does not validate that credential; don't use a production provider key. | Text SSE is available for Chat Completions. | **No on the translated path.** OpenCode's agent requires model tool calls and returned tool results. The router path is usable only with a node engine that implements those features. | **No on the translated path.** For router forwarding, check the selected engine's support. | HTTP request cancellation is propagated; no standalone inference-cancel API. | The translation does not implement tool-use, reasoning fields, image input, prompt-cache controls, or other provider-specific options. OpenCode needs tools to perform coding actions. |
| [Gemini CLI v0.70.0](https://github.com/google-gemini/gemini-cli/releases/tag/v0.70.0) | The native Gemini API uses `POST /v1beta/models/{model}:generateContent` and `POST /v1beta/models/{model}:streamGenerateContent`; the CLI also documents Google sign-in and Vertex AI authentication. mAIstr0 exposes neither Gemini endpoint nor Gemini model discovery. | Gemini CLI's Google credentials authenticate with Google's API; mAIstr0 does not accept Gemini credentials as a provider connection. Do not send Google credentials to mAIstr0. | No Gemini protocol stream endpoint. | No Gemini protocol tool calls/results endpoint. | No Gemini protocol structured-output endpoint. | No Gemini protocol request to cancel. | **Unsupported as a native Gemini CLI model endpoint.** Do not treat Gemini CLI's use inside another application (such as an ACP integration) as compatibility with mAIstr0's model API. |
| [OpenHands v1.43.0](https://github.com/OpenHands/OpenHands/releases/tag/v1.43.0) | OpenHands documents a custom model/base URL for OpenAI-compatible local endpoints. Its model server URL can target `/v1`; calls to `POST /v1/chat/completions` use mAIstr0's **translated** endpoint. Configure the model ID explicitly. | OpenHands has an API-key field; mAIstr0 does not validate it on the direct compatibility path. Use only a non-secret placeholder for a loopback test. | Text SSE is available. | **No on the translated path.** OpenHands needs function/tool calls and results for agent actions. `/router/v1/chat/completions` passes through only to an engine with the required support. | **No on the translated path.** Router forwarding is engine-dependent. | Request cancellation follows the HTTP request context; there is no separate per-inference cancel endpoint. | A text-only response may make the configured model appear reachable while the OpenHands agent cannot use tools. OpenHands' model documentation warns that local model capability varies. |
| [Hermes Agent v0.23.3](https://github.com/NousResearch/hermes-agent/releases/tag/v0.23.3) | Official Hermes docs describe custom endpoints and OpenAI-compatible providers. Configure the custom provider for `chat_completions` with a `/v1` base URL; its request reaches mAIstr0's **translated** `POST /v1/chat/completions`. Hermes model discovery is enabled by default for custom providers and can be disabled; explicitly configure the model ID if discovery is unsuitable. | Hermes' custom provider can be configured with a key. The direct mAIstr0 endpoint does not validate it; use a non-secret placeholder on loopback. | Text SSE is available for Chat Completions. | **No on the translated path.** Hermes' main agent uses iterative tool calls/results. The router can forward them only if the engine implements the same protocol and feature set. | **No on the translated path.** Router support depends on the selected engine. | Request cancellation follows the HTTP request context; no separate inference cancel endpoint. | Hermes may also issue auxiliary model requests. The text translator does not establish support for tool execution, vision, or other auxiliary model capabilities. |

## Authentication and exposure

The direct compatibility handlers do not authenticate an OpenAI, Anthropic, or
Ollama API key. A client may require a credential-shaped setting, but that
setting is not an mAIstr0 access-control boundary. On the actual server,
loopback HTTP is loopback-only; the remote HTTPS listener requires a trusted
paired mTLS peer. The `/router/` path forwards request headers, including
`Authorization`, to the selected node. Do not put a real third-party credential
in a test request or assume a bearer header protects the direct endpoint.

Keep the listener on loopback for local-client tests. For remote access, use
the repository's paired mTLS model and do not bypass it with an unauthenticated
public proxy. See [API and network guidance](./api.md) and
[pairing and security](./pairing-and-security.md).

## Opt-in real-client verification

This procedure is intentionally manual and makes no hardware or real-client
support claim. Use a disposable mAIstr0 instance and a client from the exact
release snapshot above (or record the different exact version). Before each
test:

1. Keep mAIstr0 bound to loopback. Use an empty scratch directory/repository
   and synthetic prompts only; do not expose private source, prompts, model
   output, or secrets to logs or third parties.
2. Read `GET http://127.0.0.1:<port>/v1/models` and select an exact `id` that
   appears in that response and is backed by a working local/test model. Do not
   substitute a guessed model name. Gemini CLI has no matching Gemini
   endpoint here, and current Codex's documented Responses endpoint is absent;
   record these as unsupported rather than changing protocols speculatively.
3. Use a throwaway, non-secret placeholder only if the client refuses to start
   without an API-key field. Direct compatibility endpoints do not validate
   that value. Do not use an Anthropic, OpenAI, Google, OpenHands, or Hermes
   production credential. A `/router/` test forwards headers to the node, so
   leave real credentials unset there too.
4. Record separate outcomes for model selection/discovery, a plain text turn,
   streamed text, a tool call plus tool result, structured output, and
   cancellation. A successful plain text request is only a text-endpoint
   result; it is not proof of agent compatibility. The translated endpoints
   should return an explicit unsupported-field error for features they do not
   implement. Router results must be attributed to the actual selected engine.
5. Cancel an in-flight request using the client's normal interrupt/disconnect
   control and verify the request stops. There is no separate mAIstr0 inference
   cancellation endpoint. Avoid testing actions that would modify the scratch
   environment until tool forwarding has been independently verified.

## Official documentation and release sources checked

- Claude Code: [gateway protocol and model discovery](https://code.claude.com/docs/en/llm-gateway-protocol), [gateway setup](https://code.claude.com/docs/en/llm-gateway-connect), and [v2.1.330 release](https://github.com/anthropics/claude-code/releases/tag/v2.1.330).
- Codex: [advanced configuration](https://developers.openai.com/codex/config-advanced) and [rust-v0.171.0 release](https://github.com/openai/codex/releases/tag/rust-v0.171.0).
- OpenCode: [providers](https://opencode.ai/docs/providers/), [models](https://opencode.ai/docs/models/), and [v1.19.6 release](https://github.com/anomalyco/opencode/releases/tag/v1.19.6).
- Gemini CLI: [authentication and provider choices](https://geminicli.com/docs/get-started/authentication/), [Gemini API generation endpoints](https://ai.google.dev/api/generate-content), and [v0.70.0 release](https://github.com/google-gemini/gemini-cli/releases/tag/v0.70.0).
- OpenHands: [LLM setup](https://docs.openhands.dev/openhands/usage/llms/), [local OpenAI-compatible endpoints](https://docs.openhands.dev/openhands/usage/llms/local-llms), and [v1.43.0 release](https://github.com/OpenHands/OpenHands/releases/tag/v1.43.0).
- Hermes Agent: [provider integrations](https://hermes-agent.nousresearch.com/docs/integrations/providers), [model configuration](https://hermes-agent.nousresearch.com/docs/user-guide/configuring-models), and [v0.23.3 release](https://github.com/NousResearch/hermes-agent/releases/tag/v0.23.3).
