# Contributing

Thanks for helping improve mAIstr0. Start with [AGENTS.md](./AGENTS.md) for
repository architecture, compatibility rules, lock ordering, and implementation
conventions. See [Development](./docs/development.md) for supported toolchains,
checks, fixtures, and release builds.

## Changes

- Keep changes small and focused. Preserve existing routes, methods, status
  codes, JSON fields, configuration keys/defaults, and bbolt formats unless an
  intentional compatibility change is approved and documented.
- Add regression tests alongside behavior changes. Prefer deterministic
  simulated engines, `httptest`, and isolated temporary stores.
- Keep prompts, model outputs, API keys, pairing PINs, authentication material,
  and private keys out of logs and examples.
- Update the OpenAPI specification, schemas, fixtures, and
  [porting guide](./contracts/PORTING.md) when a wire contract changes.
- Update user-facing docs and `changelog.md` for user-visible or configuration
  changes. Avoid regenerating goldens simply to make a failing test pass.

## Pull requests

Use the [pull-request template](./.github/PULL_REQUEST_TEMPLATE.md). Explain
the motivation, behavior and compatibility impact, tests run, and any
limitations. Preserve unrelated working-tree changes and do not include
secrets or personal absolute paths.

Before requesting review, run the applicable checks from
[Development](./docs/development.md). If a check cannot run on your platform,
report the limitation rather than marking it as passed.
