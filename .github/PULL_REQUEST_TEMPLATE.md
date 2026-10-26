## Summary

<!-- Describe the change and why it is needed. -->

## Compatibility and risk

- [ ] Existing routes, JSON formats, config keys, and persisted formats remain compatible, or changes are documented and versioned.
- [ ] Prompts, model outputs, credentials, pairing PINs, and private keys are not logged or included in examples.
- [ ] User-facing documentation and `changelog.md` are updated when appropriate.
- [ ] Added or updated dependency licenses (including transitive dependencies) were reviewed; explain any redistribution obligations.

## Validation

- [ ] `go test ./...`
- [ ] `go vet ./...`
- [ ] `go build ./...`
- [ ] `node --test web/app_test.js`
- [ ] `go test -race ./...` on a supported Linux environment when changing concurrent code

<!-- List any checks that could not be run and why. -->
