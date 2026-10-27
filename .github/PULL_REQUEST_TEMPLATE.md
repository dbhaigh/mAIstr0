## Summary

<!-- Describe the change and why it is needed. -->

## Compatibility and risk

- [ ] Existing routes, JSON field names, config keys, and bbolt bucket names/key formats remain unchanged; compatibility changes are explicitly authorized and additive.
- [ ] No dependency was added without approval; licenses were reviewed for any approved dependency.
- [ ] Raw prompts, secrets, PINs, and keys are not logged or included in examples.
- [ ] User-visible changes update `README.md` or relevant docs and `changelog.md`.
- [ ] Optimizations include representative before/after benchmark or pprof evidence and `benchstat` output; removed code has `deadcode` or repository/reference evidence.
- [ ] Mutex-protected state and lock order are documented; owned goroutines are canceled and joined on shutdown, with exceptions documented.

## Validation

- [ ] `go build ./...`
- [ ] `go vet ./...`
- [ ] `go test ./...`
- [ ] `node --test web/app_test.js`
- [ ] `go test -race ./...` on Linux where available; document platform limitations.

<!-- List any checks that could not be run and why. -->
