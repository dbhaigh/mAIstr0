# Phase 6: security audit and trust-boundary hardening

## Scope and conclusion

This review covers the current working checkout, including the uncommitted
engineering work from earlier phases. It is a source review with deterministic
negative tests and local process/TLS integration tests, not a penetration-test
certification or a guarantee against every malicious engine or network attacker.
No production model downloads, external exploit targets, or malicious executable
payloads were used.

The highest-impact changes are authenticated first-contact enrollment and
default-off peer executable updates. The former replaces implicit LAN trust;
the latter reduces default authority, but does not add release signatures.
Existing trust-file encodings, API routes, and database buckets are retained.
Intentional request/default changes are documented in the
[migration guide](./pairing-and-security.md#migration),
[OpenAPI](../contracts/openapi.yaml), and
[porting guide](../contracts/PORTING.md#security-migration).

## Findings and disposition

### SEC-01: unauthenticated first contact and PIN-less LAN enrollment

- **Severity: HIGH; confidence: 10/10.** Assets: membership, coordinator identity,
  enrollment PIN, and subsequent control-plane authority.
- **Preconditions and impact:** an attacker on the discovered subnet could
  enroll without a PIN. An attacker able to redirect first-contact traffic could
  present another valid self-signed certificate under the expected public ID;
  checking that ID did not authenticate the server before disclosing the PIN.
- **Fixed:** [bootstrap TLS](../internal/peeridentity/identity.go) requires an
  existing pin or independently verified SHA-256 DER certificate fingerprint.
  [Enrollment](../internal/orchestrator/api_handlers.go) requires a valid one-use
  PIN on every subnet. Invitations verify the destination node before sending
  the PIN and require independent local fingerprint confirmation at the node.
  Discovery values and the offered invitation fingerprint are not auto-trusted.
- **Safe reproductions:** `TestBootstrapRequiresVerifiedFingerprintOrExistingPin`,
  `TestPairingStatusDoesNotOfferPINlessEnrollment`, same-subnet rejection tests,
  invitation rejection/success tests in
  [node tests](../internal/nodeagent/agent_test.go), and the
  [process API test](../internal/integration/process_test.go).
  Rejection leaves trust unchanged; failed confirmation retains the invitation.

### SEC-02: default executable-publisher authority for paired peers

- **Severity: HIGH policy risk; confidence: 10/10.** Assets: application-account
  code execution, files, inherited environment, and credentials.
- **Preconditions and impact:** a compromised paired publisher could offer a
  newer executable and matching metadata. A same-source digest and reported
  version detect neither a malicious publisher nor its malicious binary.
  This was documented authority, not a bypass of mTLS authentication.
- **Mitigated by an explicit policy change:** [startup](../cmd/maistr0/main.go)
  does not start the receiving updater unless `--allow-peer-updates` is set.
  `--no-peer-updates` overrides it. The opt-in warning describes code execution
  as the local account. Publishing endpoints remain compatible.
- **Evidence:** CLI help shows default-off opt-in; the startup gate requires
  both allow and absence of disable. Existing
  [updater tests](../internal/selfupdate/selfupdate_test.go) retain transfer,
  version/platform, idle-admission, rollback, and partial-file checks.
  No malicious executable was executed to reproduce this policy risk.
- **Residual:** opting in still authorizes paired publishers. Independent
  release signing and publisher authorization are not implemented. Deployments
  needing that assurance must leave peer updates disabled.

### SEC-03: enrollment payload certificates were not TLS-channel bound

- **Severity: MEDIUM; confidence: 10/10.** Assets: stored membership pins and
  future peer identity.
- **Preconditions and impact:** a participant able to complete enrollment could
  advertise a certificate different from the TLS key it actually proved.
  Identity-name/validity checks alone did not prove ownership of that key.
- **Fixed:** [shared validation](../internal/peeridentity/trust_boundary.go)
  compares complete DER leaf certificates in both enrollment directions.
  Join rejects missing/mismatched TLS proof before consuming the PIN.
  The coordinator's roster entry cannot subsequently replace that leaf;
  malformed roster certificates are rejected before any trust writes.
- **Safe reproductions:** `TestPairingRecordMustMatchTLSKeyProof`,
  `TestEnrollmentRejectsCertificateSubstitutionWithoutConsumingPIN`, and
  `TestPairingRejectsSubstitutionOversizeAndRedirect` cover key substitution,
  roster substitution, malformed rosters, missing proof, and authorized success.
- **Residual:** multiple disk writes are not a transactional enrollment.
  Persistence failures can still leave partially stored trust; existing
  [durability limitations](./distributed-guarantees.md) remain explicit.

### SEC-04: credentials exposed by diagnostic URLs/configuration

- **Severity: MEDIUM; confidence: 10/10.** Assets: engine credentials, opaque
  credential-bearing query parameters/fragments, and enrollment PINs.
- **Preconditions and impact:** an operator configured credentials in an engine
  URL or PIN-bearing configuration and exposed a properties/config dump or log
  to someone not intended to receive those secrets. Inventory errors could also
  reflect arbitrary upstream content into public engine health.
- **Fixed:** [diagnostic URL redaction](../internal/httpx/redaction.go) removes
  userinfo, all query parameters, and fragments from engine API presentation.
  [Printed configuration](../cmd/maistr0/main.go) redacts PINs, API keys, and
  configured endpoint/registry URLs without changing the executable configuration.
  [Logging](../internal/logging/slog.go) also omits opaque query/fragment values
  from complete HTTP URL arguments. Inventory health exposes generic failures.
- **Safe reproductions:** `TestEngineAPIsRedactCredentialsAndPreserveRoundTrip`,
  `TestPrintNormalizedConfigRedactsPINAndURLCredentials`, and
  `TestStructuredLoggingOmitsOpaqueURLSecrets`.
  Unchanged redacted endpoint saves retain the original engine and credentials.
  Raw URLs remain private execution/cache inputs, not sanitized execution URLs.

### SEC-05: regex-filtered process output could still disclose private content

- **Severity: MEDIUM; confidence: 9/10.** Assets: credentials and prompt/model
  content written to managed-process stdout/stderr.
- **Preconditions and impact:** an authorized engine emitted unlabelled secrets
  or private content not recognized by the credential regexes, and another party
  obtained the application log. Pattern replacement could not guarantee secrecy.
- **Fixed:** [process output](../internal/nodeagent/supervisor/manager_output.go)
  is still drained with bounded line buffering, but logs only byte/truncation
  metadata. Raw lines are not persisted. This intentionally sacrifices detailed
  child-process diagnostics; lifecycle failures and exit/readiness state remain.
- **Safe reproduction:** `TestProcessOutputSummaryOmitsCredentialAndUnlabelledContent`
  checks labelled credentials and arbitrary unlabelled secret/prompt strings.
  Existing process-tree, readiness-timeout, and lifecycle tests retain cleanup
  coverage. This does not sandbox an engine that can independently write files.

### SEC-06: unbounded pairing-response decoding and redirect handling

- **Severity: MEDIUM; confidence: 10/10.** Assets: node memory, enrollment
  availability, and PIN destination.
- **Preconditions and impact:** a reachable first-contact endpoint could return
  an oversized JSON response; bootstrap requests could also be redirected.
  Prior response decoding did not bound the read itself.
- **Fixed:** [pairing client](../internal/nodeagent/orchestrator_pairing.go)
  reads at most the 1 MiB control limit plus one byte, rejects oversized/trailing
  JSON, and refuses redirects. Outgoing invitations refuse redirects as well.
  Rejected payloads do not persist trust.
- **Safe reproduction:** `TestPairingRejectsSubstitutionOversizeAndRedirect`
  uses local TLS/HTTP servers for oversized JSON, trailing JSON, redirects, and
  successful enrollment. The redirect target must never receive a request.

### SEC-07: creation modes did not restrict pre-existing private files

- **Severity: LOW; confidence: 10/10.** Assets: identity private key and logs.
- **Preconditions and impact:** existing identity/log files had been made
  group/world-readable on POSIX. `OpenFile`/`WriteFile` creation permissions do
  not repair permissions of an existing file.
- **Fixed:** [identity persistence](../internal/peeridentity/identity.go)
  restricts the opened file before truncating/writing. The shared
  [log opener](../internal/logging/daily.go) restricts files on initial open,
  rotation, and recovery. Permission/close failures remain explicit.
- **Safe reproductions:** `TestIdentityReopenRestrictsExistingPermissions` and
  `TestDailyFileRestrictsExistingPermissions` reopen temporary 0644 files and
  require 0600, preserving existing log contents.
- **Platform limit:** those POSIX tests skip Windows. Windows chmod is not
  owner-only ACL enforcement; protect data directories and custom paths with
  OS ACLs. Existing configuration permissions are the operator's responsibility.

### SEC-08: remote paired peers could read the active enrollment PIN

- **Severity: LOW; confidence: 10/10.** Assets: authority to enroll new members.
- **Preconditions and impact:** any paired TLS peer could retrieve the active
  PIN from status, extending enrollment authority beyond the local operator.
- **Fixed:** [pairing status](../internal/orchestrator/api_handlers.go) omits PIN
  and expiry on TLS requests; only the protected local plaintext dashboard
  boundary exposes them. The public certificate fingerprint remains available.
- **Safe reproduction:** `TestRemotePairingStatusNeverExposesPIN` checks both
  remote omission and successful local access. Existing local boundary tests
  continue to cover Host/origin/loopback enforcement.

PIN validity was also consolidated into one locked time predicate so verify,
status, and consumption all reject the exact expiration instant.
`TestPairingPINSharedExpirationBoundary` checks before/equal/after without sleeps.
`TestPinnedLeaderReconnectIgnoresPriorBootstrapAnchor` proves that a prior
bootstrap configuration does not block a selected already-pinned leader, while
an explicitly supplied re-pairing fingerprint remains strict.

## Other inspected boundaries and retained assumptions

- **Discovery/election:** LAN announcements are not authenticated consensus.
  Pinned membership and current-coordinator checks protect management writes,
  but spoofing, partitions, stale local views, and denial of availability remain
  possible. See [LAN election](./lan-election.md).
- **TLS/authorization:** ordinary protected control-plane requests check current
  pins; managed-engine lifecycle routes additionally require the current paired
  coordinator. Existing negative/authorized tests cover stale coordinators,
  revoked pins, unpaired peers, TLS 1.3, and connection invalidation.
- **Listener:** plaintext dashboard access is loopback/Host/same-origin fenced.
  It is not isolation between mutually untrusted local OS users; do not expose
  it through an unauthenticated reverse proxy.
- **Downloads/extraction:** worker/model integrations constrain HTTPS destinations,
  redirects, identifiers, size, paths, regular files/aliases, hashes, and
  publication/partial-file cleanup. Existing archive traversal, model-ID traversal,
  redirect, cancellation, and digest tests were retained. Approved publishers,
  model parsers, and executable contents remain supply-chain dependencies;
  same-source checksums are not independent publisher authentication.
- **Processes:** executable/model allowlists and typed argv avoid shell
  interpolation. Windows job objects and POSIX process groups own child cleanup;
  existing process-tree and startup-failure tests exercise those paths.
  Executables inherit the account's environment and privileges, not a sandbox.
- **Transport exceptions:** RPC relay traffic is pinned mTLS; raw worker-side
  GGML RPC is loopback-only. vLLM distributed data traffic is not mTLS and requires
  trusted private interfaces/overlays unless explicitly opted out. External
  inference endpoints use their own configured HTTP/TLS transport.
- **Exhaustion:** route body limits, bounded pools/queues, execution reservations,
  stream cancellation, sync paging/byte limits, and process cleanup remain.
  These are not complete connection/rate/disk quotas or protection against a
  malicious already-authorized executable. See [ownership](./concurrency-ownership.md).

## Reproduction and verification

Run the focused suites without live models:

```powershell
go test ./internal/peeridentity ./internal/nodeagent ./internal/orchestrator ./internal/logging ./internal/nodeagent/supervisor -count=1 -timeout=120s
go test ./cmd/maistr0 ./internal/config ./contracts -count=1 -timeout=120s
node --test web/app_test.js
```

Final Phase 6 results, before the release commit:

- `go build ./...`: passed on Windows.
- `go vet ./...`: passed on Windows.
- `go test -p=1 -count=1 -timeout=120s ./...`: passed on Windows, including the
  real process enrollment/restart integration fixture.
- `node --test web/app_test.js`: passed, 59 tests.
- `.\build\test-race.ps1 -GoTestArgs @('-p=1','-count=1','-timeout=180s','./...')`:
  passed, complete Windows race suite.
- `go test -race -count=1 -p=1 -timeout=180s ./...`:
  passed under Ubuntu WSL, complete Linux race suite using Go 1.27.3.
- `go test -race -v -count=1 ./internal/peeridentity ./internal/logging -run 'TestIdentityReopenRestrictsExistingPermissions|TestDailyFileRestrictsExistingPermissions'`:
  both permission regressions actually executed under Ubuntu WSL and passed,
  not skipped.
- `go generate ./contracts ./internal/scheduler`: passed; regenerated exported
  fixtures have no additional drift. The new
  [security fixture](../contracts/fixtures/security/invitation-requires-fingerprint.json)
  is directly asserted by `TestInvitationFingerprintSecurityContract`.
- `go run .\build\audit-concurrency.go -root . -output .\docs\concurrency-census.json`:
  two final generations were byte-identical: 275 production Go files, 2,486
  syntactic sites, 39 direct goroutine launches, and 29 timer/wait sites.
- `git diff --check`: passed. Security-report/migration local links resolve.

Environment and limitations:

- Windows C compiler DLL shadowing from existing FLM/Multipass PATH entries
  was handled by the existing race helper's scoped MinGW PATH setup. No global
  PATH changes were made. This is a successful race run, not a platform skip.
- The installed esbuild package targets Linux, so native Windows dashboard
  building was unavailable. The existing WSL build succeeded with
  `node scripts/build.mjs` from the web directory under Ubuntu WSL;
  all generated/hash/compression asset checks passed afterward.
- The first broad test run exposed a process fixture using the old unverified
  bootstrap contract. It now obtains the fingerprint from its trusted local
  process dashboard, verifies server TLS with it, and checks certificate binding.
  No insecure production fallback was added.
- Existing CI advisory scanning (`govulncheck`) was not rerun locally. These
  source/test results do not certify dependencies or external publishers.
- No dependencies were added and no commits were made. Existing unrelated
  working-tree changes were preserved.
