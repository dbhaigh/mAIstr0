# Security model

mAIstr0 is local-first. Inference and orchestration run on configured local
engines and cluster nodes; the application does not require a cloud service.
Cluster peers authenticate each other with self-signed certificates whose
SHA-256 fingerprints are pinned during pairing. Remote cluster traffic requires
mutual TLS 1.3 and a currently valid, explicitly trusted peer certificate.
The trust roster is stored with the node identity using owner-only file
permissions.

## Pairing

Pairing PINs are six digits, expire after 30 minutes, can be used once, and are
invalidated after 20 failed attempts. Comparisons use a constant-time
comparison. PINs are communicated out of band; attempt and outcome logs do
not include PIN values. The pairing status and PIN regeneration endpoints
intentionally return the active PIN to the local operator.

Nodes on the same discovered subnet may use the existing automatic-join path
without a PIN. This is a deliberate trust boundary: only enable LAN discovery
on networks where devices are trusted. Cross-subnet pairing requires the
out-of-band PIN.

## Certificates and pin changes

Peer-facing TLS listeners and consensus transports require TLS 1.3 or later.
Identity certificates are checked for their validity period when loaded and
during TLS handshakes. An expired local identity certificate is rotated
automatically while preserving the stable node ID and trusted peer roster.
Because peer trust is fingerprint-pinned, a rotated certificate is not
implicitly trusted by existing peers; the affected peer relationship must be
paired again using the normal authenticated pairing flow.

Changing or removing a peer pin closes the identity's pooled connections so
that requests cannot continue over a connection authenticated with the old
pin. If a peer's certificate changes unexpectedly, the connection is rejected;
verify the peer out of band and re-pair it rather than disabling certificate
checks.

## Local dashboard

Plain HTTP is accepted only from loopback clients and only when the HTTP Host
is `localhost` or a loopback IP address. Browser requests must be same-origin;
cross-site fetch metadata, mismatched Origin headers, and mismatched Referer
headers are rejected. Non-browser local API clients without browser-origin
metadata remain supported. Remote clients must use paired mutual TLS.

The local dashboard is not an authentication boundary between mutually
untrusted local users. Protect access to the operating-system account and
avoid exposing the loopback service through an unauthenticated reverse proxy.

## HTTP request limits and logging

The HTTP servers bound header-read and idle-connection time. JSON request
bodies use route-appropriate limits: 64 KiB for small control records, 1 MiB
for control requests, 8 MiB for node-status reports, and 20 MiB for inference
payloads. Existing stage, synchronization, compatibility, and router-specific
limits remain in effect; the server also enforces a 256 MiB absolute request
body ceiling.

Default logs and the SSE cluster snapshot do not include prompts, model
outputs, pairing PINs, API keys, authentication material, or private keys.
Pairing errors returned by a peer are not copied into local errors or logs.
The `govulncheck` scan and `go vet` run in CI.
