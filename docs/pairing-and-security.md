# Pairing and security

mAIstr0 is local-first. Inference and orchestration run on configured engines
and cluster nodes; a cloud service is not required. Cluster peers authenticate
with self-signed certificates whose SHA-256 fingerprints are pinned during
pairing. Normal HTTP control-plane and RPC-relay traffic requires mutual TLS 1.3 or later and a
currently valid, explicitly trusted peer certificate. The trust roster is
stored with the node identity. POSIX identity and log files are restricted to
mode 0600, including existing files reopened by the application. On Windows
startup checks the identity file, current log, and retained log archives for
allow entries granted to broad principals. It warns but does not change ACLs.
Review the file and parent-directory Security settings; remove broad grants
such as Everyone, Authenticated Users, Users, Guests, Interactive, Network,
and Anonymous, while retaining the current service/user account and SYSTEM.
For a service installation, preserve its service account instead of assuming
the interactive user is the owner. Apply the same review to custom identity,
data, and log paths.

## Pairing a node

1. The orchestrator's **Pairing access** panel displays the active one-time PIN
   and certificate SHA-256 fingerprint. The fingerprint is 64 hexadecimal
   characters of the complete DER-encoded leaf certificate, not the public key
   alone.
2. With the default policy, a discovered node on a prefix assigned to one of
   the orchestrator's local interfaces can join automatically without the
   fingerprint or PIN. The node resolves advertised hostnames when checking
   its local interface prefixes. The coordinator independently checks the
   actual accepted connection's local and remote addresses against its
   interface prefixes; discovery and DNS alone never authorize membership.
   After restart, a node with an already trusted orchestrator retries
   registration in the background without opening a new-join prompt. The
   dashboard waits for a discovered candidate before prompting for an
   untrusted pairing.
3. For connections outside those prefixes, or whenever strict verification is
   enabled, enter the orchestrator's HTTPS address, out-of-band verified
   fingerprint, and PIN in the node's **Setup** panel. For unattended
   enrollment, provide the PIN and fingerprint through protected node
   configuration.

PINs are six digits, expire after 30 minutes, can be used once, and are
invalidated after 20 failed attempts. Comparisons use a constant-time
comparison. Attempt and outcome logs omit PIN values. The local operator can
view the active PIN or generate a replacement from the Setup panel. A PIN
generated there remains visible across dashboard status refreshes without
exposing it on remote TLS status requests.

The **Require the out-of-band fingerprint and one-use PIN** control in
**Pairing access** is a cluster-wide strict-mode setting. The active coordinator
enforces changes immediately and propagates them to joined nodes during their
regular peer refresh. Strict mode applies to new joins and re-pairs; it does not
revoke or rewrite existing trust. With strict mode off, subnet-based auto-pair
uses the local interface prefix and the discovered orchestrator identity as the
trust boundary. First-contact TLS outside that path still requires the
independently verified fingerprint. Existing pins reconnect without supplying
it again. Orchestrator-to-orchestrator pairing is disabled. Remote pairing
status never discloses the active PIN.

Discovery remains unauthenticated by default and can optionally authenticate
announcements with a shared cluster HMAC key (see below). Discovery identifies
candidate addresses only; the server-side interface-prefix check is what
authorizes PIN-less enrollment.

TLS invitations require the sender to verify the destination node's fingerprint.
They remain pending until the node's local operator supplies the orchestrator's
fingerprint, independently verified out of band. The offered fingerprint is
informational, never an automatically trusted anchor. Neither direction follows
redirects or persists an enrollment certificate different from the actual TLS
leaf. Pairing responses are bounded to 1 MiB. Invitations do not bypass PIN
validation.

### Migration

Existing stored pins and identities do not need conversion. By default,
same-subnet auto-pairing is enabled for new joins and re-pairs; turn on strict
mode in the coordinator's **Pairing access** panel to require the verified
fingerprint and a valid PIN on every subnet. The global policy is stored in
each node's runtime settings as peers refresh and is used if a node becomes the
coordinator. Existing trust remains unchanged when strict mode is enabled.
Invitation-accept clients continue to send JSON
`{"fingerprint":"<verified 64-hex SHA-256>"}`. The compatibility status field
`auto_join_available` now reports whether a discovered candidate is on a local
interface prefix and strict mode is disabled. Peer executable updates are now
opt-in with `--allow-peer-updates`; `--no-peer-updates` wins if both flags are
present.

## Certificates and pin changes

Identity certificates are checked for validity when loaded and during TLS
handshakes. An expired local identity certificate rotates automatically while
preserving the stable node ID and trusted peer roster. Because peer trust is
fingerprint-pinned, a rotated certificate is not implicitly trusted; the
affected peer relationship must be paired again.

Changing or removing a peer pin closes pooled connections authenticated with
the old pin. If a peer certificate changes unexpectedly, verify the peer out
of band and re-pair it rather than disabling certificate checks. A pin proves
identity, not current leadership; the current election authority is checked
separately for mutating requests.

## Optional LAN discovery authentication

Discovery remains unauthenticated by default for compatibility. To authenticate
LAN announcements, set `discovery_hmac_secret` to the same standard-base64
encoding of a random 32-byte value in every node and orchestrator config.
Generate a value locally with `openssl rand -base64 32`; do not commit it or
include it in support logs. In `auto` or `both` startup, the node and
orchestrator values must match. When configured, listeners reject unsigned,
forged, stale, and replayed announcements. All discovery participants must run
a version configured with the same key; older or keyless announcements are
rejected. This authenticates discovery only; it does not replace
PIN/fingerprint pairing or prevent packet loss, denial of service, or election
splits during a network partition.

The Local Network panel labels the current instance as
**HMAC-SHA256 enabled** or **Unauthenticated (default)**. `GET
/api/security/status` returns only that mode and never the secret.

## Optional signed peer updates

Peer updates remain disabled unless `--allow-peer-updates` is set. Without a
configured publisher key, the existing paired-peer and SHA-256 checksum path
is retained for compatibility. To require release signatures on a node, set
its `peer_update_publisher_key` to the standard-base64 32-byte Ed25519 public
key for the release publisher. A configured key causes unsigned, invalidly
signed, or tampered manifests to be rejected before staging.

Keep the Ed25519 private key offline. `cmd/updatebundle` creates a `.peer.json`
manifest beside each executable. Create an unencrypted PKCS#8 key on the
publisher machine with `openssl genpkey -algorithm ED25519 -out release-private.pem`,
then sign each published manifest with:

```text
maistr0 sign-release --manifest path/to/maistr0.peer.json --key path/to/release-private.pem
```

The key must be an unencrypted PKCS#8 Ed25519 PEM file. The command updates the
manifest in place and prints the public-key configuration value; distribute
that public value to receiving nodes through protected configuration. The
signature covers the manifest metadata other than the signature itself, whose
SHA-256 and size bind it to the executable. Protect and back up the private
key separately; rotating the configured public key requires signing future
manifests with the matching private key. Every peer that may publish an update must serve a signed sidecar matching its
executable once a receiver key is configured. macOS update manifests support
both `darwin/arm64` and `darwin/amd64`.

## Local dashboard boundary

Plain HTTP is accepted only from loopback clients and only when the Host is
`localhost` or a loopback IP address. Browser requests must be same-origin;
cross-site fetch metadata and mismatched Origin or Referer headers are
rejected. Non-browser local API clients without browser-origin metadata remain
supported.

The dashboard is not an authentication boundary between mutually untrusted
local users. Protect the operating-system account and do not expose the
loopback listener through an unauthenticated reverse proxy. A reverse proxy
must authenticate users and keep cluster mTLS traffic separate; see the
[API reference](./api.md#access-and-trust).

For an authenticated dashboard proxy, disable buffering on the SSE endpoint
so updates arrive promptly. Cluster nodes must continue connecting directly
to the orchestrator's HTTPS/mTLS listener; do not terminate or replace the
client-certificate identity used for cluster traffic. Prometheus on the
orchestrator host can scrape `http://127.0.0.1:7450/metrics`; remote scrapers
must use a trusted client certificate or a separately authenticated,
network-restricted proxy.

Example Nginx dashboard location:

```nginx
location / {
    auth_basic "mAIstr0";
    auth_basic_user_file /etc/nginx/maistr0.htpasswd;
    proxy_pass http://127.0.0.1:7450;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_buffering off;
    proxy_read_timeout 660s;
}
```

## Request limits and prompt-free logging

HTTP servers bound header-read and idle-connection time. JSON request bodies
use route-appropriate limits: 64 KiB for small control records, 1 MiB for
control requests, 8 MiB for node-status reports, and 20 MiB for inference
payloads. Existing stage, synchronization, compatibility, and router-specific
limits remain in effect, with a 256 MiB absolute request-body ceiling.

Default logs and SSE snapshots omit prompts, model outputs, pairing PINs, API
keys, authentication material, and private keys. Pairing errors returned by a
peer are not copied into local errors or logs. CI runs `go vet` and
`govulncheck`.

Public engine URLs omit userinfo, query strings, and fragments; inventory
failures expose a generic health message instead of upstream content. Saving an
unchanged redacted engine endpoint preserves its existing credentials; change
credentials through protected configuration. Managed-process stdout/stderr is
drained with bounded line buffering but logged only as byte/truncation metadata,
not regex-filtered raw output. This intentionally reduces engine log detail.

## External integrations and remaining trust

Managed executables run as the application account, inherit its environment,
and are not sandboxed. Use a dedicated account with a minimal environment and
operator-controlled executable/model allowlists. HTTPS host, archive path,
size, and digest checks restrict downloads but do not independently authenticate
their publishers. A checksum fetched from the same source is not a release
signature. Signed peer manifests are optional and only enforced when a trusted
publisher key is configured; peer updates also remain disabled unless explicitly
enabled. Enabling updates trusts paired publishers to execute code, and a
configured signing key adds publisher verification (see
[configuration](./configuration.md#automatic-peer-executable-updates)).
External inference adapters retain their configured HTTP/TLS transport; cluster
certificate pinning does not secure a separate remote engine HTTP endpoint.
Use HTTPS or an authenticated encrypted network for those connections. Keep
PIN-bearing configuration files private; command-line PINs may also appear in
shell history or process listings.

vLLM distributed data traffic is **not mTLS protected**. Keep it on explicitly
trusted private interfaces, preferably an authenticated encrypted overlay such
as WireGuard; `allow_insecure_shard_network` deliberately bypasses the network
preflight. The GGML peer RPC relay uses mTLS, while its worker-side raw service
is loopback-only. Discovery can still be spoofed in the default unauthenticated
mode or disrupted regardless of HMAC configuration; network partitions can
elect multiple local coordinators. Neither certificate pins nor PINs provide
global consensus or availability against network attackers.

For coordinated vulnerability reporting, see the repository
[security policy](../SECURITY.md).
