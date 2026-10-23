# Pairing and security

mAIstr0 is local-first. Inference and orchestration run on configured engines
and cluster nodes; a cloud service is not required. Cluster peers authenticate
with self-signed certificates whose SHA-256 fingerprints are pinned during
pairing. Remote cluster traffic requires mutual TLS 1.3 or later and a
currently valid, explicitly trusted peer certificate. The trust roster is
stored with the node identity using owner-only file permissions.

## Pairing a node

1. Open the orchestrator dashboard's **Setup** panel and copy its one-time
   pairing PIN.
2. Open the node dashboard's **Setup** panel, enter the orchestrator's HTTPS
   address and PIN, and confirm the selected orchestrator identity.
3. For unattended enrollment, provide the PIN out of band with
   `--pairing-pin` or the node config's `pairing_pin`.

PINs are six digits, expire after 30 minutes, can be used once, and are
invalidated after 20 failed attempts. Comparisons use a constant-time
comparison. Attempt and outcome logs omit PIN values. The local operator can
view the active PIN or generate a replacement from the Setup panel.

Nodes on the same discovered subnet may use the existing automatic-join path
without a PIN. This is a deliberate trust boundary: enable LAN discovery only
on networks where devices are trusted. Cross-subnet pairing requires the
out-of-band PIN. Orchestrator-to-orchestrator pairing is disabled.

TLS invitations to nodes remain pending until accepted locally. An invitation
does not bypass the orchestrator's PIN validation. Discovery visibility alone
does not grant membership.

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

For coordinated vulnerability reporting, see the repository
[security policy](../SECURITY.md).
