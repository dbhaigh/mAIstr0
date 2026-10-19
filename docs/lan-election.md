# LAN election and partitions

Default automatic startup waits one 15-second discovery lease before choosing
a role. If it sees an active orchestrator, it joins that coordinator as a node;
otherwise, it elects among the visible startup candidates by capability score
and stable ID. Explicit orchestrator startup still uses capability and stable
ID to select its coordinator. The UDP discovery listener keeps an announcement
visible for 15 seconds after it was last heard. The election has no quorum,
consensus log, or cross-partition coordinator.

If a LAN is partitioned, the candidates visible on each side can differ. Each
partition may elect its own local winner, and both winners may continue serving
their local cluster. When announcements become visible again, instances rank
the currently visible candidates using the same capability/ID ordering; an
announcement that has expired is no longer part of that view. This is local
leader selection, not a guarantee that only one leader exists across a
partitioned network.

Node agents probe their current orchestrator's `GET /api/status` endpoint every
five seconds with a two-second request timeout. Four consecutive failed probes
start a background failover election without blocking node task scheduling.
The failed coordinator is excluded from that node's election view while the
visible startup candidates are ranked for one 15-second discovery lease.

A promoted node reuses its paired node identity and member certificate roster;
it does not create a new trust identity. Members that already trust that node
can register with the promoted coordinator, and update its role to
orchestrator after registration. Untrusted LAN announcements cannot win a
failover election. New members still require authorization: remote pairing
requires the elected leader's one-time PIN, and the existing same-subnet
auto-pairing and invitation flows remain unchanged. Discovery visibility alone
does not grant membership.

Failover remains partition-local. Simultaneous elections on disconnected
network segments may promote different nodes, and recovery does not provide
quorum-based conflict resolution or globally exclusive writes.

## Write authority and trust

Election is evaluated against each process's current discovery snapshot. An
orchestrator that sees a higher-ranked active orchestrator stops serving its
control API with HTTP 503; this applies to paired TLS callers as well as local
dashboard requests. A partition that cannot see the other winner can continue
to accept writes under its own local view. Ranking and expiration cannot fence
a remote partition that is not visible; this system does not provide consensus
or globally exclusive writes.

Node agents accept plaintext API traffic only from loopback. Other requests
must present a client certificate whose fingerprint is pinned by pairing.
When a node sees a different active election winner, it rejects state-changing
requests signed by a previously paired orchestrator identity; reads and
requests from other pinned cluster members retain their existing behavior.
The paired identities remain pinned across a leadership change, but a pin
proves identity, not current leadership. A previously paired orchestrator can
become the local authority again if it is the best active orchestrator visible
to that node.

Pairing bootstrap is the deliberate exception to ordinary pin authentication.
`POST /api/pairing/join` is accepted only by the locally elected orchestrator
and requires its one-time PIN, except for the existing same-subnet auto-pair
flow. A TLS invitation to a node creates only a pending invitation; the node
user must accept it locally, and the subsequent join still goes through the
orchestrator's PIN check. An off-subnet join with a wrong or expired PIN does
not add a trust pin.

## Optional request-view fencing

Active orchestrator responses expose the server's current view in
`X-Maistr0-Orchestrator-ID` and `X-Maistr0-Orchestrator-Epoch` headers.
`GET /api/status` keeps its existing JSON body shape; the ID and opaque
per-process startup epoch are available only in response headers. A client may
send both headers on a state-changing request. If either supplied value is
missing or does not match the receiving server's view, the request is rejected
with HTTP 409 (`apperr.Conflict`). The response still carries the server's
current view, so the client can refresh before retrying.

This fence is deliberately backward-compatible: clients that send neither
header continue to work as before. Pairing and invitation endpoints remain
governed by their existing PIN/trust checks and are not fenced by these
optional headers. A yielded server continues to reject service with HTTP 503
(`apperr.Unavailable`); request-view fencing does not create consensus or make
partition-local elections globally authoritative.

## Consensus migration status

The application still runs the ranking-and-TTL election described above.
Quorum consensus is being introduced in stages; the current status and the
coordinated-upgrade requirement are documented in
[`consensus-migration.md`](./consensus-migration.md). Until consensus is wired
to startup and all mutation paths, the runtime does not guarantee one global
coordinator across partitions.
