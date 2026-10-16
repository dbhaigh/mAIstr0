# LAN election and partitions

Orchestrator startup uses the capability score and stable ID of orchestrator
candidates visible during the six-second startup election. The UDP discovery
listener keeps an announcement visible for 15 seconds after it was last heard.
The election has no quorum, consensus log, or cross-partition coordinator.

If a LAN is partitioned, the candidates visible on each side can differ. Each
partition may elect its own local winner, and both winners may continue serving
their local cluster. When announcements become visible again, instances rank
the currently visible candidates using the same capability/ID ordering; an
announcement that has expired is no longer part of that view. This is local
leader selection, not a guarantee that only one leader exists across a
partitioned network.

Pairing trust is specific to the identity that was paired. It does not transfer
to another elected orchestrator, even when the same node can see that
orchestrator on the LAN. A node must be authorized separately by a new winner:
remote pairing requires that leader's one-time PIN, and a node may accept its
invitation locally. The existing same-subnet auto-pairing behavior is
unchanged. Discovery visibility alone does not grant membership.

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
