# Cluster sync traffic

## Existing flow

- A joined node fetches the orchestrator's memory snapshot, merges it, and
  posts its local snapshot back to the orchestrator on each memory pass.
- In a standalone peer cluster, the elected leader fetches each healthy peer's
  memory snapshot and merges it. It only posts memory when it has new records.
- The orchestrator and standalone leader fetch task-history pages from healthy
  nodes, merge them, then post each peer's filtered task snapshot. An empty task
  snapshot was still sent as `[]`.
- Store-change notifications can wake memory and task loops early. Otherwise,
  the existing adaptive heartbeat advances through 5, 15, 30, and 60 seconds
  based on watermark progress. The orchestrator task loop has a 3-second
  startup wait. Changes were not debounced and heartbeat intervals had no
  jitter.
- Memory GETs returned JSON snapshots, task GETs returned JSON arrays, and the
  sync protocol was version 2. Incremental watermarks and full-snapshot
  fallback were already supported.

## Idle five-node baseline

The reproducible benchmark is
`go test ./internal/nodeagent -run '^$' -bench '^BenchmarkIdleSyncTraffic5Node$' -benchtime=1x -benchmem`.
It simulates one standalone leader and four healthy, idle peers. Each benchmark
iteration performs one memory sync pass and one task-history pass. Byte counts
include HTTP request/status lines, headers, and bodies; they exclude TLS,
TCP/IP framing, connection setup, and idle keep-alive bytes.
Both measurements used Windows/amd64 with Go 1.27.0 and `-benchtime=1x`; the
reported byte metrics are deterministic for this fixture, while `ns/op` is
not intended as a stable latency result.

Before the protocol changes, the measured values were:

- Memory: 1,424 bytes per pass.
- Task history: 2,008 bytes per pass, including the empty `[]` POST to each
  peer.
- At a 30-second memory interval and 15-second task interval: 181.3 bytes/sec.
- At a 15-second memory interval and 15-second task interval: 228.8 bytes/sec.

These are steady-state application HTTP bytes for the stated topology, not a
measurement of TLS/network framing or first-time full-snapshot replication.

## Updated protocol and sync behavior

Protocol v3 adds a 500 ms quiet-window debounce for change-triggered sync and
up to 10% jitter on heartbeat waits. Existing adaptive intervals remain the
heartbeat fallback. Empty v3 GET exchanges return 204 with negotiated version
and watermark headers; senders omit empty task and memory POSTs. Gzip is
advertised with `Accept-Encoding: gzip` and used for payloads of at least 1 KiB
when it reduces size. Request compression is enabled only after the peer
responds as protocol v3. The decompressed and encoded body sizes both remain
subject to the configured sync limit.

Protocol v1/v2 peers continue to exchange JSON; v2 pagination remains
available. Unsupported versions and watermarks ahead of local state still use
the full-snapshot fallback. See the [porting guide](../contracts/PORTING.md) for
wire compatibility details.

The same benchmark after implementation measured:

- Memory: 1,024 bytes per pass, down 28.1%.
- Task history: 1,000 bytes per pass, down 50.2%.
- At a 30-second memory interval and 15-second task interval: 100.8 bytes/sec,
  down 44.4%.
- At a 15-second memory interval and 15-second task interval: 134.9 bytes/sec,
  down 41.0%.

The idle benchmark uses empty responses, so gzip does not contribute to this
reduction; the gains come from watermark-only 204 responses and omitting the
empty task POST. The benchmark output reports application bytes/sec directly
for both memory heartbeat assumptions.
