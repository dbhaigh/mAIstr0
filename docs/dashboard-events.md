# Dashboard events and payloads

## Current behavior

`GET /api/events` continues to send full JSON snapshots as data-only SSE
messages: `data: <json>\n\n`. `internal/hub.Publisher` coalesces requests,
waits 25 ms before building, checks at a one-second backstop, and suppresses a
frame when the complete serialized snapshot is byte-identical to the previous
one. When the client accepts gzip, the hub compresses the streaming response
and flushes after each event; HTTP decompression restores the same SSE frame.
Actual publication rate depends on state changes and subscriber behavior; the
backstop is not a periodic event guarantee.

The browser coalesces incoming snapshots into animation frames. It also keeps
signatures for the status header and the overview, nodes/setup, routing,
pipeline, discovery, and task sections, so unchanged sections do not trigger
redundant DOM updates. This does not omit sections from a changing network
frame: full snapshots remain necessary for compatibility with the current
client and any existing event consumers.

## Snapshot benchmark

`BenchmarkBuildSnapshot` measures JSON serialization for a synthetic fixture
with 100 nodes, four models per node, and 50 tasks. It reports `frame-B/op`,
which includes the eight bytes used by `data: ` and `\n\n`, plus `builds/s` for
local serialization throughput. It does not run the publisher or measure a
live cluster's event frequency.

On Go 1.27.3, Windows/amd64, AMD Ryzen AI 9 HX 370, this command ran ten samples:

```sh
go test ./internal/orchestrator -run '^$' -bench '^BenchmarkBuildSnapshot$' -benchmem -count=10
```

The synthetic uncompressed SSE frame measured 168,929–169,129 bytes (about
165.2 KiB; median 169,129 bytes). A single frame flushed through a fresh gzip
stream measured 4,251–4,273 bytes (median 4,254 bytes), about 97.5% smaller.
Snapshot JSON serialization measured 1,328–1,543 builds/s (median 1,447
builds/s). This is serializer throughput only: it excludes gzip CPU time and is
not a claim that the server emits that many events per second. The compression
size is a first-frame estimate, including the gzip stream header. The exact
sample output is in
[bench-baseline/dashboard-frame.txt](./bench-baseline/dashboard-frame.txt).

## Future versioned delta format (proposal only)

No delta events are implemented. The default endpoint and current full-snapshot
format must remain unchanged. A future opt-in can use a query parameter because
the browser's native `EventSource` cannot set arbitrary request headers, for
example `GET /api/events?format=delta-v1`.

The proposed opt-in would:

- Send an initial full checkpoint with event type `snapshot.full.v1` and a
  monotonically increasing SSE `id`.
- Send later `snapshot.delta.v1` events containing a base ID, a new ID, and
  only changed named sections, such as `nodes`, `tasks`, `discovery`, `capacity`,
  `inventory`, or `pipelines`.
- Send a new full checkpoint after reconnect, an unknown base ID, or any gap;
  clients must never apply a delta to the wrong base.
- Leave clients that omit `format=delta-v1` on the existing data-only full
  snapshots. Add contract fixtures and mixed-client tests before enabling the
  opt-in.

The proposal intentionally does not change routes, event framing, snapshot
fields, or client behavior in this change.
