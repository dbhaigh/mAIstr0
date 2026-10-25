# Memory and task-history synchronization

Joined nodes synchronize memory and task history with the orchestrator.
Standalone peer clusters use the elected leader to relay records between
healthy members. Memory and task-history synchronization are separate from
inference requests and use bounded HTTP transfers.

## Protocol behavior

Stores attach optional origin and sequence metadata to records and keep
per-peer watermarks. Version 3 exchanges watermark-based updates and supports
quiet-window debounce, bounded jitter, gzip for payloads of at least 1 KiB
when it reduces size, and empty-response optimization. Protocol v2 pagination
remains supported. Older or unsupported peers use the full-snapshot
compatibility path; a watermark ahead of local state also triggers a full
snapshot.

Merges are idempotent. Experience records retain fingerprint-based
deduplication, facts retain their last-writer-wins tie rules, and legacy
records are backfilled when databases are opened without changing their
existing fields. A receiver advances its peer watermark only after a transfer
has completed; intermediate pages do not advance it.

The heartbeat interval adapts through 5, 15, 30, and 60 seconds according to
watermark progress. Store-change notifications can trigger an earlier pass
after a 500 ms quiet window. Task-history collection has a short startup wait.

## Limits and observability

Memory and task-history requests default to 64 MiB and a 10-minute timeout.
Configuration can raise the per-request limit to 256 MiB and the timeout to
3600 seconds. A single record larger than the active limit is rejected
explicitly. Paging keeps the transfer's upper watermark fixed until the
transfer completes.

The `/metrics` endpoint exposes bounded sync-page counters and peer watermark
lag. Task-history and memory-sync diagnostic logs do not print record contents
to the console.

The [sync traffic note](./sync-traffic.md) records the idle five-node
benchmark setup and measurements. The
[porting guide](../contracts/PORTING.md) documents mixed-version wire
compatibility.
