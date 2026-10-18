# Dispatcher retries and response loss

## Workflow dispatch policy

Each subtask gets a three-minute dispatch deadline after its prerequisites are
ready. Dependency waiting happens before that deadline; waiting for capacity,
worker requests, and retry backoff happen within it. Cancellation or deadline
exhaustion stops the current request or wait and prevents another retry.

The dispatcher allows up to five total assignment attempts, including the
initial assignment. Before each retry it excludes the exact node/model pair
already tried and asks the scheduler for a different eligible pair, which may
be another model on the same node. No retry is made when none is available.
Retry delays are deterministic exponential backoff: 100, 200, 400, then 800
ms, capped at 800 ms. There is no jitter or `Retry-After` handling.
Unhealthy-node checks and capacity reservation can cause reassignment before a
worker request is sent; those preflight moves are not worker attempts and do
not create dispatch-attempt rows.

Transport errors and network timeouts are retryable. So are HTTP 408, 429, 500,
502, 503, and 504; a 404 whose response body contains “model” (case
insensitive); and a successful HTTP response with invalid JSON. Other HTTP
errors, worker-reported execution errors, and output-contract validation
failures do not trigger a cross-node retry. Output validation may request a
bounded correction from the same assignment (`MaxRetries`); each such worker
request has its own attempt record and ID.

Before each `/execute` request, task history persists a running attempt with a
stable request ID (`task_id:subtask_id`) and a distinct, one-based attempt ID
(`task_id:subtask_id:number`). The IDs are sent as `X-Maistr0-Request-ID` and
`X-Maistr0-Attempt-ID`. Failed attempts are persisted before selecting the
next assignment. A successful response is committed to task history only
after the response is decoded, output validation succeeds, and one task-store
write records both the successful attempt and the subtask output. With no
persistent task store configured, that commit is only in memory. If the
orchestrator restarts while an attempt is still running, recovery marks it
failed with `orchestrator_restart`; it cannot tell whether the worker finished.

## Lost responses and duplicate compute

An HTTP success is not proof that the orchestrator received or committed the
result. If the worker finishes but the response is lost, the first attempt is
recorded as a transport failure and normal retry policy may send the same
logical subtask to a different node/model. Both workers may compute it. The
stable IDs are correlation data, not deduplication tokens, and workflow
dispatch is **not exactly once**. A response received but not yet committed to
the task store may also be lost in a process or storage failure.

## Optional generate idempotency key

Non-streaming `POST /api/generate` requests may include the standard
`Idempotency-Key` header (up to 200 bytes). The orchestrator forwards it to the
worker's `/generate` endpoint. For that worker process, successful results are
cached for ten minutes, up to 512 keys and 8 MiB of result text; individual
results larger than 1 MiB are returned but not retained. Concurrent requests
with the same key and request payload share the result. Reusing a key with a
different effective model, prompt, or generation options returns 409.
If all 512 slots are in-flight, a new key is rejected with 503. Failed
generations are not cached. Streaming generate requests do not support this
header.

This cache is worker-local, in-memory, and bounded. Pinning a node (for
example, using `/api/nodes/{id}/generate`) gives retries the best chance to
reach the same cache. A cluster-routed retry may select another worker, and a
worker restart or cache eviction forgets prior results. The key therefore
reduces duplicate work in the same worker process but does not provide
exactly-once execution. Requests without the header keep their existing
behavior; this facility does not cover workflow `/execute` dispatch.
