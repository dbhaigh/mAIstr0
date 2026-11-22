# Dispatcher retries and response loss

## Workflow dispatch policy

Each subtask gets a dispatch deadline after its prerequisites are ready. The
default is 1800 seconds and can be changed with `dispatch_timeout_seconds`.
Dependency waiting happens before that deadline; waiting for capacity, worker
requests, and retry backoff happen within it. Cancellation or deadline
exhaustion stops the current request or wait and prevents another retry. See
[configuration](./configuration.md#capacity-timeouts-and-synchronization) for
the timeout setting.

The retry budget remains five total assignment attempts, including the initial
assignment, but it does not authorize replay after an ambiguous `/execute`
result. Before an eligible retry, the dispatcher excludes the exact node/model
pair already tried and asks the scheduler for a different eligible pair, which
may be another model on the same node. A `/execute` HTTP 404 whose response
body identifies a missing model is treated as a pre-execution rejection and
may retry. Preflight reassignment before sending a worker request is also safe
to retry. Transport errors, timeouts or cancellation after dispatch starts,
worker 5xx responses, worker-reported execution errors, malformed success
responses, and failures committing a received result are not replayed.
Retry delays for eligible retries are deterministic exponential backoff: 100,
200, 400, then 800 ms, capped at 800 ms. There is no jitter or `Retry-After`
handling. Router/inference retries and shard-group inference retry behavior
remain separate and unchanged.
Unhealthy-node checks and capacity reservation can cause reassignment before a
worker request is sent; those preflight moves are not worker attempts and do
not create dispatch-attempt rows, but still consume a dispatch-loop iteration.
If the configured budget is exhausted before a worker request, the subtask now
completes with an attempt-limit error instead of remaining unfinished.

## Failure tracing

Terminal dispatch failures emit a structured `dispatch_error` log event with
the request, task, subtask, phase, assignment, failure class, retryability, and
error type. The event records error types rather than raw error messages.
Attempt-completion records use the `dispatch_attempt` event type and include
the same request and attempt identifiers. Task errors include the failing
phase, while the in-process error preserves its cause for `errors.Is` and
`errors.As`; task history stores the enriched error text. These records do not
change retry classification, assignment exclusions, attempt limits, or
backoff.

For side-effecting `/execute` calls, transport errors, timeouts, worker 5xx
responses, malformed success responses, and worker-reported execution errors
are ambiguous and stop the dispatch. Only a 404 whose response body identifies
a missing model is considered a known pre-execution rejection eligible for a
cross-node retry. Output-contract validation can still request the subtask's
configured bounded correction from the same assignment after a result has been
received; each request has its own attempt record and ID. This policy does not
change router or shard-group inference retries.

Before each `/execute` request, task history persists a running attempt with a
stable request ID (`task_id:subtask_id`) and a distinct, one-based attempt ID
(`task_id:subtask_id:number`). The IDs are sent as `X-Maistr0-Request-ID` and
`X-Maistr0-Attempt-ID`. Failed attempts are persisted before selecting the
next assignment. A successful response is committed to task history only after the response is
decoded, output validation succeeds, and one task-store write records both the
successful attempt and the subtask output. With no persistent task store
configured, that commit is only in memory. A failure to decode or persist a
response after dispatch is treated as ambiguous. If the orchestrator restarts
while an attempt is still running, recovery marks it `outcome_unknown`; it
cannot tell whether the worker finished.

## Lost responses and duplicate compute

An HTTP success is not proof that the orchestrator received or committed the
result. If the worker finishes but the response is lost, the attempt is
recorded as failed with `failure_class: "outcome_unknown"` and the task error
reports “outcome unknown; reconciliation required before retrying.” The task is
not listed as resumable, and its resume endpoint returns HTTP 409. Cancellation
of a running dispatch and recovery of a persisted running attempt use the same
state. Late responses cannot change the unknown outcome to success. Inspect
the assigned worker and any external effects, then decide whether to compensate
or submit new work; there is no automated reconciliation or fencing endpoint.

Dispatch admission allows only one active or unknown attempt for a subtask
within one task manager. This prevents a local duplicate dispatch from
launching a concurrent worker request. It does not deduplicate separate task
submissions: task IDs and admission state are local to each manager. Two
coordinators during a partition can both execute a copy, and a delayed worker
may continue after request cancellation. Stable IDs are correlation data, not
deduplication tokens. There is no cluster-wide fencing or exactly-once
guarantee.

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
