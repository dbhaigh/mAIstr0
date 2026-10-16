# Dispatcher retries

Subtask dispatch retries transient transport failures, timeouts, rate limits,
upstream server errors, invalid node responses, and stale-model responses. A
retry excludes the exact node/model pair already attempted, so the next attempt
uses a distinct pair when one is available. Permanent client errors and output
validation failures do not trigger a cross-node retry. Retry waits and capacity
reservations honor the dispatch context; cancellation or deadline exhaustion
fails the dispatch rather than starting another attempt.

Each dispatch attempt has a stable attempt ID recorded in task history and
forwarded to the worker with the request ID. These IDs support tracing; they
do not provide worker-side deduplication or exactly-once execution. If a worker
completes work but its response is lost before the orchestrator receives it,
the orchestrator cannot distinguish that case from a failed execution and may
retry the subtask on another node/model. Both workers may therefore have
completed the same logical work.
