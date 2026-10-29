# Distributed-system guarantees and limits

This page states what the current architecture guarantees, what it does not,
and which tests show each behavior. It summarizes behavior that is documented
in more detail in:

- [LAN election and partitions](./lan-election.md)
- [Dispatcher retries](./dispatcher-retries.md)
- [Memory and task-history synchronization](./memory-sync.md)
- [Consensus migration status](./consensus-migration.md)
- [Concurrency, backpressure, and resource ownership](./concurrency-ownership.md)

These characterization tests run in the default suite without real models,
accelerators, or network partitions. They use fake engines, `httptest`
servers, injected discovery snapshots, and fake supervisor processes. A
passing test shows the behavior of those code paths. It does not certify a
particular hardware or network deployment.

## Summary of non-guarantees

The runtime does **not** provide:

- **A single cluster-wide coordinator during a partition.** Election ranks the
  candidates visible to each process. Each side of a partition can elect,
  promote, and serve its own coordinator.
- **Globally exclusive writes or fencing.** Pins prove identity, not current
  leadership. View headers are optional, and a node that cannot see a newer
  winner still accepts writes from the coordinator it last saw.
- **Exactly-once execution.** Workflow dispatch, router retries before the
  first response byte, and failover can run the same logical work more than
  once.
- **Conflict-free merging of divergent coordinators.** When a partition
  heals, the losing coordinator yields. Memory merges are additive and
  idempotent. Task history replaces records by task ID. Neither replays or
  reconciles the work each side performed independently.
- **Node-side admission for every inference route.** The orchestrator
  limits the work it routes or dispatches to each worker. A worker enforces
  `max_concurrent_tasks` for `/execute` and stage requests, but not for direct
  `/generate`, `/chat`, transparent-proxy, or dialogue requests.
- **Forced termination of uncooperative handlers.** SIGTERM, Ctrl+C, and
  tray **Quit** initiate cooperative component shutdown. Pending protocol
  classifiers are closed and joined; HTTP handlers receive cancellation and
  have a shared five-second shutdown grace. An engine that ignores its context
  cannot be forcibly stopped as a Go goroutine. Signal notifications are
  restored before cleanup, so a subsequent signal uses the operating-system
  default action. Startup calls without context support may
  finish before component shutdown begins. Restart recovery still marks
  interrupted work as failed and resumable.
- **Crash-atomic trust persistence.** Pairing writes the trust file in place.
  A crash or disk failure during the write can leave it unreadable.

## Coordinator discovery, election, and failover

**Guaranteed**

- Ranking is deterministic. The same visible candidate set elects the same
  winner regardless of the order in which announcements arrived. Ties are
  broken by capability score, then stable ID.
  (`TestElectionTieBreakIsStableAcrossInputPermutations`)
- An announcement expires 15 seconds after it was last heard, and only
  expired candidates leave the view. A fresh announcement restores an expired
  leader. (`TestSnapshotDropsOnlyCandidatesOutsideTheTTL`,
  `TestExpiredLeaderIsRestoredByAFreshAnnouncement`)
- Once every process shares one view again, every process elects the same
  winner. (`TestPartitionElectsOneLeaderPerSideAndHealsToOne`,
  `TestPartitionWinnersAgreeOnceAllSidesShareOneView`)
- An orchestrator's write authority follows its current local view; it
  rejects service with HTTP 503 when it sees a higher-ranked active
  orchestrator. (`TestElectionComponentWriteAuthorityFollowsLocalView`,
  `TestYieldedOrchestratorRejectsPairingAndMutations`)
- A node starts a failover election only after the configured number of
  consecutive failed heartbeat probes. Untrusted candidates cannot win.
  (`TestHeartbeatMonitorElectionThresholdCharacterization`,
  `TestFailoverReElectionCharacterization`)
- After the failover coordinator is closed, an in-progress election window
  ends and no later election, promotion, or step-down watch starts.
  (`TestFailoverCoordinatorCloseFencesLaterPromotion`,
  `TestFailoverElectionWindowElapsesWhileOpen`)

**Not guaranteed under partition**

- Each side elects and serves a local winner.
  (`TestPartitionElectsOneLeaderPerSideAndHealsToOne`,
  `TestFailoverPartitionsPromoteIndependently`)
- Both winners accept writes for as long as they cannot see each other. There
  is no quorum, lease, or epoch that stops the minority side.
- Detection is bounded by timers, not by agreement: four failed five-second
  probes, then a 15-second election window. A slow but live coordinator can
  be replaced while it is still serving some members.
- Healing depends on announcements becoming mutually visible. One-way
  visibility heals only through the step-down check described in
  [LAN election](./lan-election.md#stepping-down-after-failover). If the
  weaker leader can never see the stronger one, both stay elected.
- After a failover election, the heartbeat monitor stays triggered until a
  probe succeeds. Its first success reports the previous leader as recovered
  even when a new leader was promoted.
- Close does not wait for an election goroutine that is already running. A
  narrow window remains between a promotion and the local state update.

## Pairing, trust, and lifecycle authority

**Guaranteed**

- A pairing PIN expires after 30 minutes, is accepted once, and is locked out
  after 20 failed attempts. A successful remote join rotates the PIN.
  (`TestPairingPINLifecycleCharacterization`,
  `TestRemotePINJoinRotatesPINForNextJoin`)
- A rejected join (wrong, expired, or reused PIN) leaves the trust roster
  unchanged. (`TestRemotePairingJoinRejectionsLeaveTrustUnchanged`)
- Pinned clients refuse a server that presents a different certificate, or a
  pinned certificate under another expected ID. Unpinned TLS clients are
  rejected by trusted-peer gates.
  (`TestPinnedClientsRefuseServerWithDifferentCertificate`,
  `TestPinnedDialRejectsPinnedServerUnderAnotherExpectedID`,
  `TestTrustedPeerGatesRejectUnpinnedClientsOverTLS`)
- Revoking a pin closes its tracked inbound and outbound connections, and
  later requests fail. (`TestRevocationClosesConnectionsAndFailsLaterRequests`,
  `TestRevocationClosesTrackedInboundConnection`)
- When a node sees a different election winner, it rejects state-changing
  requests from the previously paired orchestrator. Reads, and requests from
  other pinned members, keep their existing behavior. Managed-engine lifecycle
  endpoints reject callers that are not the current coordinator.
  (`TestCoordinatorWriteFenceMethodAndCallerMatrix`,
  `TestCoordinatorHandoffMovesWriteAuthorityOverMutualTLS`,
  `TestManagedLifecycleEndpointsRejectUnauthorizedCallers`)
- A request that supplies stale or incomplete view headers is rejected with
  HTTP 409. (`TestViewFenceAdditionalStaleViewCases`)

**Not guaranteed**

- Discovery does not authenticate membership or leadership. New enrollment
  can omit the PIN and fingerprint only when strict verification is disabled
  and the accepted connection shares a prefix assigned to the orchestrator's
  local interfaces. Discovery metadata alone never grants membership. Existing
  pinned relationships can reconnect.
- Requests without view headers are not fenced, for backward compatibility.
- A node that cannot see the newer winner still treats the old orchestrator
  as current.
- Trust-file writes are not crash-atomic. A failed pin write is rolled back in
  memory, so the peer stays untrusted. A failed revocation write keeps the peer
  revoked in memory but leaves it trusted on disk, so trust returns after a
  restart unless the revocation is retried.
  (`TestTrustPersistenceFailureCharacterization`,
  `TestPairingJoinPersistenceFailureCharacterization`)
- A pairing join whose trust write fails returns HTTP 400, not a server error,
  and consumes the PIN without issuing a replacement.
- The node write fence only checks callers that were paired as orchestrators.
  Other pinned members can still send state-changing requests. A node that
  knows no coordinator treats itself as the coordinator.

## Task lifecycle, retries, and recovery

**Guaranteed**

- A dependent subtask waits for every prerequisite and receives their outputs
  in order. A failed prerequisite releases its dependents without input
  rather than blocking them.
  (`TestDependentWaitsForEveryPrerequisiteAndReceivesOrderedOutputs`,
  `TestFailedPrerequisiteReleasesDependentsWithoutInput`)
- Approval is accepted once, including under concurrent approval, and only
  for a task that is awaiting approval.
  (`TestApproveIsAcceptedOnlyOnceUnderConcurrency`,
  `TestApproveRejectsTasksNotAwaitingApproval`)
- A finished subtask or terminal task ignores late or duplicate outcomes. A
  finished subtask cannot be started again, and a finished dispatch attempt
  is recorded once. (`TestTerminalTasksIgnoreLateSubtaskUpdates`,
  `TestFinishedDispatchAttemptCannotBeRecordedTwice`)
- Only failed or cancelled tasks can be resumed. Resuming any other task
  returns HTTP 409, and concurrent resume requests admit exactly one caller.
  (`TestResumeIsLimitedToStoppedTasks`,
  `TestConcurrentResumeAdmitsExactlyOneCaller`)
- After a restart, in-progress work is recovered as failed and resumable, and
  a later resume continues the persisted attempt history.
  (`TestRestartRecoveryThenResumeContinuesAttemptHistory`)

**Not guaranteed**

- Execution is at least once, not exactly once. See
  [lost responses and duplicate compute](./dispatcher-retries.md#lost-responses-and-duplicate-compute).
- Subtasks have no dispatch lease. Two concurrent dispatches of the same
  running subtask can both execute on workers. Only the first outcome is
  recorded. (`TestConcurrentDuplicateDispatchRecordsSingleWinner`)
- A result from a dispatch that started before a cancel and resume can still
  arrive after the resume and be recorded for the re-queued subtask.
- Every task is created resumable. Whether a task can actually be resumed is
  checked only when resume is requested.
- Recovery cannot tell whether an interrupted attempt finished on its worker.
- Coordinators on both sides of a partition can each run their own copy of a
  task. Task-history synchronization replaces records by task ID and does not
  merge their attempts.

## Capacity reservations

**Guaranteed**

- A router reservation is released exactly once on each tested terminal
  path: a successful stream, a non-retryable upstream rejection, a client
  cancellation before upstream headers, and a client disconnect after the
  stream is committed. (`TestRouterReleasesCapacityOnEveryTerminalPath`)
- Concurrent routed requests never exceed a node's orchestrator-side
  capacity. (`TestRouterConcurrentRequestsNeverExceedNodeCapacity`)
- A shard-group request lease is single-use and remains safe when the group
  is stopped while the lease is held.
  (`TestShardAcquireRequestReleaseIsSingleUseAcrossStop`)
- A failed shard-group acquisition does not call a release callback, and the
  router verifies the head's model only while it holds a lease. It releases
  the lease when verification fails.
  (`TestRouterShardGroupAcquisitionAndHeadVerification`)

**Not guaranteed**

- Reservations are local to one orchestrator process. Two coordinators in a
  partition reserve against the same worker independently.
- A worker does not apply its own capacity limit to direct generate, chat,
  proxy, or dialogue requests.
- Router timeouts and handler panics release reservations through deferred
  calls, but no test asserts those paths.

## Streaming, cancellation, and dashboard updates

**Guaranteed**

- Each in-flight harness or dialogue turn owns its own cancellation handle.
  An overlapping turn on the same session cannot remove another turn's
  handle, and an interrupt cancels every in-flight turn on that session.
  (`TestOverlappingHarnessTurnsKeepTheirOwnCancellation`,
  `TestOverlappingDialogueTurnsKeepTheirOwnCancellation`)
- A streamed harness turn ends with a `done` event when it completes and an
  `error` event when the backend fails. A client disconnect cancels the turn
  without an error event. In every case the turn's cancellation handle is
  removed. (`TestHarnessStreamTerminalEventsAndCleanup`)
- The router does not retry after the first response byte has been sent to
  the client.
- The dashboard snapshot counts every healthy member, regardless of where the
  leader appears in the node list.
  (`TestSnapshotCountsEveryHealthyMemberRegardlessOfLeaderPosition`)

**Not guaranteed**

- Agent-loop progress events are best effort. An event that cannot be queued
  within 50 ms is dropped. The final message returned by the turn is not
  affected.
- Streams cannot be resumed after a disconnect.
- Dashboard updates are coalesced. Slow subscribers miss intermediate
  snapshots and receive a later full snapshot.
- Overlapping turns on one dialogue are not serialized. Each reads the history
  as it was when it started, and replies are appended in completion order.

## Memory synchronization

**Guaranteed**

- Merges are idempotent; repeated and reordered deliveries converge. See
  [memory sync](./memory-sync.md#protocol-behavior) for the tie rules.
- A peer watermark advances only after a complete transfer. An interrupted
  paged pull leaves the watermark unchanged, so the next pass resumes from the
  last committed point. (`TestSyncPeerMemoryInterruptedPagedPullDoesNotAdvanceWatermarks`)
- A store that cannot be read returns an error (HTTP 500 to a peer) instead
  of an empty snapshot, and a sync pass against an unreadable peer does not
  advance watermarks. (`TestPeerMemorySyncGetReportsUnreadableStore`,
  `TestFetchMemorySyncFromUnreadablePeerDoesNotAdvanceWatermarks`)
- A peer watermark ahead of local state, or an older protocol version, falls
  back to a full snapshot.
- Replayed, reordered, interleaved-stale, and partially applied pages
  converge to the same state, and a bbolt peer resume point survives a
  reopen. (`TestSyncCharacterizationReplayedBatchIsIdempotent`,
  `TestSyncCharacterizationReorderedPagesConverge`,
  `TestSyncCharacterizationPartialPageKeepsPeerResumePoint`,
  `TestSyncCharacterizationResumePointSurvivesReopen`)

**Not guaranteed**

- Synchronization is eventual. Records written during a partition reach the
  other side only after a later successful pass.
- Facts resolve conflicts by last writer wins, so clock skew between nodes can
  affect which value is kept.
- Store-level `Snapshot()` callers that are not part of synchronization still
  log a read failure and receive an empty snapshot.
- A sequence gap that is already covered by an advertised watermark is
  recovered only by a full snapshot.
  (`TestSyncCharacterizationSequenceGapRecovery`)
- A wiped bbolt peer that keeps its owner ID and writes past its old watermark
  before the next sync hides the reused sequences until a full snapshot.
  (`TestSyncCharacterizationWipedPeerSequenceRegression`)

## Managed shard groups

**Guaranteed**

- Creation fails without reserving capacity when members lack memory, and
  before starting any engine when a member is unavailable.
  (`TestShardCreateInsufficientMemoryFailsWithoutReservations`,
  `TestShardCreateUnavailableMemberFailsBeforeStartingEngines`)
- When a member fails to start, or starts but does not report ready, members
  that were already started are stopped in reverse order.
  (`TestShardCreateMemberStartFailureStopsStartedMembersInReverse`)
- Stop and delete are idempotent, and conflicting operations are rejected
  while one is in progress. (`TestShardStopAndDeleteIdempotency`,
  `TestShardOperationsConflictWhileStartInProgress`)
- Recovery from persisted state re-verifies every member before the group is
  ready. (`TestShardRecoveryFromPersistedStateReverifiesMembers`)
- A supervisor startup failure leaves the engine retryable, duplicate starts
  are rejected, stops are idempotent, and a pre-launch failure does not leak
  the output pipe. (`TestManagerStartupFailureLeavesRetryableFailedState`,
  `TestManagerRejectsDuplicateStartAndStopsIdempotently`,
  `TestVLLMNetworkPreflightFailureClosesOutputPipe`)

**Not guaranteed**

- A failed member is not replaced automatically; the group degrades and then
  fails until it is restarted.
- Inference is serialized per group. Adding members increases model capacity,
  not request throughput.
- vLLM distributed traffic is not encrypted and requires a trusted private
  network.
- Fake-process tests do not certify tensor correctness or accelerator
  compatibility. Use the [hardware validation checklist](./hardware-validation.md).

## Race testing

The concurrency boundaries above are exercised under `go test -race` on Linux,
including concurrent approval, resume, overlapping turns, router reservations,
and shard leases. Native Windows race runs can fail before tests start; see
[development](./development.md) for that limitation.
