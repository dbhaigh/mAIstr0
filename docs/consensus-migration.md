# Quorum consensus migration

## Required behavior

The target control plane uses Raft with a stable node identity and a persistent
cluster identity. Every paired node is a voter. Voter additions and removals
are committed through the Raft configuration, and a minority partition cannot
commit coordinator mutations. A two-voter cluster requires both voters to be
available; three voters can tolerate one unavailable voter. Capability may
influence preferred candidacy, but never overrides quorum.

Consensus transport is mutually authenticated using paired certificate pins
and checks the cluster identity before exchanging Raft messages. A node with a
different persisted cluster identity must not join. Application mutations use
stable operation IDs so retries after uncertain responses are safe.

All nodes must be upgraded together. Older binaries still use ranking plus TTL
and cannot participate in or be fenced by Raft; mixed-version operation cannot
claim the global no-split-brain guarantee.

## Migration stages

1. Add the durable consensus core, dynamic voter operations, authenticated
   transport primitives, and deterministic partition tests.
2. Start consensus on paired nodes and use quorum-backed authority to gate
   coordinator and node-agent writes.
3. Move all coordinator mutations into the replicated state machine and define
   recovery and retry behavior for operations whose response is lost.
4. Add the discovery context action to pair or re-pair with the selected
   orchestrator and enroll the node in the consensus voter set.
5. Complete coordinated-upgrade documentation, contracts, and multi-node
   acceptance tests.

The first-stage foundation is implemented in `internal/consensus`: persistent
identity and Raft state, snapshot/log recovery, operation-ID deduplication,
dynamic voter operations, authenticated transport primitives, and deterministic
partition tests. It is not yet wired into application startup, pairing flow, or
API handlers. The running application therefore still uses the ranking-and-TTL
behavior documented in [`lan-election.md`](./lan-election.md), and does not yet
provide a global no-split-brain guarantee.
