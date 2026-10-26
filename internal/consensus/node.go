package consensus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

var (
	ErrNotLeader         = errors.New("consensus: this node is not the quorum-backed leader")
	ErrOutcomeUnknown    = errors.New("consensus: operation outcome is unknown")
	ErrLastVoter         = errors.New("consensus: cannot remove the last voter")
	ErrMemberNotFound    = errors.New("consensus: member is not in the voter configuration")
	ErrInvalidRaftConfig = errors.New("consensus: invalid Raft configuration")
)

const (
	defaultHeartbeatTimeout = time.Second
	defaultElectionTimeout  = time.Second
	defaultLeaderLease      = 500 * time.Millisecond
	defaultCommitTimeout    = 50 * time.Millisecond
	defaultEnqueueTimeout   = 5 * time.Second
	defaultMembershipWait   = 10 * time.Second
)

type Config struct {
	ID                 string
	Address            string
	ClusterID          string
	DataDir            string
	Bootstrap          bool
	Transport          raft.Transport
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
	CommitTimeout      time.Duration
	EnqueueTimeout     time.Duration
	MembershipTimeout  time.Duration
	SnapshotInterval   time.Duration
	SnapshotThreshold  uint64
	TrailingLogs       uint64
}

type Node struct {
	id                raft.ServerID
	clusterID         string
	raft              *raft.Raft
	fsm               *stateMachine
	transport         raft.Transport
	store             *boltStore
	enqueueTimeout    time.Duration
	membershipTimeout time.Duration

	closeOnce sync.Once
	closeErr  error
}

// Open creates or restores a durable Raft participant. The caller owns
// bootstrap policy and must supply a transport secured for this cluster.
func Open(cfg Config) (*Node, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("consensus: create data directory: %w", err)
	}
	identity, err := LoadOrCreateIdentity(
		filepath.Join(cfg.DataDir, "cluster-identity.json"),
		cfg.ID,
		cfg.ClusterID,
		cfg.Bootstrap,
	)
	if err != nil {
		return nil, err
	}
	store, err := openBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("consensus: open Raft store: %w", err)
	}
	snapshots, err := raft.NewFileSnapshotStore(filepath.Join(cfg.DataDir, "snapshots"), 3, io.Discard)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("consensus: open snapshot store: %w", err), store.Close())
	}
	existing, err := raft.HasExistingState(store, store, snapshots)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("consensus: inspect persisted Raft state: %w", err), store.Close())
	}

	fsm := newStateMachine()
	fsm.persistAppliedIndex = store.RecordAppliedIndex
	if err := restoreApplicationState(fsm, store, snapshots); err != nil {
		return nil, errors.Join(fmt.Errorf("consensus: restore committed application state: %w", err), closeTransport(cfg.Transport), store.Close())
	}
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.ID)
	raftConfig.HeartbeatTimeout = durationOr(cfg.HeartbeatTimeout, defaultHeartbeatTimeout)
	raftConfig.ElectionTimeout = durationOr(cfg.ElectionTimeout, defaultElectionTimeout)
	raftConfig.LeaderLeaseTimeout = durationOr(cfg.LeaderLeaseTimeout, defaultLeaderLease)
	raftConfig.CommitTimeout = durationOr(cfg.CommitTimeout, defaultCommitTimeout)
	raftConfig.SnapshotInterval = durationOr(cfg.SnapshotInterval, 30*time.Second)
	raftConfig.SnapshotThreshold = uintOr(cfg.SnapshotThreshold, 1024)
	raftConfig.TrailingLogs = uintOr(cfg.TrailingLogs, 256)
	raftConfig.ShutdownOnRemove = true
	raftConfig.NoSnapshotRestoreOnStart = true
	raftConfig.LogOutput = io.Discard
	raftConfig.LogLevel = "ERROR"
	raftNode, err := raft.NewRaft(raftConfig, fsm, store, store, snapshots, cfg.Transport)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("consensus: create Raft node: %w", err), closeTransport(cfg.Transport), store.Close())
	}
	node := &Node{
		id:                raft.ServerID(cfg.ID),
		clusterID:         identity.ClusterID,
		raft:              raftNode,
		fsm:               fsm,
		transport:         cfg.Transport,
		store:             store,
		enqueueTimeout:    durationOr(cfg.EnqueueTimeout, defaultEnqueueTimeout),
		membershipTimeout: durationOr(cfg.MembershipTimeout, defaultMembershipWait),
	}
	if cfg.Bootstrap && !existing {
		future := raftNode.BootstrapCluster(raft.Configuration{Servers: []raft.Server{{
			ID: raft.ServerID(cfg.ID), Address: raft.ServerAddress(cfg.Address), Suffrage: raft.Voter,
		}}})
		if err := future.Error(); err != nil {
			return nil, errors.Join(fmt.Errorf("consensus: bootstrap cluster: %w", err), node.Close())
		}
	}
	return node, nil
}

func restoreApplicationState(fsm *stateMachine, logs *boltStore, snapshots raft.SnapshotStore) error {
	metadata, err := snapshots.List()
	if err != nil {
		return fmt.Errorf("list snapshots: %w", err)
	}
	sort.Slice(metadata, func(i, j int) bool { return metadata[i].Index > metadata[j].Index })
	var snapshotIndex uint64
	if len(metadata) > 0 {
		restored := false
		var restoreErr error
		for _, snapshot := range metadata {
			opened, reader, err := snapshots.Open(snapshot.ID)
			if err != nil {
				restoreErr = fmt.Errorf("open snapshot %s: %w", snapshot.ID, err)
				continue
			}
			if err := fsm.Restore(reader); err != nil {
				restoreErr = fmt.Errorf("restore snapshot %s: %w", snapshot.ID, err)
				continue
			}
			snapshotIndex = opened.Index
			restored = true
			break
		}
		if !restored {
			return fmt.Errorf("no persisted state snapshot could be restored: %w", restoreErr)
		}
	}
	appliedIndex, err := logs.GetAppliedIndex()
	if err != nil {
		return fmt.Errorf("read applied log index: %w", err)
	}
	lastIndex, err := logs.LastIndex()
	if err != nil {
		return fmt.Errorf("read last log index: %w", err)
	}
	if appliedIndex > lastIndex {
		appliedIndex = lastIndex
	}
	for index := snapshotIndex + 1; index <= appliedIndex; index++ {
		var entry raft.Log
		if err := logs.GetLog(index, &entry); err != nil {
			return fmt.Errorf("read committed log %d: %w", index, err)
		}
		fsm.Apply(&entry)
	}
	return nil
}

func validateConfig(cfg Config) error {
	if validateIdentifier("node ID", cfg.ID) != nil ||
		validateIdentifier("Raft address", cfg.Address) != nil ||
		strings.TrimSpace(cfg.DataDir) == "" ||
		cfg.Transport == nil ||
		string(cfg.Transport.LocalAddr()) != cfg.Address {
		return ErrInvalidRaftConfig
	}
	if cfg.ClusterID != "" && validateIdentifier("cluster ID", cfg.ClusterID) != nil {
		return ErrInvalidRaftConfig
	}
	if cfg.HeartbeatTimeout < 0 || cfg.ElectionTimeout < 0 || cfg.LeaderLeaseTimeout < 0 ||
		cfg.CommitTimeout < 0 || cfg.EnqueueTimeout < 0 || cfg.MembershipTimeout < 0 ||
		cfg.SnapshotInterval < 0 {
		return ErrInvalidRaftConfig
	}
	return nil
}

func durationOr(value, fallback time.Duration) time.Duration {
	if value == 0 {
		return fallback
	}
	return value
}

func uintOr(value, fallback uint64) uint64 {
	if value == 0 {
		return fallback
	}
	return value
}

func (n *Node) ID() string        { return string(n.id) }
func (n *Node) ClusterID() string { return n.clusterID }

// IsLeader is a state hint only. Mutations must use Apply or a membership
// method, which require the Raft quorum to commit the operation.
func (n *Node) IsLeader() bool { return n.raft.State() == raft.Leader }

// Leader returns the latest leader hint, which can be empty or stale during a
// partition. It is suitable for routing retries, not for authorizing writes.
func (n *Node) Leader() (id, address string) {
	leaderAddress, leaderID := n.raft.LeaderWithID()
	return string(leaderID), string(leaderAddress)
}

func (n *Node) WaitForLeader(ctx context.Context) (id, address string, err error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		id, address = n.Leader()
		if id != "" && address != "" {
			return id, address, nil
		}
		select {
		case <-ctx.Done():
			return "", "", ctx.Err()
		case <-ticker.C:
		}
	}
}

// VerifyLeader confirms that this node still has quorum-backed leadership.
func (n *Node) VerifyLeader(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !n.IsLeader() {
		return ErrNotLeader
	}
	return waitFuture(ctx, n.raft.VerifyLeader(), false)
}

// Apply commits a mutation to a voter majority before it changes the FSM.
// If ctx expires after submission, the command may still commit; callers must
// retry with the same operation ID and payload.
func (n *Node) Apply(ctx context.Context, mutation Mutation) (MutationResult, error) {
	if err := ctx.Err(); err != nil {
		return MutationResult{}, err
	}
	if err := n.fsm.StorageError(); err != nil {
		return MutationResult{}, err
	}
	if err := validateMutation(mutation); err != nil {
		return MutationResult{}, err
	}
	raw, err := json.Marshal(mutation)
	if err != nil {
		return MutationResult{}, fmt.Errorf("consensus: encode mutation: %w", err)
	}
	timeout := boundedTimeout(ctx, n.enqueueTimeout)
	future := n.raft.Apply(raw, timeout)
	if err := waitFuture(ctx, future, true); err != nil {
		return MutationResult{}, err
	}
	response, ok := future.Response().(fsmResponse)
	if !ok {
		return MutationResult{}, errors.New("consensus: Raft returned an invalid mutation response")
	}
	if response.Error != "" {
		if response.Error == ErrOperationIDReuse.Error() {
			return MutationResult{}, ErrOperationIDReuse
		}
		return MutationResult{}, errors.New(response.Error)
	}
	return response.Result, nil
}

// Read performs a quorum-verified read and returns a copy of the value.
func (n *Node) Read(ctx context.Context, key string) ([]byte, bool, error) {
	if err := validateIdentifier("mutation key", key); err != nil {
		return nil, false, err
	}
	if err := n.VerifyLeader(ctx); err != nil {
		return nil, false, err
	}
	if err := n.fsm.StorageError(); err != nil {
		return nil, false, err
	}
	value, ok := n.fsm.Read(key)
	return value, ok, nil
}

// AddVoter dynamically adds a paired node to the voter configuration. The
// new node must already be listening at address and use this cluster ID.
func (n *Node) AddVoter(ctx context.Context, id, address string) error {
	if err := validateIdentifier("node ID", id); err != nil {
		return err
	}
	if err := validateIdentifier("Raft address", address); err != nil {
		return err
	}
	if err := n.VerifyLeader(ctx); err != nil {
		return err
	}
	configurationFuture := n.raft.GetConfiguration()
	if err := waitFuture(ctx, configurationFuture, false); err != nil {
		return err
	}
	configuration := configurationFuture.Configuration()
	for _, server := range configuration.Servers {
		if server.ID == raft.ServerID(id) && server.Address == raft.ServerAddress(address) && server.Suffrage == raft.Voter {
			return nil
		}
	}
	future := n.raft.AddVoter(
		raft.ServerID(id),
		raft.ServerAddress(address),
		configurationFuture.Index(),
		boundedTimeout(ctx, n.membershipTimeout),
	)
	return waitFuture(ctx, future, true)
}

// RemoveVoter removes a paired member from the voter configuration. The last
// remaining voter is protected so the cluster cannot become unrecoverable.
func (n *Node) RemoveVoter(ctx context.Context, id string) error {
	if err := validateIdentifier("node ID", id); err != nil {
		return err
	}
	if err := n.VerifyLeader(ctx); err != nil {
		return err
	}
	configurationFuture := n.raft.GetConfiguration()
	if err := waitFuture(ctx, configurationFuture, false); err != nil {
		return err
	}
	configuration := configurationFuture.Configuration()
	voters := 0
	found := false
	for _, server := range configuration.Servers {
		if server.Suffrage == raft.Voter {
			voters++
		}
		if server.ID == raft.ServerID(id) {
			found = true
		}
	}
	if !found {
		return ErrMemberNotFound
	}
	if voters <= 1 {
		return ErrLastVoter
	}
	future := n.raft.RemoveServer(
		raft.ServerID(id),
		configurationFuture.Index(),
		boundedTimeout(ctx, n.membershipTimeout),
	)
	return waitFuture(ctx, future, true)
}

func (n *Node) Configuration(ctx context.Context) (raft.Configuration, error) {
	if err := n.VerifyLeader(ctx); err != nil {
		return raft.Configuration{}, err
	}
	future := n.raft.GetConfiguration()
	if err := waitFuture(ctx, future, false); err != nil {
		return raft.Configuration{}, err
	}
	configuration := future.Configuration()
	return configuration.Clone(), nil
}

func waitFuture(ctx context.Context, future raft.Future, outcomeUnknown bool) error {
	if err := ctx.Err(); err != nil {
		if outcomeUnknown {
			return fmt.Errorf("%w: %w", ErrOutcomeUnknown, err)
		}
		return err
	}
	done := make(chan error, 1)
	go func() { done <- future.Error() }()
	select {
	case err := <-done:
		if errors.Is(err, raft.ErrNotLeader) {
			return errors.Join(ErrNotLeader, err)
		}
		return err
	case <-ctx.Done():
		if outcomeUnknown {
			return fmt.Errorf("%w: %w", ErrOutcomeUnknown, ctx.Err())
		}
		return ctx.Err()
	}
}

func boundedTimeout(ctx context.Context, fallback time.Duration) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining > 0 && remaining < fallback {
			return remaining
		}
	}
	return fallback
}

func closeTransport(transport raft.Transport) error {
	closer, ok := transport.(interface{ Close() error })
	if !ok {
		return nil
	}
	return closer.Close()
}

func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		n.closeErr = errors.Join(n.raft.Shutdown().Error(), closeTransport(n.transport), n.store.Close())
	})
	return n.closeErr
}
