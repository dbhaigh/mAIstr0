package consensus

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestSingleNodeMutationSurvivesRestart(t *testing.T) {
	directory := t.TempDir()
	transportAddress := raft.ServerAddress("single")
	_, transport := raft.NewInmemTransport(transportAddress)
	node, err := Open(testConfig("single", transportAddress, filepath.Join(directory, "raft"), transport, true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := node.Close(); err != nil {
			t.Errorf("close original node: %v", err)
		}
	})
	waitForNodeLeader(t, node)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	result, err := node.Apply(ctx, Mutation{OperationID: "task-create-1", Kind: MutationPut, Key: "task/1", Value: []byte("pending")})
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if result.Index == 0 || result.Duplicate {
		t.Fatalf("mutation result = %+v", result)
	}
	retryCtx, retryCancel := context.WithTimeout(context.Background(), 3*time.Second)
	retry, err := node.Apply(retryCtx, Mutation{OperationID: "task-create-1", Kind: MutationPut, Key: "task/1", Value: []byte("pending")})
	retryCancel()
	if err != nil {
		t.Fatal(err)
	}
	if !retry.Duplicate || retry.Index != result.Index {
		t.Fatalf("retry result = %+v, want duplicate of %+v", retry, result)
	}
	appliedIndex, err := node.store.GetAppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	lastIndex, err := node.store.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if appliedIndex < result.Index || appliedIndex > lastIndex {
		t.Fatalf("persisted applied index %d is outside mutation/log range %d..%d", appliedIndex, result.Index, lastIndex)
	}
	if err := node.Close(); err != nil {
		t.Fatal(err)
	}
	recoveryStore, err := openBoltStore(filepath.Join(directory, "raft", "raft.db"))
	if err != nil {
		t.Fatal(err)
	}
	recoveredAppliedIndex, err := recoveryStore.GetAppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	recoveredLastIndex, err := recoveryStore.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if recoveredAppliedIndex < result.Index || recoveredAppliedIndex > recoveredLastIndex {
		t.Fatalf("reopened store applied index %d is outside mutation/log range %d..%d", recoveredAppliedIndex, result.Index, recoveredLastIndex)
	}
	if err := recoveryStore.Close(); err != nil {
		t.Fatal(err)
	}

	_, restartedTransport := raft.NewInmemTransport(transportAddress)
	restarted, err := Open(testConfig("single", transportAddress, filepath.Join(directory, "raft"), restartedTransport, true))
	if err != nil {
		t.Fatal(err)
	}
	restartedAppliedIndex, err := restarted.store.GetAppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	restartedLastIndex, err := restarted.store.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("restarted Raft log: applied=%d last=%d", restartedAppliedIndex, restartedLastIndex)
	t.Cleanup(func() {
		if err := restarted.Close(); err != nil {
			t.Errorf("close restarted node: %v", err)
		}
	})
	waitForNodeLeader(t, restarted)
	readCtx, readCancel := context.WithTimeout(context.Background(), 3*time.Second)
	value, ok, err := restarted.Read(readCtx, "task/1")
	readCancel()
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "pending" {
		t.Fatalf("restarted value = %q, present %t", value, ok)
	}
	if err := restarted.raft.Snapshot().Error(); err != nil {
		t.Fatalf("snapshot restored node state: %v", err)
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}

	_, snapshotTransport := raft.NewInmemTransport(transportAddress)
	snapshotRestored, err := Open(testConfig("single", transportAddress, filepath.Join(directory, "raft"), snapshotTransport, true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := snapshotRestored.Close(); err != nil {
			t.Errorf("close snapshot-restored node: %v", err)
		}
	})
	waitForNodeLeader(t, snapshotRestored)
	snapshotReadCtx, snapshotReadCancel := context.WithTimeout(context.Background(), 3*time.Second)
	snapshotValue, snapshotOK, err := snapshotRestored.Read(snapshotReadCtx, "task/1")
	snapshotReadCancel()
	if err != nil {
		t.Fatal(err)
	}
	if !snapshotOK || string(snapshotValue) != "pending" {
		t.Fatalf("snapshot-restored value = %q, present %t", snapshotValue, snapshotOK)
	}
}

func TestDynamicVoterMembershipAndLastVoterProtection(t *testing.T) {
	nodes, _, addresses := startTestCluster(t, 3)
	leader := waitForClusterLeader(t, nodes)
	for index := 1; index < len(nodes); index++ {
		if err := leader.AddVoter(context.Background(), nodes[index].ID(), string(addresses[index])); err != nil {
			t.Fatalf("add voter %s: %v", nodes[index].ID(), err)
		}
	}
	leader = waitForClusterLeader(t, nodes)
	waitForVoterCount(t, leader, 3)

	removedID := nodes[2].ID()
	if err := leader.RemoveVoter(context.Background(), removedID); err != nil {
		t.Fatalf("remove voter %s: %v", removedID, err)
	}
	leader = waitForClusterLeader(t, nodes)
	waitForVoterCount(t, leader, 2)
	configuration, err := leader.Configuration(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var secondID string
	for _, server := range configuration.Servers {
		if server.ID != raft.ServerID(leader.ID()) {
			secondID = string(server.ID)
		}
	}
	if secondID == "" {
		t.Fatal("two-voter configuration did not include a second member")
	}
	if err := leader.RemoveVoter(context.Background(), secondID); err != nil {
		t.Fatalf("remove second voter: %v", err)
	}
	leader = waitForClusterLeader(t, nodes)
	waitForVoterCount(t, leader, 1)
	if err := leader.RemoveVoter(context.Background(), leader.ID()); !errors.Is(err, ErrLastVoter) {
		t.Fatalf("removing last voter error = %v, want %v", err, ErrLastVoter)
	}
}

func TestPartitionedMinorityCannotCommitMutations(t *testing.T) {
	nodes, transports, addresses := startTestCluster(t, 3)
	leader := waitForClusterLeader(t, nodes)
	for index := 1; index < len(nodes); index++ {
		if err := leader.AddVoter(context.Background(), nodes[index].ID(), string(addresses[index])); err != nil {
			t.Fatalf("add voter %s: %v", nodes[index].ID(), err)
		}
	}
	leader = waitForClusterLeader(t, nodes)
	waitForVoterCount(t, leader, 3)
	minority := leader
	minorityIndex := indexOfNode(nodes, minority)
	if minorityIndex < 0 {
		t.Fatal("leader is not in the test cluster")
	}

	for index := range nodes {
		if index == minorityIndex {
			continue
		}
		transports[minorityIndex].Disconnect(addresses[index])
		transports[index].Disconnect(addresses[minorityIndex])
	}
	majority := make([]*Node, 0, 2)
	for index, node := range nodes {
		if index != minorityIndex {
			majority = append(majority, node)
		}
	}
	newLeader := waitForClusterLeader(t, majority)
	if newLeader.ID() == minority.ID() {
		t.Fatal("isolated voter retained leadership")
	}

	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	err := minority.VerifyLeader(verifyCtx)
	verifyCancel()
	if err == nil {
		t.Fatal("isolated minority still verified leadership")
	}
	applyCtx, applyCancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	_, err = minority.Apply(applyCtx, Mutation{
		OperationID: "minority-write",
		Kind:        MutationPut,
		Key:         "tasks/minority",
		Value:       []byte("must-not-commit"),
	})
	applyCancel()
	if err == nil {
		t.Fatal("isolated minority committed a mutation")
	}

	readCtx, readCancel := context.WithTimeout(context.Background(), 2*time.Second)
	value, found, err := newLeader.Read(readCtx, "tasks/minority")
	readCancel()
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatalf("minority mutation appeared in majority state: %q", value)
	}
}

func startTestCluster(t *testing.T, count int) ([]*Node, []*raft.InmemTransport, []raft.ServerAddress) {
	t.Helper()
	transports := make([]*raft.InmemTransport, count)
	addresses := make([]raft.ServerAddress, count)
	for index := range transports {
		address := raft.ServerAddress(fmt.Sprintf("node-%d", index))
		addresses[index], transports[index] = raft.NewInmemTransport(address)
	}
	for i := range transports {
		for j := range transports {
			if i != j {
				transports[i].Connect(addresses[j], transports[j])
			}
		}
	}

	nodes := make([]*Node, count)
	for index := range nodes {
		node, err := Open(testConfig(
			fmt.Sprintf("node-%d", index),
			addresses[index],
			filepath.Join(t.TempDir(), fmt.Sprintf("node-%d", index)),
			transports[index],
			index == 0,
		))
		if err != nil {
			t.Fatalf("open node %d: %v", index, err)
		}
		nodes[index] = node
	}
	t.Cleanup(func() {
		for index, node := range nodes {
			if node == nil {
				continue
			}
			if err := node.Close(); err != nil {
				t.Errorf("close node %d: %v", index, err)
			}
		}
	})
	return nodes, transports, addresses
}

func testConfig(id string, address raft.ServerAddress, dataDir string, transport raft.Transport, bootstrap bool) Config {
	return Config{
		ID:                 id,
		Address:            string(address),
		ClusterID:          "test-cluster",
		DataDir:            dataDir,
		Bootstrap:          bootstrap,
		Transport:          transport,
		HeartbeatTimeout:   200 * time.Millisecond,
		ElectionTimeout:    200 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
		CommitTimeout:      20 * time.Millisecond,
		EnqueueTimeout:     time.Second,
		MembershipTimeout:  2 * time.Second,
		SnapshotInterval:   5 * time.Second,
		SnapshotThreshold:  16,
		TrailingLogs:       8,
	}
}

func waitForNodeLeader(t *testing.T, node *Node) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		if node.IsLeader() {
			verifyCtx, verifyCancel := context.WithTimeout(ctx, 300*time.Millisecond)
			err := node.VerifyLeader(verifyCtx)
			verifyCancel()
			if err == nil {
				return
			}
		}
		select {
		case <-ctx.Done():
			leaderID, leaderAddress := node.Leader()
			t.Fatalf("node %s did not become a quorum-backed leader: %v (state %s, leader hint %s at %s)", node.ID(), ctx.Err(), node.raft.State(), leaderID, leaderAddress)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func waitForClusterLeader(t *testing.T, nodes []*Node) *Node {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		for _, node := range nodes {
			if !node.IsLeader() {
				continue
			}
			verifyCtx, verifyCancel := context.WithTimeout(ctx, 300*time.Millisecond)
			err := node.VerifyLeader(verifyCtx)
			verifyCancel()
			if err == nil {
				return node
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("no quorum-backed leader found: %v", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func waitForVoterCount(t *testing.T, leader *Node, count int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		configuration, err := leader.Configuration(ctx)
		if err == nil {
			voters := 0
			for _, server := range configuration.Servers {
				if server.Suffrage == raft.Voter {
					voters++
				}
			}
			if voters == count {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("voter count did not reach %d: %v", count, ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func indexOfNode(nodes []*Node, target *Node) int {
	for index, node := range nodes {
		if node == target {
			return index
		}
	}
	return -1
}

func TestMutationInputValidation(t *testing.T) {
	for _, mutation := range []Mutation{
		{OperationID: "", Kind: MutationPut, Key: "key"},
		{OperationID: "op", Kind: MutationPut, Key: ""},
		{OperationID: "op", Kind: "unknown", Key: "key"},
	} {
		if err := validateMutation(mutation); err == nil {
			t.Fatalf("invalid mutation passed validation: %+v", mutation)
		}
	}
	if !errors.Is(ErrLastVoter, ErrLastVoter) {
		t.Fatal("last-voter sentinel is not comparable")
	}
}
