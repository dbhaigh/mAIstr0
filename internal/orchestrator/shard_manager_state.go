package orchestrator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/shard"
)

func (m *ShardManager) AcquireRequest(ctx context.Context, id string) (func(), error) {
	m.mu.RLock()
	group, exists := m.groups[id]
	gate := m.requestGates[id]
	m.mu.RUnlock()
	if !exists {
		return nil, errShardGroupNotFound
	}
	if group.State != ShardGroupReady || gate == nil {
		return nil, errShardGroupUnavailable
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-gate:
	}
	m.mu.RLock()
	group, exists = m.groups[id]
	stillReady := exists && group.State == ShardGroupReady && m.requestGates[id] == gate
	m.mu.RUnlock()
	if !stillReady {
		gate <- struct{}{}
		return nil, errShardGroupUnavailable
	}
	var once sync.Once
	return func() {
		once.Do(func() { gate <- struct{}{} })
	}, nil
}

func (m *ShardManager) waitForRequestIdle(ctx context.Context, id string) error {
	m.mu.RLock()
	gate := m.requestGates[id]
	m.mu.RUnlock()
	if gate == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-gate:
		gate <- struct{}{}
		return nil
	}
}

func (m *ShardManager) mutate(id string, change func(*ShardGroup)) error {
	m.mu.Lock()
	group, ok := m.groups[id]
	if !ok {
		m.mu.Unlock()
		return errShardGroupNotFound
	}
	change(&group)
	group.UpdatedAt = time.Now().UTC()
	m.groups[id] = cloneShardGroup(group)
	m.mu.Unlock()
	return m.persistCurrent(id)
}

func (m *ShardManager) persistCurrent(id string) error {
	m.persistMu.Lock()
	defer m.persistMu.Unlock()
	m.mu.RLock()
	group, ok := m.groups[id]
	m.mu.RUnlock()
	if !ok {
		return nil
	}
	payload, err := json.Marshal(group)
	if err != nil {
		return fmt.Errorf("encode shard group %s: %w", id, err)
	}
	if err := m.store.SaveShardGroup(id, payload); err != nil {
		return fmt.Errorf("persist shard group %s: %w", id, err)
	}
	return nil
}

func (m *ShardManager) beginOperation(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.groups[id]; !ok {
		return errShardGroupNotFound
	}
	if m.operations[id] {
		return errShardGroupOperation
	}
	m.operations[id] = true
	return nil
}

func (m *ShardManager) finishOperation(id string) {
	m.mu.Lock()
	delete(m.operations, id)
	m.mu.Unlock()
}

// completeOperation publishes the terminal state and releases its operation
// together, after the persistence attempt. Observers can act on it immediately.
func (m *ShardManager) completeOperation(id string, change func(*ShardGroup)) error {
	m.persistMu.Lock()
	defer m.persistMu.Unlock()
	m.mu.RLock()
	group, ok := m.groups[id]
	group = cloneShardGroup(group)
	m.mu.RUnlock()
	if !ok {
		return errShardGroupNotFound
	}
	change(&group)
	group.UpdatedAt = time.Now().UTC()
	payload, err := json.Marshal(group)
	if err != nil {
		err = fmt.Errorf("encode shard group %s: %w", id, err)
	} else if saveErr := m.store.SaveShardGroup(id, payload); saveErr != nil {
		err = fmt.Errorf("persist shard group %s: %w", id, saveErr)
	}
	m.mu.Lock()
	m.groups[id] = cloneShardGroup(group)
	delete(m.operations, id)
	m.mu.Unlock()
	return err
}

func (m *ShardManager) releaseReservation(id string) {
	m.mu.Lock()
	reservation := m.reservations[id]
	delete(m.reservations, id)
	delete(m.verified, id)
	m.mu.Unlock()
	if reservation != nil {
		reservation.Release()
	}
}

func (m *ShardManager) removeInMemory(id string) {
	m.mu.Lock()
	delete(m.groups, id)
	delete(m.verified, id)
	delete(m.recovery, id)
	delete(m.operations, id)
	delete(m.requestGates, id)
	m.mu.Unlock()
}

func newShardRequestGate() chan struct{} {
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	return gate
}

func newShardGroupID() (string, error) {
	value := make([]byte, 12)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return "shard-" + hex.EncodeToString(value), nil
}

func cloneShardGroup(group ShardGroup) ShardGroup {
	group.PinnedNodes = append([]string(nil), group.PinnedNodes...)
	group.Rationale = append([]string(nil), group.Rationale...)
	group.Members = append([]ShardGroupMember(nil), group.Members...)
	group.Plan.Nodes = append([]shard.NodePlan(nil), group.Plan.Nodes...)
	for i := range group.Plan.Nodes {
		group.Plan.Nodes[i].Devices = append([]shard.DeviceAllocation(nil), group.Plan.Nodes[i].Devices...)
	}
	group.Plan.Rationale = append([]string(nil), group.Plan.Rationale...)
	return group
}

var errShardGroupNotFound = errors.New("shard group not found")
