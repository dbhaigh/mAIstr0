package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/maistr0/maistr0/internal/nodeagent/supervisor"
)

func (m *ShardManager) Recover(ctx context.Context) error {
	var result error
	for _, group := range m.List() {
		m.mu.RLock()
		previousState := m.recovery[group.ID]
		m.mu.RUnlock()
		if previousState == ShardGroupFailed || group.State == ShardGroupFailed || group.State == ShardGroupStopped {
			m.mu.Lock()
			m.verified[group.ID] = true
			delete(m.recovery, group.ID)
			m.mu.Unlock()
			continue
		}
		if previousState == ShardGroupStopping {
			if err := m.stopMembers(ctx, group, false); err != nil {
				if mutateErr := m.mutate(group.ID, func(current *ShardGroup) {
					current.State = ShardGroupFailed
					current.Error = "startup cleanup failed: " + err.Error()
				}); mutateErr != nil {
					return errors.Join(err, mutateErr)
				}
				continue
			}
			m.releaseReservation(group.ID)
			if err := m.mutate(group.ID, func(current *ShardGroup) {
				current.State = ShardGroupStopped
				current.Error = ""
			}); err != nil {
				return fmt.Errorf("finish stopping shard group %s: %w", group.ID, err)
			}
			m.mu.Lock()
			delete(m.recovery, group.ID)
			m.mu.Unlock()
			continue
		}
		ready, verifyErr := m.verifyGroup(ctx, group)
		if ready && verifyErr == nil {
			reservation, reserveErr := m.capacity.ReserveShardNodes(ctx, capacityTargets(group.Members))
			if reserveErr == nil {
				m.mu.Lock()
				m.reservations[group.ID] = reservation
				m.mu.Unlock()
				if mutateErr := m.mutate(group.ID, func(current *ShardGroup) {
					current.State = ShardGroupReady
					current.Error = ""
				}); mutateErr != nil {
					reservation.Release()
					m.releaseReservation(group.ID)
					return mutateErr
				}
				m.mu.Lock()
				m.verified[group.ID] = true
				delete(m.recovery, group.ID)
				m.mu.Unlock()
				continue
			}
			verifyErr = fmt.Errorf("reserve recovered member capacity: %w", reserveErr)
		}
		var err error
		if verifyErr != nil {
			err = fmt.Errorf("verify managed engines: %w", verifyErr)
		} else {
			err = errors.New("one or more managed engines are not ready")
		}
		stopErr := m.stopMembers(ctx, group, true)
		m.releaseReservation(group.ID)
		if stopErr != nil {
			err = errors.Join(err, stopErr)
		}
		if mutateErr := m.mutate(group.ID, func(current *ShardGroup) {
			current.State = ShardGroupFailed
			current.Error = "startup verification failed: " + err.Error()
		}); mutateErr != nil {
			return errors.Join(err, mutateErr)
		}
		m.mu.Lock()
		m.verified[group.ID] = true
		delete(m.recovery, group.ID)
		m.mu.Unlock()
		result = errors.Join(result, fmt.Errorf("verify shard group %s: %w", group.ID, err))
	}
	return result
}

// CheckHealth makes a member loss visible as degraded first. The next sweep
// tears down the group and marks it failed, allowing clients to observe the
// unavailable state without treating a lost worker as a ready model.
func (m *ShardManager) CheckHealth(ctx context.Context) error {
	var result error
	for _, group := range m.List() {
		m.mu.RLock()
		verified := m.verified[group.ID]
		m.mu.RUnlock()
		if !verified {
			continue
		}
		switch group.State {
		case ShardGroupReady:
			if err := m.beginOperation(group.ID); err != nil {
				continue
			}
			group, _ = m.Get(group.ID)
			if group.State != ShardGroupReady {
				m.finishOperation(group.ID)
				continue
			}
			ready, checkErr := m.verifyGroup(ctx, group)
			if checkErr != nil {
				result = errors.Join(result, checkErr)
			}
			if !ready {
				reason := "a shard group member is offline or its managed engine is not ready"
				if checkErr != nil {
					reason = "a shard group member readiness check failed"
				}
				if err := m.mutate(group.ID, func(current *ShardGroup) {
					current.State = ShardGroupDegraded
					current.Error = reason
				}); err != nil {
					result = errors.Join(result, err)
				}
			}
			m.finishOperation(group.ID)
		case ShardGroupDegraded:
			if err := m.beginOperation(group.ID); err != nil {
				continue
			}
			group, _ = m.Get(group.ID)
			if group.State != ShardGroupDegraded {
				m.finishOperation(group.ID)
				continue
			}
			if err := m.waitForRequestIdle(ctx, group.ID); err != nil {
				result = errors.Join(result, err)
				m.finishOperation(group.ID)
				continue
			}
			stopErr := m.stopMembers(ctx, group, false)
			m.releaseReservation(group.ID)
			mutateErr := m.completeOperation(group.ID, func(current *ShardGroup) {
				current.State = ShardGroupFailed
				if stopErr != nil {
					current.Error = "member loss teardown failed: " + stopErr.Error()
				}
			})
			result = errors.Join(result, stopErr, mutateErr)
		}
	}
	return result
}

func (m *ShardManager) verifyGroup(ctx context.Context, group ShardGroup) (bool, error) {
	if len(group.Members) == 0 || !m.membersHealthy(group.Members) {
		return false, nil
	}
	statuses := make(map[string]map[string]supervisor.Status)
	for _, member := range group.Members {
		if statuses[member.NodeID] == nil {
			list, err := m.engines.List(ctx, member.Address)
			if err != nil {
				return false, err
			}
			statuses[member.NodeID] = make(map[string]supervisor.Status, len(list))
			for _, status := range list {
				statuses[member.NodeID][status.ID] = status
			}
		}
		status, exists := statuses[member.NodeID][member.EngineID]
		if !exists || !strings.EqualFold(status.State, "ready") {
			return false, nil
		}
		if member.Role == "head" {
			if err := m.engines.VerifyModel(ctx, member.Address, member.EngineID, group.Model, false); err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

func (m *ShardManager) membersHealthy(members []ShardGroupMember) bool {
	for _, member := range members {
		node, exists := m.registry.Get(member.NodeID)
		if !exists || !node.Healthy || node.Address != member.Address {
			return false
		}
	}
	return len(members) > 0
}
