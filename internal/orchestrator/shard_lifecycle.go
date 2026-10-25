package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/shard"
)

// Stop retains the group configuration and waits for active inference before
// stopping the head and then its workers.
func (m *ShardManager) Stop(ctx context.Context, id string) (ShardGroup, error) {
	if err := m.beginOperation(id); err != nil {
		return ShardGroup{}, err
	}
	defer m.finishOperation(id)
	return m.stopGroup(ctx, id)
}

func (m *ShardManager) stopGroup(ctx context.Context, id string) (ShardGroup, error) {
	group, _ := m.Get(id)
	if group.State == ShardGroupStopped {
		return group, nil
	}
	if err := m.mutate(id, func(current *ShardGroup) { current.State = ShardGroupStopping }); err != nil {
		return group, err
	}
	if err := m.waitForRequestIdle(ctx, id); err != nil {
		// No process has been stopped yet; restore the previous availability.
		restoreErr := m.mutate(id, func(current *ShardGroup) {
			current.State = group.State
			current.Error = group.Error
		})
		result, _ := m.Get(id)
		return result, errors.Join(fmt.Errorf("wait for shard group requests: %w", err), restoreErr)
	}
	stopErr := m.stopMembers(ctx, group, false)
	if stopErr == nil {
		m.releaseReservation(id)
	}
	persistErr := m.mutate(id, func(current *ShardGroup) {
		if stopErr != nil {
			current.State = ShardGroupFailed
			current.Error = "stop failed: " + stopErr.Error()
		} else {
			current.State = ShardGroupStopped
			current.Error = ""
		}
	})
	result, _ := m.Get(id)
	return result, errors.Join(stopErr, persistErr)
}

func (m *ShardManager) Start(ctx context.Context, id string) (ShardGroup, error) {
	return m.startOrRestart(ctx, id, false)
}

func (m *ShardManager) Restart(ctx context.Context, id string) (ShardGroup, error) {
	return m.startOrRestart(ctx, id, true)
}

func (m *ShardManager) startOrRestart(ctx context.Context, id string, restart bool) (ShardGroup, error) {
	if err := m.beginOperation(id); err != nil {
		return ShardGroup{}, err
	}
	defer m.finishOperation(id)
	group, _ := m.Get(id)
	if !restart && group.State == ShardGroupReady {
		return group, nil
	}
	if !restart && group.State != ShardGroupStopped && group.State != ShardGroupFailed {
		return group, fmt.Errorf("%w: start requires a stopped or failed group", errShardGroupConflict)
	}
	if group.State != ShardGroupStopped {
		var err error
		group, err = m.stopGroup(ctx, id)
		if err != nil {
			return group, err
		}
	}
	request := CreateShardGroupRequest{
		Model: group.Model, Mode: group.Mode, PinnedNodes: group.PinnedNodes,
	}
	if group.Mode == shard.ModeLlamaCPPRPC {
		for _, member := range group.Members {
			if member.Role == "head" {
				request.HeadNodeID, request.HeadEngineID = member.NodeID, member.EngineID
				break
			}
		}
	}
	m.mu.Lock()
	for otherID, other := range m.groups {
		if otherID != id && strings.EqualFold(other.Model.Name, group.Model.Name) &&
			(other.State != ShardGroupFailed || m.operations[otherID]) {
			m.mu.Unlock()
			return group, fmt.Errorf("%w: another group owns model %q; delete the other group first", errShardGroupConflict, group.Model.Name)
		}
	}
	current := m.groups[id]
	current.State, current.Error, current.UpdatedAt = ShardGroupPlanning, "", time.Now().UTC()
	m.groups[id] = current
	m.mu.Unlock()
	if err := m.persistCurrent(id); err != nil {
		return m.failCreate(id, err)
	}
	return m.startGroup(ctx, id, request)
}

func (m *ShardManager) claimMembers(id string, plan shard.ShardPlan, members []ShardGroupMember) error {
	m.mu.Lock()
	for otherID, other := range m.groups {
		if otherID == id || other.State == ShardGroupStopped {
			continue
		}
		for _, existing := range other.Members {
			if other.State == ShardGroupFailed && !existing.Started {
				continue
			}
			for _, member := range members {
				if member.NodeID == existing.NodeID && member.EngineID == existing.EngineID {
					m.mu.Unlock()
					return fmt.Errorf("%w: engine %s on node %s belongs to group %s", errShardGroupConflict, member.EngineID, member.NodeID, otherID)
				}
			}
		}
	}
	group := m.groups[id]
	group.Plan, group.Members = plan, members
	group.Rationale = append([]string(nil), plan.Rationale...)
	group.Error, group.State, group.UpdatedAt = "", ShardGroupStartingWorkers, time.Now().UTC()
	m.groups[id] = cloneShardGroup(group)
	m.mu.Unlock()
	return m.persistCurrent(id)
}
