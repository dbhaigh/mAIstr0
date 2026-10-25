package orchestrator

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/shard"
)

type ShardMembershipRequest struct {
	NodeIDs []string `json:"node_ids"`
}

// Reconfigure drains the existing group before replacing its placement.
// Failed reloads retain the requested membership for an explicit Start retry.
func (m *ShardManager) Reconfigure(ctx context.Context, id string, request ShardMembershipRequest) (ShardGroup, error) {
	if err := m.beginOperation(id); err != nil {
		return ShardGroup{}, err
	}
	defer m.finishOperation(id)
	group, _ := m.Get(id)
	if group.State != ShardGroupReady && group.State != ShardGroupStopped && group.State != ShardGroupFailed && group.State != ShardGroupDegraded {
		return group, fmt.Errorf("%w: group cannot change members while %s", errShardGroupConflict, group.State)
	}
	if len(request.NodeIDs) == 0 {
		return group, fmt.Errorf("%w: select at least one member node", errShardGroupInvalid)
	}
	selected := make(map[string]bool, len(request.NodeIDs))
	for _, id := range request.NodeIDs {
		if strings.TrimSpace(id) == "" || selected[id] {
			return group, fmt.Errorf("%w: node IDs must be nonempty and unique", errShardGroupInvalid)
		}
		selected[id] = true
		node, ok := m.registry.Get(id)
		if !ok || !node.Healthy {
			return group, fmt.Errorf("%w: selected node %q is offline or not registered", errShardGroupUnavailable, id)
		}
	}
	create := CreateShardGroupRequest{Model: group.Model, Mode: group.Mode, PinnedNodes: slices.Clone(request.NodeIDs)}
	create.requireAllNodes = true
	for _, member := range group.Members {
		if member.Role == "head" && group.Mode == shard.ModeLlamaCPPRPC {
			if !selected[member.NodeID] {
				return group, fmt.Errorf("%w: retain model-head node %q; moving model weights to another head is not supported by membership editing", errShardGroupInvalid, member.NodeID)
			}
			create.HeadNodeID, create.HeadEngineID = member.NodeID, member.EngineID
		}
	}
	// Validate runtime availability and engine ownership before disrupting the
	// running group. Memory placement is recomputed after releasing old weights.
	candidates, engines, err := m.candidates(ctx, create)
	if err != nil {
		return group, err
	}
	eligible := make(map[string]bool, len(candidates))
	for _, candidate := range candidates {
		eligible[candidate.ID] = true
	}
	for _, id := range request.NodeIDs {
		if !eligible[id] {
			return group, fmt.Errorf("%w: node %q lacks the required managed worker", errShardGroupInvalid, id)
		}
	}
	m.mu.RLock()
	for otherID, other := range m.groups {
		if otherID == id || other.State == ShardGroupStopped {
			continue
		}
		for _, member := range other.Members {
			if !selected[member.NodeID] || (other.State == ShardGroupFailed && !member.Started) {
				continue
			}
			for _, status := range engines[member.NodeID] {
				if member.EngineID == status.ID {
					m.mu.RUnlock()
					return group, fmt.Errorf("%w: node %q engine %q belongs to group %s", errShardGroupConflict, member.NodeID, member.EngineID, otherID)
				}
			}
		}
	}
	m.mu.RUnlock()
	if _, err := m.stopGroup(ctx, id); err != nil {
		current, _ := m.Get(id)
		return current, err
	}
	if err := m.mutate(id, func(current *ShardGroup) {
		current.PinnedNodes = slices.Clone(request.NodeIDs)
		current.State, current.Error, current.UpdatedAt = ShardGroupPlanning, "", time.Now().UTC()
	}); err != nil {
		current, _ := m.Get(id)
		return current, err
	}
	create.nodeSnapshots = make(map[string]cluster.NodeStatus, len(request.NodeIDs))
	for _, nodeID := range request.NodeIDs {
		node, ok := m.registry.Get(nodeID)
		if !ok {
			return m.failCreate(id, fmt.Errorf("%w: node %s disappeared during reload", errShardGroupUnavailable, nodeID))
		}
		fresh, err := m.engines.NodeStatus(ctx, node.Address)
		if err != nil {
			return m.failCreate(id, fmt.Errorf("refresh placement telemetry for %s: %w", nodeID, err))
		}
		if fresh.ID != nodeID {
			return m.failCreate(id, fmt.Errorf("%w: placement status identity mismatch for %s", errShardGroupUnavailable, nodeID))
		}
		fresh.Address = node.Address
		fresh.Healthy = true
		create.nodeSnapshots[nodeID] = fresh
	}
	return m.startGroup(ctx, id, create)
}
