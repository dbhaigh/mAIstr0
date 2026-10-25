package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/shard"
)

func (m *ShardManager) Create(ctx context.Context, request CreateShardGroupRequest) (ShardGroup, error) {
	if strings.TrimSpace(request.Model.Name) == "" {
		return ShardGroup{}, fmt.Errorf("%w: model.name is required", errShardGroupInvalid)
	}
	request.Model.Name = strings.TrimSpace(request.Model.Name)
	if (request.HeadNodeID == "") != (request.HeadEngineID == "") || (request.HeadNodeID != "" && request.Mode != shard.ModeLlamaCPPRPC) {
		return ShardGroup{}, fmt.Errorf("%w: head_node_id and head_engine_id must be supplied together for llama.cpp RPC", errShardGroupInvalid)
	}
	if request.HeadNodeID != "" && len(request.PinnedNodes) > 0 && !slices.Contains(request.PinnedNodes, request.HeadNodeID) {
		return ShardGroup{}, fmt.Errorf("%w: selected head must be one of the pinned nodes", errShardGroupInvalid)
	}
	if len(request.PinnedNodes) > 0 {
		seen := make(map[string]struct{}, len(request.PinnedNodes))
		for _, nodeID := range request.PinnedNodes {
			if strings.TrimSpace(nodeID) == "" {
				return ShardGroup{}, fmt.Errorf("%w: pinned node IDs must not be empty", errShardGroupInvalid)
			}
			if _, exists := seen[nodeID]; exists {
				return ShardGroup{}, fmt.Errorf("%w: pinned node %q is duplicated", errShardGroupInvalid, nodeID)
			}
			seen[nodeID] = struct{}{}
		}
	}
	if _, err := shard.Plan(request.Model, nil, shard.Policy{Mode: request.Mode}); err != nil {
		var refusal *shard.Refusal
		if !errors.As(err, &refusal) || refusal.Code != shard.RefusalNoCompatibleNodes {
			return ShardGroup{}, fmt.Errorf("%w: %v", errShardGroupInvalid, err)
		}
	}
	id, err := newShardGroupID()
	if err != nil {
		return ShardGroup{}, fmt.Errorf("generate shard group ID: %w", err)
	}
	now := time.Now().UTC()
	group := ShardGroup{
		ID: id, Model: request.Model, Mode: request.Mode, State: ShardGroupPlanning,
		Plan:        shard.ShardPlan{Mode: request.Mode, Model: request.Model, Nodes: []shard.NodePlan{}, Rationale: []string{}},
		Members:     []ShardGroupMember{},
		PinnedNodes: append([]string(nil), request.PinnedNodes...),
		Rationale:   []string{"planning against the current healthy node snapshot"},
		CreatedAt:   now, UpdatedAt: now,
	}
	m.mu.Lock()
	for _, existing := range m.groups {
		if strings.EqualFold(existing.Model.Name, group.Model.Name) && (existing.State != ShardGroupFailed || m.operations[existing.ID]) {
			m.mu.Unlock()
			return ShardGroup{}, fmt.Errorf("%w: a shard group for model %q already exists", errShardGroupConflict, group.Model.Name)
		}
	}
	m.groups[id] = cloneShardGroup(group)
	m.operations[id] = true
	m.requestGates[id] = newShardRequestGate()
	m.mu.Unlock()
	defer m.finishOperation(id)
	if err := m.persistCurrent(id); err != nil {
		m.removeInMemory(id)
		return ShardGroup{}, fmt.Errorf("persist initial shard group: %w", err)
	}
	return m.startGroup(ctx, id, request)
}

func (m *ShardManager) startGroup(ctx context.Context, id string, request CreateShardGroupRequest) (ShardGroup, error) {
	candidates, engineByNode, err := m.candidates(ctx, request)
	if err != nil {
		return m.failCreate(id, err)
	}
	plan, err := m.planner(request.Model, candidates, shard.Policy{
		Mode: request.Mode, SafetyMarginMB: managedShardSafetyMarginMB,
	})
	if err != nil {
		return m.failCreate(id, err)
	}
	if request.requireAllNodes {
		for _, nodeID := range request.PinnedNodes {
			if !slices.ContainsFunc(plan.Nodes, func(node shard.NodePlan) bool { return node.NodeID == nodeID }) {
				return m.failCreate(id, fmt.Errorf("%w: selected member %q cannot receive layers; check free device memory and plan compatibility", errShardGroupUnavailable, nodeID))
			}
		}
	}
	networkWarnings, err := shardPlanNetworkWarnings(plan, candidates)
	if err != nil {
		return m.failCreate(id, err)
	}
	plan.Rationale = append(plan.Rationale, networkWarnings...)
	for _, rationale := range plan.Rationale {
		if strings.HasPrefix(rationale, "WARNING:") {
			logging.FileWarnf("shard", "%s", rationale)
		}
	}
	members, err := buildShardMembers(plan, engineByNode, m.registry, request.HeadNodeID)
	if err != nil {
		return m.failCreate(id, err)
	}
	if err := m.claimMembers(id, plan, members); err != nil {
		return m.failCreate(id, err)
	}

	reservation, err := m.capacity.ReserveShardNodes(ctx, capacityTargets(members))
	if err != nil {
		return m.failCreate(id, fmt.Errorf("%w: reserve member capacity: %v", errShardGroupUnavailable, err))
	}
	m.mu.Lock()
	m.reservations[id] = reservation
	m.mu.Unlock()
	for _, member := range members {
		if member.Role == "head" {
			if err := m.engines.VerifyModel(ctx, member.Address, member.EngineID, request.Model, true); err != nil {
				return m.failCreate(id, fmt.Errorf("verify saved model on head %s: %w", member.NodeID, err))
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return m.failCreate(id, err)
	}

	for index := range members {
		if index == len(members)-1 {
			if err := m.mutate(id, func(current *ShardGroup) { current.State = ShardGroupStartingHead }); err != nil {
				return m.failCreate(id, err)
			}
		}
		member := members[index]
		status, startErr := m.engines.Start(ctx, member.Address, member.EngineID, memberStartPlan(plan, member, members))
		if startErr != nil {
			return m.failCreate(id, fmt.Errorf("start %s engine %s on node %s: %w", member.Role, member.EngineID, member.NodeID, startErr))
		}
		if !strings.EqualFold(status.State, "ready") {
			return m.failCreate(id, fmt.Errorf("managed engine %s on node %s did not report ready", member.EngineID, member.NodeID))
		}
		if err := m.mutate(id, func(current *ShardGroup) {
			current.Members[index].Started = true
		}); err != nil {
			return m.failCreate(id, err)
		}
	}
	if err := m.mutate(id, func(current *ShardGroup) {
		current.State = ShardGroupReady
		current.Error = ""
	}); err != nil {
		return m.failCreate(id, err)
	}
	m.mu.Lock()
	m.verified[id] = true
	m.mu.Unlock()
	result, _ := m.Get(id)
	return result, nil
}

func (m *ShardManager) Delete(ctx context.Context, id string) error {
	if err := m.beginOperation(id); err != nil {
		return err
	}
	defer m.finishOperation(id)
	group, ok := m.Get(id)
	if !ok {
		return errShardGroupNotFound
	}
	if err := m.mutate(id, func(current *ShardGroup) { current.State = ShardGroupStopping }); err != nil {
		return err
	}
	if err := m.waitForRequestIdle(ctx, id); err != nil {
		return fmt.Errorf("wait for shard group requests to finish: %w", err)
	}
	stopErr := m.stopMembers(ctx, group, false)
	m.releaseReservation(id)
	if stopErr != nil {
		mutateErr := m.mutate(id, func(current *ShardGroup) {
			current.State = ShardGroupFailed
			current.Error = "teardown failed: " + stopErr.Error()
		})
		return errors.Join(fmt.Errorf("stop shard group %s: %w", id, stopErr), mutateErr)
	}
	m.persistMu.Lock()
	err := m.store.DeleteShardGroup(id)
	m.persistMu.Unlock()
	if err != nil {
		mutateErr := m.mutate(id, func(current *ShardGroup) {
			current.State = ShardGroupFailed
			current.Error = "delete persistence failed"
		})
		return errors.Join(fmt.Errorf("delete persisted shard group %s: %w", id, err), mutateErr)
	}
	m.removeInMemory(id)
	return nil
}

// Recover verifies every persisted member against the current node and supervisor
// state before restoring readiness and member-capacity reservations.
func (m *ShardManager) Close(ctx context.Context) error {
	var result error
	for _, group := range m.List() {
		if group.State == ShardGroupFailed || group.State == ShardGroupPlanning || group.State == ShardGroupStopped {
			continue
		}
		if err := m.beginOperation(group.ID); err != nil {
			result = errors.Join(result, err)
			continue
		}
		stateErr := m.mutate(group.ID, func(current *ShardGroup) { current.State = ShardGroupStopping })
		if err := m.waitForRequestIdle(ctx, group.ID); err != nil {
			result = errors.Join(result, err)
		}
		stopErr := m.stopMembers(ctx, group, false)
		m.releaseReservation(group.ID)
		mutateErr := m.mutate(group.ID, func(current *ShardGroup) {
			if stopErr != nil {
				current.State = ShardGroupFailed
				current.Error = "shutdown teardown failed: " + stopErr.Error()
			} else {
				current.State = ShardGroupFailed
				current.Error = "orchestrator shut down"
			}
		})
		m.finishOperation(group.ID)
		result = errors.Join(result, stateErr, stopErr, mutateErr)
	}
	return result
}

func (m *ShardManager) stopMembers(ctx context.Context, group ShardGroup, force bool) error {
	var result error
	for index := len(group.Members) - 1; index >= 0; index-- {
		member := group.Members[index]
		if !force && !member.Started {
			continue
		}
		_, err := m.engines.Stop(ctx, member.Address, member.EngineID)
		if err != nil {
			result = errors.Join(result, fmt.Errorf("stop engine %s on node %s: %w", member.EngineID, member.NodeID, err))
			continue
		}
		mutateErr := m.mutate(group.ID, func(current *ShardGroup) {
			if index < len(current.Members) {
				current.Members[index].Started = false
			}
		})
		if mutateErr != nil {
			result = errors.Join(result, mutateErr)
		}
	}
	return result
}

func (m *ShardManager) failCreate(id string, cause error) (ShardGroup, error) {
	group, ok := m.Get(id)
	if !ok {
		return ShardGroup{}, cause
	}
	stopErr := m.stopMembers(context.Background(), group, false)
	m.releaseReservation(id)
	mutateErr := m.mutate(id, func(current *ShardGroup) {
		current.State = ShardGroupFailed
		current.Error = cause.Error()
		var refusal *shard.Refusal
		if errors.As(cause, &refusal) {
			current.Rationale = append(current.Rationale, refusal.Rationale...)
			current.Plan.Rationale = append([]string(nil), refusal.Rationale...)
		}
		current.Rationale = append(current.Rationale, "group startup failed: "+cause.Error())
	})
	result, _ := m.Get(id)
	return result, errors.Join(cause, stopErr, mutateErr)
}
