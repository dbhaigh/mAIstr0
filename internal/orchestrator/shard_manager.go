package orchestrator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/logging"
	"github.com/maistr0/maistr0/internal/nodeagent/supervisor"
	"github.com/maistr0/maistr0/internal/shard"
	"github.com/maistr0/maistr0/internal/workpool"
)

type ShardGroupState string

const (
	ShardGroupPlanning        ShardGroupState = "planning"
	ShardGroupStartingWorkers ShardGroupState = "starting_workers"
	ShardGroupStartingHead    ShardGroupState = "starting_head"
	ShardGroupReady           ShardGroupState = "ready"
	ShardGroupDegraded        ShardGroupState = "degraded"
	ShardGroupFailed          ShardGroupState = "failed"
	ShardGroupStopping        ShardGroupState = "stopping"
	ShardGroupStopped         ShardGroupState = "stopped"
)

const managedShardSafetyMarginMB = 2048

type ShardGroup struct {
	ID          string             `json:"id"`
	Model       shard.ModelProfile `json:"model"`
	Mode        shard.Mode         `json:"mode"`
	State       ShardGroupState    `json:"state"`
	Plan        shard.ShardPlan    `json:"plan"`
	Members     []ShardGroupMember `json:"members"`
	PinnedNodes []string           `json:"pinned_nodes,omitempty"`
	Rationale   []string           `json:"rationale"`
	Error       string             `json:"error,omitempty"`
	CreatedAt   time.Time          `json:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at"`
}

type ShardGroupMember struct {
	NodeID            string `json:"node_id"`
	Address           string `json:"address"`
	Role              string `json:"role"`
	EngineID          string `json:"engine_id"`
	EngineKind        string `json:"engine_kind"`
	Rank              int    `json:"rank,omitempty"`
	MasterAddr        string `json:"master_addr,omitempty"`
	Started           bool   `json:"started,omitempty"`
	RPCRelayEndpoint  string `json:"rpc_relay_endpoint,omitempty"`
	RPCBackendAddress string `json:"rpc_backend_address,omitempty"`
}

type CreateShardGroupRequest struct {
	Model           shard.ModelProfile `json:"model"`
	Mode            shard.Mode         `json:"mode"`
	PinnedNodes     []string           `json:"pinned_nodes,omitempty"`
	HeadNodeID      string             `json:"head_node_id,omitempty"`
	HeadEngineID    string             `json:"head_engine_id,omitempty"`
	nodeSnapshots   map[string]cluster.NodeStatus
	requireAllNodes bool
}

type ShardGroupStore interface {
	SaveShardGroup(id string, group []byte) error
	ShardGroups() ([][]byte, error)
	DeleteShardGroup(id string) error
}

type ShardNodeRegistry interface {
	All() []cluster.NodeStatus
	Get(id string) (cluster.NodeStatus, bool)
}

type ManagedEngineControl interface {
	NodeStatus(ctx context.Context, address string) (cluster.NodeStatus, error)
	VerifyModel(ctx context.Context, address, id string, expected shard.ModelProfile, restore bool) error
	List(ctx context.Context, address string) ([]supervisor.Status, error)
	Start(ctx context.Context, address, id string, plan *supervisor.ShardPlan) (supervisor.Status, error)
	Stop(ctx context.Context, address, id string) (supervisor.Status, error)
}

type ShardCapacityReservation interface {
	Release()
}

type ShardCapacityReserver interface {
	ReserveShardNodes(ctx context.Context, candidates []workpool.CapacityTarget) (ShardCapacityReservation, error)
}

type ShardPlanner func(shard.ModelProfile, []shard.CandidateNode, shard.Policy) (shard.ShardPlan, error)

type ShardManager struct {
	// mu protects group snapshots, operation claims, and the process-local reservations map.
	// It is never held during store, supervisor, registry, or capacity-gate calls.
	mu sync.RWMutex
	// persistMu serializes snapshots written to the store; state snapshots are copied under mu.
	persistMu    sync.Mutex
	groups       map[string]ShardGroup
	operations   map[string]bool
	verified     map[string]bool
	recovery     map[string]ShardGroupState
	reservations map[string]ShardCapacityReservation
	requestGates map[string]chan struct{}
	store        ShardGroupStore
	registry     ShardNodeRegistry
	engines      ManagedEngineControl
	capacity     ShardCapacityReserver
	planner      ShardPlanner
}

var (
	errShardGroupInvalid     = errors.New("invalid shard group request")
	errShardGroupConflict    = errors.New("shard group conflict")
	errShardGroupUnavailable = errors.New("shard group members are unavailable")
	errShardGroupOperation   = errors.New("shard group operation is already in progress")
)

func NewShardManager(
	store ShardGroupStore,
	registry ShardNodeRegistry,
	engines ManagedEngineControl,
	capacity ShardCapacityReserver,
	planner ShardPlanner,
) (*ShardManager, error) {
	if store == nil || registry == nil || engines == nil || capacity == nil {
		return nil, errors.New("shard manager requires store, registry, managed engine control, and capacity dependencies")
	}
	if planner == nil {
		planner = shard.Plan
	}
	manager := &ShardManager{
		groups:       make(map[string]ShardGroup),
		operations:   make(map[string]bool),
		verified:     make(map[string]bool),
		recovery:     make(map[string]ShardGroupState),
		reservations: make(map[string]ShardCapacityReservation),
		requestGates: make(map[string]chan struct{}),
		store:        store,
		registry:     registry,
		engines:      engines,
		capacity:     capacity,
		planner:      planner,
	}
	payloads, err := store.ShardGroups()
	if err != nil {
		return nil, fmt.Errorf("load shard groups: %w", err)
	}
	for _, payload := range payloads {
		var group ShardGroup
		if err := json.Unmarshal(payload, &group); err != nil {
			return nil, fmt.Errorf("decode persisted shard group: %w", err)
		}
		if group.ID == "" {
			return nil, errors.New("decode persisted shard group: id is required")
		}
		manager.recovery[group.ID] = group.State
		if group.State != ShardGroupFailed && group.State != ShardGroupStopped {
			group.State = ShardGroupDegraded
			group.Error = "awaiting startup verification"
		}
		manager.groups[group.ID] = cloneShardGroup(group)
		manager.requestGates[group.ID] = newShardRequestGate()
	}
	return manager, nil
}

func (m *ShardManager) List() []ShardGroup {
	m.mu.RLock()
	groups := make([]ShardGroup, 0, len(m.groups))
	for _, group := range m.groups {
		groups = append(groups, cloneShardGroup(group))
	}
	m.mu.RUnlock()
	sort.Slice(groups, func(i, j int) bool { return groups[i].ID < groups[j].ID })
	return groups
}

func (m *ShardManager) Get(id string) (ShardGroup, bool) {
	m.mu.RLock()
	group, ok := m.groups[id]
	if ok {
		group = cloneShardGroup(group)
	}
	m.mu.RUnlock()
	return group, ok
}

func (m *ShardManager) GroupForModel(name string) (ShardGroup, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var selected ShardGroup
	found := false
	for _, group := range m.groups {
		if !strings.EqualFold(group.Model.Name, name) {
			continue
		}
		if !found ||
			(selected.State == ShardGroupFailed && group.State != ShardGroupFailed) ||
			((selected.State == ShardGroupFailed) == (group.State == ShardGroupFailed) &&
				(group.CreatedAt.After(selected.CreatedAt) ||
					(group.CreatedAt.Equal(selected.CreatedAt) &&
						(group.UpdatedAt.After(selected.UpdatedAt) ||
							(group.UpdatedAt.Equal(selected.UpdatedAt) && group.ID < selected.ID))))) {
			selected = group
			found = true
		}
	}
	return cloneShardGroup(selected), found
}

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
			mutateErr := m.mutate(group.ID, func(current *ShardGroup) {
				current.State = ShardGroupFailed
				if stopErr != nil {
					current.Error = "member loss teardown failed: " + stopErr.Error()
				}
			})
			m.finishOperation(group.ID)
			result = errors.Join(result, stopErr, mutateErr)
		}
	}
	return result
}

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

func (m *ShardManager) candidates(
	ctx context.Context,
	request CreateShardGroupRequest,
) ([]shard.CandidateNode, map[string]map[string]supervisor.Status, error) {
	pinned := make(map[string]struct{}, len(request.PinnedNodes))
	for _, id := range request.PinnedNodes {
		pinned[id] = struct{}{}
	}
	nodes := m.registry.All()
	healthyCount := 0
	for _, node := range nodes {
		if node.Healthy {
			healthyCount++
		}
	}
	seenPinned := make(map[string]bool, len(pinned))
	var candidates []shard.CandidateNode
	engineByNode := make(map[string]map[string]supervisor.Status)
	for _, node := range nodes {
		if snapshot, ok := request.nodeSnapshots[node.ID]; ok {
			node = snapshot
		}
		if !node.Healthy {
			continue
		}
		if len(pinned) > 0 {
			if _, ok := pinned[node.ID]; !ok {
				continue
			}
			seenPinned[node.ID] = true
		}
		statuses, err := m.engines.List(ctx, node.Address)
		if err != nil {
			return nil, nil, fmt.Errorf("list managed engines on node %s: %w", node.ID, err)
		}
		byKind := make(map[string]supervisor.Status)
		for _, status := range statuses {
			kind := strings.ToLower(status.Kind)
			if kind == "llama-server" && request.HeadNodeID != "" &&
				(node.ID != request.HeadNodeID || status.ID != request.HeadEngineID) {
				continue
			}
			if _, exists := byKind[kind]; !exists {
				byKind[kind] = status
			}
		}
		engineByNode[node.ID] = byKind
		var modeEngine supervisor.Status
		switch request.Mode {
		case shard.ModeLlamaCPPRPC:
			modeEngine = byKind["ggml-rpc-server"]
			if modeEngine.ID == "" {
				continue
			}
		case shard.ModeVLLMMP:
			modeEngine = byKind["vllm"]
			if modeEngine.ID == "" {
				continue
			}
		default:
			continue
		}
		capabilities := managedShardCapabilities(node, request.Mode)
		rpcAddress := ""
		if request.Mode == shard.ModeLlamaCPPRPC {
			rpcAddress = modeEngine.RelayEndpoint
		} else if request.Mode == shard.ModeVLLMMP {
			rpcAddress = advertisedEndpointAddress(modeEngine.Endpoint, node.Address)
		}
		rpcProtected := request.Mode != shard.ModeLlamaCPPRPC ||
			validRemoteRPCRelayAddress(modeEngine.RelayEndpoint)
		candidates = append(candidates, shard.CandidateNode{
			ID: node.ID, OS: strings.ToLower(node.Hardware.OS),
			RPCAddress: rpcAddress, RPCProtected: rpcProtected,
			AllowInsecureShardNetwork: node.AllowInsecureShardNetwork,
			Devices:                   append([]hardware.Device(nil), node.Hardware.Devices...),
			NetworkLinks:              append([]hardware.NetworkLink(nil), node.Hardware.NetworkLinks...),
			Capabilities:              capabilities,
		})
	}
	for id := range pinned {
		if !seenPinned[id] {
			return nil, nil, fmt.Errorf("%w: pinned node %q is not healthy or not registered", errShardGroupUnavailable, id)
		}
	}
	if len(candidates) == 0 {
		return nil, nil, fmt.Errorf("%w: no healthy nodes have the managed engines required by the requested shard mode", errShardGroupUnavailable)
	}
	if request.Mode == shard.ModeLlamaCPPRPC {
		hasHead := false
		for _, candidate := range candidates {
			if engineByNode[candidate.ID]["llama-server"].ID != "" {
				hasHead = true
				break
			}
		}
		if !hasHead {
			return nil, nil, fmt.Errorf("%w: no candidate node has a configured llama-server head engine", errShardGroupUnavailable)
		}
	}
	return candidates, engineByNode, nil
}

func shardPlanNetworkWarnings(plan shard.ShardPlan, candidates []shard.CandidateNode) ([]string, error) {
	byID := make(map[string]shard.CandidateNode, len(candidates))
	for _, candidate := range candidates {
		byID[candidate.ID] = candidate
	}
	var warnings []string
	for _, planned := range plan.Nodes {
		candidate, exists := byID[planned.NodeID]
		if !exists {
			return nil, &shard.Refusal{
				Code:      shard.RefusalUntrustedNetwork,
				Message:   fmt.Sprintf("shard plan selected unknown node %q; network security cannot be verified", planned.NodeID),
				Rationale: []string{fmt.Sprintf("node %q is absent from the validated candidate snapshot", planned.NodeID)},
			}
		}
		switch plan.Mode {
		case shard.ModeLlamaCPPRPC:
			if !candidate.RPCProtected || !validRemoteRPCRelayAddress(candidate.RPCAddress) {
				return nil, &shard.Refusal{
					Code:      shard.RefusalUntrustedNetwork,
					Message:   fmt.Sprintf("node %q has no authenticated mTLS RPC relay", candidate.ID),
					Rationale: []string{fmt.Sprintf("node %q was selected without a reachable mTLS-protected RPC relay", candidate.ID)},
				}
			}
		case shard.ModeVLLMMP:
			if !trustedNetworkLink(candidate) {
				if !candidate.AllowInsecureShardNetwork {
					return nil, &shard.Refusal{
						Code:      shard.RefusalUntrustedNetwork,
						Message:   fmt.Sprintf("node %q has no operator-trusted network interface for vllm-mp", candidate.ID),
						Rationale: []string{fmt.Sprintf("node %q does not mark any distributed-traffic interface trusted", candidate.ID)},
					}
				}
				warnings = append(warnings, fmt.Sprintf(
					"WARNING: node %q permits unencrypted vllm-mp traffic on an untrusted network because allow_insecure_shard_network is enabled",
					candidate.ID,
				))
			}
		}
	}
	return warnings, nil
}

func trustedNetworkLink(candidate shard.CandidateNode) bool {
	for _, link := range candidate.NetworkLinks {
		if link.Trusted {
			return true
		}
	}
	return false
}

func validRemoteRPCRelayAddress(endpoint string) bool {
	host, port, ok := endpointHostPort(endpoint)
	if !ok {
		return false
	}
	if number, err := strconv.Atoi(port); err != nil || number < 1 || number > 65535 {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return false
	}
	if ip := net.ParseIP(strings.Trim(host, "[]")); ip != nil {
		return !ip.IsLoopback() && !ip.IsUnspecified() && !ip.IsLinkLocalUnicast()
	}
	return true
}

func managedShardCapabilities(node cluster.NodeStatus, mode shard.Mode) engine.Capabilities {
	var capabilities engine.Capabilities
	expectedName := "llamacpp"
	devices := []string{"gpu"}
	if mode == shard.ModeVLLMMP {
		expectedName = "vllm"
	}
	for _, status := range node.Engines {
		if strings.EqualFold(status.Name, expectedName) {
			capabilities = engine.CloneCapabilities(status.Capabilities)
			break
		}
	}
	if len(capabilities.Devices) == 0 {
		capabilities.Devices = devices
	}
	capabilities.SupportsSharding = appendUniqueString(capabilities.SupportsSharding, string(mode))
	if mode == shard.ModeVLLMMP {
		capabilities.HomogeneousOnly = true
		capabilities.OS = appendUniqueString(capabilities.OS, "linux")
	}
	for _, status := range node.Engines {
		capabilities.ValidatedRPC = capabilities.ValidatedRPC || status.Capabilities.ValidatedRPC
	}
	return capabilities
}

func appendUniqueString(values []string, value string) []string {
	for _, existing := range values {
		if strings.EqualFold(existing, value) {
			return values
		}
	}
	return append(values, value)
}

func advertisedEndpointAddress(endpoint, nodeAddress string) string {
	_, port, ok := endpointHostPort(endpoint)
	if !ok {
		return ""
	}
	advertised, err := url.Parse(nodeAddress)
	if err != nil || advertised.Hostname() == "" {
		return ""
	}
	return net.JoinHostPort(advertised.Hostname(), port)
}

func advertisedNodeHost(nodeAddress string) string {
	advertised, err := url.Parse(nodeAddress)
	if err != nil {
		return ""
	}
	return advertised.Hostname()
}

func endpointHostPort(endpoint string) (string, string, bool) {
	if parsed, err := url.Parse(endpoint); err == nil && parsed.Scheme != "" {
		host, port := parsed.Hostname(), parsed.Port()
		return host, port, host != "" && port != ""
	}
	host, port, err := net.SplitHostPort(endpoint)
	return host, port, err == nil && host != "" && port != ""
}

func buildShardMembers(
	plan shard.ShardPlan,
	engineByNode map[string]map[string]supervisor.Status,
	registry ShardNodeRegistry,
	selectedHead string,
) ([]ShardGroupMember, error) {
	if len(plan.Nodes) == 0 {
		return nil, errors.New("shard planner returned an empty node list")
	}
	var members []ShardGroupMember
	switch plan.Mode {
	case shard.ModeLlamaCPPRPC:
		for _, node := range plan.Nodes {
			status := engineByNode[node.NodeID]["ggml-rpc-server"]
			if status.ID == "" {
				return nil, fmt.Errorf("node %s has no managed ggml-rpc-server", node.NodeID)
			}
			members = append(members, ShardGroupMember{
				NodeID: node.NodeID, Role: "worker", EngineID: status.ID,
				EngineKind: status.Kind, RPCRelayEndpoint: status.RelayEndpoint,
				RPCBackendAddress: status.Endpoint,
			})
		}
		var head ShardGroupMember
		if selectedHead != "" {
			status := engineByNode[selectedHead]["llama-server"]
			head = ShardGroupMember{NodeID: selectedHead, Role: "head", EngineID: status.ID, EngineKind: status.Kind}
		}
		for _, node := range plan.Nodes {
			if selectedHead != "" {
				break
			}
			if status := engineByNode[node.NodeID]["llama-server"]; status.ID != "" {
				head = ShardGroupMember{
					NodeID: node.NodeID, Role: "head", EngineID: status.ID,
					EngineKind: status.Kind,
				}
				break
			}
		}
		if head.EngineID == "" {
			return nil, errors.New("planned nodes do not include a managed llama-server head")
		}
		members = append(members, head)
	case shard.ModeVLLMMP:
		headNode := plan.Nodes[0]
		headStatus := engineByNode[headNode.NodeID]["vllm"]
		if headStatus.ID == "" {
			return nil, fmt.Errorf("node %s has no managed vllm engine", headNode.NodeID)
		}
		for rank, node := range plan.Nodes[1:] {
			status := engineByNode[node.NodeID]["vllm"]
			if status.ID == "" {
				return nil, fmt.Errorf("node %s has no managed vllm engine", node.NodeID)
			}
			members = append(members, ShardGroupMember{
				NodeID: node.NodeID, Role: "worker", EngineID: status.ID,
				EngineKind: status.Kind, Rank: rank + 1,
			})
		}
		members = append(members, ShardGroupMember{
			NodeID: headNode.NodeID, Role: "head", EngineID: headStatus.ID,
			EngineKind: headStatus.Kind, Rank: 0,
		})
	default:
		return nil, fmt.Errorf("unsupported shard mode %q", plan.Mode)
	}
	for i := range members {
		node, ok := registry.Get(members[i].NodeID)
		if !ok || !node.Healthy || strings.TrimSpace(node.Address) == "" {
			return nil, fmt.Errorf("node %s is no longer healthy or has no address", members[i].NodeID)
		}
		members[i].Address = node.Address
	}
	if plan.Mode == shard.ModeVLLMMP {
		headNode := plan.Nodes[0]
		headStatus := engineByNode[headNode.NodeID]["vllm"]
		head, ok := registry.Get(headNode.NodeID)
		if !ok || !head.Healthy || strings.TrimSpace(head.Address) == "" {
			return nil, fmt.Errorf("node %s is no longer healthy or has no address", headNode.NodeID)
		}
		masterAddr := advertisedNodeHost(head.Address)
		if masterAddr == "" || headStatus.Endpoint == "" {
			return nil, fmt.Errorf("vllm head node %s does not have a routable endpoint", headNode.NodeID)
		}
		for i := range members {
			members[i].MasterAddr = masterAddr
		}
	}
	for _, member := range members {
		if member.Address == "" {
			return nil, fmt.Errorf("no node address is available for member %s", member.NodeID)
		}
	}
	return members, nil
}

func memberStartPlan(plan shard.ShardPlan, member ShardGroupMember, members []ShardGroupMember) *supervisor.ShardPlan {
	switch plan.Mode {
	case shard.ModeLlamaCPPRPC:
		if member.Role != "head" {
			return nil
		}
		targets := make([]supervisor.RPCRelayTarget, 0, len(plan.Nodes))
		for _, node := range plan.Nodes {
			var worker *ShardGroupMember
			for index := range members {
				if members[index].NodeID == node.NodeID && members[index].Role == "worker" {
					worker = &members[index]
					break
				}
			}
			if worker == nil {
				return nil
			}
			target := supervisor.RPCRelayTarget{PeerID: worker.NodeID}
			if worker.NodeID == member.NodeID {
				target.Address = worker.RPCBackendAddress
			} else {
				target.Address = worker.RPCRelayEndpoint
			}
			targets = append(targets, target)
		}
		return &supervisor.ShardPlan{TensorSplit: plan.TensorSplit, RPCRelays: targets}
	case shard.ModeVLLMMP:
		if member.MasterAddr == "" || len(plan.Nodes) == 0 {
			return nil
		}
		return &supervisor.ShardPlan{
			NNodes: len(plan.Nodes), NodeRank: member.Rank,
			MasterAddr:           member.MasterAddr,
			TensorParallelSize:   plan.TensorParallelSize,
			PipelineParallelSize: plan.PipelineParallelSize,
		}
	default:
		return nil
	}
}

func capacityTargets(members []ShardGroupMember) []workpool.CapacityTarget {
	seen := make(map[string]bool, len(members))
	targets := make([]workpool.CapacityTarget, 0, len(members))
	for _, member := range members {
		if seen[member.NodeID] {
			continue
		}
		seen[member.NodeID] = true
		targets = append(targets, workpool.CapacityTarget{
			NodeID: member.NodeID, Address: member.Address,
		})
	}
	return targets
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
