package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
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
