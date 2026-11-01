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

// ShardGroupState identifies the planning, startup, health, and shutdown phase
// of a [ShardGroup] owned by [ShardManager].
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [shard_manager_test.go](shard_manager_test.go).
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

// ShardGroup is the persisted and API-facing snapshot owned by [ShardManager].
// Its [ShardGroupMember] values, plan, state, and timestamps describe one
// distributed model deployment.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [shard_manager_test.go](shard_manager_test.go).
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

// ShardGroupMember is a node-and-engine record owned by a [ShardGroup],
// including its role and startup state.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [shard_manager_test.go](shard_manager_test.go).
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

// CreateShardGroupRequest is the creation input consumed by [ShardManager],
// containing a model, mode, and optional placement constraints.
//
// PinnedNodes must contain nonempty unique node IDs. HeadNodeID and HeadEngineID
// must be provided together and are supported only for llama.cpp RPC mode.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [shard_manager_test.go](shard_manager_test.go).
type CreateShardGroupRequest struct {
	Model                 shard.ModelProfile `json:"model"`
	Mode                  shard.Mode         `json:"mode"`
	PinnedNodes           []string           `json:"pinned_nodes,omitempty"`
	HeadNodeID            string             `json:"head_node_id,omitempty"`
	HeadEngineID          string             `json:"head_engine_id,omitempty"`
	nodeSnapshots         map[string]cluster.NodeStatus
	requireAllNodes       bool
	allowHeadlessPlanning bool
}

// ShardGroupStore is the persistence boundary owned by [ShardManager] for
// serialized [ShardGroup] snapshots.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [shard_manager_test.go](shard_manager_test.go).
type ShardGroupStore interface {
	// SaveShardGroup stores the serialized snapshot under id.
	//
	// Parameters: id (string) is the shard-group key; group ([]byte) is its
	// serialized representation.
	// Returns: a persistence error.
	//
	// References: [request paths](../../docs/architecture.md#request-paths),
	// [shard_manager_test.go](shard_manager_test.go).
	SaveShardGroup(id string, group []byte) error
	// ShardGroups loads the serialized snapshots currently in the store.
	//
	// Returns: the stored payloads, or a persistence error.
	//
	// References: [request paths](../../docs/architecture.md#request-paths),
	// [shard_manager_test.go](shard_manager_test.go).
	ShardGroups() ([][]byte, error)
	// DeleteShardGroup removes the snapshot identified by id.
	//
	// Parameters: id (string) is the shard-group key to remove.
	// Returns: a persistence error.
	//
	// References: [request paths](../../docs/architecture.md#request-paths),
	// [shard_manager_test.go](shard_manager_test.go).
	DeleteShardGroup(id string) error
}

// ShardNodeRegistry is the registry boundary owned by [ShardManager], supplying
// the node snapshots used to plan and validate managed deployments.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [shard_manager_test.go](shard_manager_test.go).
type ShardNodeRegistry interface {
	// All returns the current node-status snapshot.
	//
	// Returns: the registered node statuses.
	//
	// References: [request paths](../../docs/architecture.md#request-paths),
	// [shard_manager_test.go](shard_manager_test.go).
	All() []cluster.NodeStatus
	// Get returns the current status for one node ID.
	//
	// Parameters: id (string) is the node ID to look up.
	// Returns: the node status and whether it is registered.
	//
	// References: [request paths](../../docs/architecture.md#request-paths),
	// [shard_manager_test.go](shard_manager_test.go).
	Get(id string) (cluster.NodeStatus, bool)
}

// ManagedEngineControl is the engine-control boundary owned by [ShardManager]
// to inspect, verify, start, and stop node-owned engines for [ShardGroup].
//
// References: [node ownership boundaries](../../docs/architecture.md#node-ownership-boundaries),
// [managed_engine_client_test.go](managed_engine_client_test.go).
type ManagedEngineControl interface {
	// NodeStatus retrieves a node's current status.
	//
	// Parameters: ctx (context.Context) controls the request; address (string)
	// is the node base URL.
	// Returns: the node status, or a request or decode error.
	//
	// References: [node ownership boundaries](../../docs/architecture.md#node-ownership-boundaries),
	// [managed_engine_client_test.go](managed_engine_client_test.go).
	NodeStatus(ctx context.Context, address string) (cluster.NodeStatus, error)
	// VerifyModel checks or restores the expected managed model profile.
	//
	// Parameters: ctx (context.Context) controls the request; address and id
	// (string) identify the node and engine; expected (shard.ModelProfile) is
	// the target profile; restore (bool) allows restoring its saved profile.
	// Returns: an error when verification or restoration fails.
	//
	// References: [node ownership boundaries](../../docs/architecture.md#node-ownership-boundaries),
	// [managed_engine_client_test.go](managed_engine_client_test.go).
	VerifyModel(ctx context.Context, address, id string, expected shard.ModelProfile, restore bool) error
	// List returns the managed-engine statuses reported by one node.
	//
	// Parameters: ctx (context.Context) controls the request; address (string)
	// is the node base URL.
	// Returns: the engine statuses, or a request, status, or decode error.
	//
	// References: [node ownership boundaries](../../docs/architecture.md#node-ownership-boundaries),
	// [managed_engine_client_test.go](managed_engine_client_test.go).
	List(ctx context.Context, address string) ([]supervisor.Status, error)
	// Start starts an engine, optionally applying a distributed shard plan.
	//
	// Parameters: ctx (context.Context) controls the request; address and id
	// (string) identify the node and engine; plan (*supervisor.ShardPlan) is
	// optional placement configuration.
	// Returns: the resulting engine status, or a request or response error.
	//
	// References: [node ownership boundaries](../../docs/architecture.md#node-ownership-boundaries),
	// [managed_engine_client_test.go](managed_engine_client_test.go).
	Start(ctx context.Context, address, id string, plan *supervisor.ShardPlan) (supervisor.Status, error)
	// Stop stops an engine on the identified node.
	//
	// Parameters: ctx (context.Context) controls the request; address and id
	// (string) identify the node and engine.
	// Returns: the resulting engine status, or a request or response error.
	//
	// References: [node ownership boundaries](../../docs/architecture.md#node-ownership-boundaries),
	// [managed_engine_client_test.go](managed_engine_client_test.go).
	Stop(ctx context.Context, address, id string) (supervisor.Status, error)
}

// ShardCapacityReservation is the capacity-release contract owned by
// [ShardManager] for one or more managed shard nodes.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [shard_manager_test.go](shard_manager_test.go).
type ShardCapacityReservation interface {
	// Release returns the capacity represented by this reservation.
	//
	// References: [request paths](../../docs/architecture.md#request-paths),
	// [shard_manager_test.go](shard_manager_test.go).
	Release()
}

// ShardCapacityReserver is the capacity-acquisition boundary owned by
// [ShardManager] for a shard placement; it returns a
// [ShardCapacityReservation].
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [shard_manager_test.go](shard_manager_test.go).
type ShardCapacityReserver interface {
	// ReserveShardNodes atomically reserves capacity for every candidate.
	//
	// Parameters: ctx (context.Context) controls waiting; candidates
	// ([]workpool.CapacityTarget) must have unique, nonempty node IDs and
	// addresses.
	// Returns: a reservation for all candidates, or an error for invalid,
	// unavailable, or canceled requests.
	//
	// References: [request paths](../../docs/architecture.md#request-paths),
	// [shard_manager_test.go](shard_manager_test.go).
	ReserveShardNodes(ctx context.Context, candidates []workpool.CapacityTarget) (ShardCapacityReservation, error)
}

// ShardPlanner is the planning hook owned by [ShardManager] that selects a
// distributed plan for a model and eligible candidate nodes.
//
// Parameters: the model (shard.ModelProfile), candidates ([]shard.CandidateNode),
// and policy (shard.Policy) define the placement input.
// Returns: a shard.ShardPlan, or an error when no valid plan can be produced.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [shard_manager_test.go](shard_manager_test.go).
type ShardPlanner func(shard.ModelProfile, []shard.CandidateNode, shard.Policy) (shard.ShardPlan, error)

// ShardManager owns shard-group state, persistence, lifecycle transitions, and
// the reservations held for managed group members. Its public snapshots are
// represented by [ShardGroup].
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [shard_manager_test.go](shard_manager_test.go).
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

// NewShardManager creates a shard manager and restores persisted group
// snapshots. The planner is optional; nil selects shard.Plan.
//
// Parameters: store (ShardGroupStore), registry (ShardNodeRegistry), engines
// (ManagedEngineControl), and capacity (ShardCapacityReserver) are required;
// planner (ShardPlanner) may be nil.
// Returns: the initialized manager, or an error for missing dependencies or
// invalid/unreadable persisted groups.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [shard_manager_test.go](shard_manager_test.go).
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

// List returns an ID-sorted snapshot of all groups. The returned groups are
// defensive copies and can be modified by the caller.
//
// Returns: the current shard-group snapshots.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [shard_manager_test.go](shard_manager_test.go).
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

// Get returns a defensive copy of the group identified by id.
//
// Parameters: id (string) is the shard-group ID.
// Returns: the group and true when found; otherwise a zero group and false.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [shard_manager_test.go](shard_manager_test.go).
func (m *ShardManager) Get(id string) (ShardGroup, bool) {
	m.mu.RLock()
	group, ok := m.groups[id]
	if ok {
		group = cloneShardGroup(group)
	}
	m.mu.RUnlock()
	return group, ok
}

// GroupForModel returns the best matching group for name using a
// case-insensitive model-name match. It prefers nonfailed groups, then the
// newest creation and update timestamps, with ID as a deterministic tie-break.
//
// Parameters: name (string) is the model name to match.
// Returns: a defensive copy and true when found; otherwise a zero group and
// false.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [shard_manager_test.go](shard_manager_test.go).
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
