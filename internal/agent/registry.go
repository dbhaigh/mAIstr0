package agent

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

type Capabilities struct {
	SessionResume      bool `json:"session_resume"`
	Streaming          bool `json:"streaming"`
	Cancellation       bool `json:"cancellation"`
	Approvals          bool `json:"approvals"`
	WorkspaceIsolation bool `json:"workspace_isolation"`
	ModelSelection     bool `json:"model_selection"`
	ToolAccess         bool `json:"tool_access"`
}

type ExecutionLocation string

const (
	ExecutionLocal   ExecutionLocation = "local"
	ExecutionRemote  ExecutionLocation = "remote"
	ExecutionUnknown ExecutionLocation = "unknown"
)

type ExecutionProfile struct {
	Location            ExecutionLocation `json:"location"`
	Workspace           string            `json:"workspace,omitempty"`
	Isolation           string            `json:"isolation"`
	ApprovalLimitations string            `json:"approval_limitations"`
}

type BackendRegistration struct {
	Backend      Backend
	Capabilities Capabilities
	Execution    ExecutionProfile
}

type Registry struct {
	mu       sync.RWMutex
	backends map[string]BackendRegistration
}

func NewRegistry() *Registry {
	return &Registry{backends: make(map[string]BackendRegistration)}
}

func (r *Registry) Register(registration BackendRegistration) error {
	if registration.Backend == nil {
		return errors.New("agent backend is required")
	}
	name := registration.Backend.Name()
	if !ValidBackendName(name) {
		return fmt.Errorf("invalid agent backend name %q", name)
	}
	if registration.Capabilities.SessionResume {
		if _, ok := registration.Backend.(SessionRestorer); !ok {
			return fmt.Errorf("agent backend %q advertises session resume without a session restorer", name)
		}
	}
	if registration.Execution.Location != "" &&
		registration.Execution.Location != ExecutionLocal &&
		registration.Execution.Location != ExecutionRemote &&
		registration.Execution.Location != ExecutionUnknown {
		return fmt.Errorf("agent backend %q has an invalid execution location", name)
	}
	if registration.Execution.Location == "" {
		registration.Execution.Location = ExecutionUnknown
	}
	if registration.Execution.Isolation == "" {
		registration.Execution.Isolation = "Not reported"
	}
	if registration.Execution.ApprovalLimitations == "" {
		registration.Execution.ApprovalLimitations = "Not reported"
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.backends[name]; exists {
		return fmt.Errorf("agent backend %q is already registered", name)
	}
	r.backends[name] = registration
	return nil
}

func (r *Registry) Get(name string) (BackendRegistration, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	registration, ok := r.backends[name]
	return registration, ok
}

func (r *Registry) List() []BackendRegistration {
	r.mu.RLock()
	out := make([]BackendRegistration, 0, len(r.backends))
	for _, registration := range r.backends {
		out = append(out, registration)
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		return out[i].Backend.Name() < out[j].Backend.Name()
	})
	return out
}

func ValidBackendName(name string) bool {
	if name == "" || len(name) > 64 || strings.TrimSpace(name) != name {
		return false
	}
	for i, char := range name {
		if char >= 'a' && char <= 'z' || i > 0 && char >= '0' && char <= '9' {
			continue
		}
		if i > 0 && (char == '-' || char == '_') {
			continue
		}
		return false
	}
	return true
}
