package agent

import (
	"context"
	"testing"
)

type registryTestBackend struct{ name string }

func (b registryTestBackend) Name() string { return b.name }
func (registryTestBackend) CreateSession(string, SessionConfig) (*Session, error) {
	return nil, nil
}
func (registryTestBackend) GetSession(string) (*Session, bool) { return nil, false }
func (registryTestBackend) ListSessions() []*Session           { return nil }
func (registryTestBackend) DeleteSession(string) bool          { return false }
func (registryTestBackend) SendMessage(context.Context, string, string, chan<- StreamEvent) (*Message, error) {
	return nil, nil
}
func (registryTestBackend) InterruptSession(string) bool { return false }
func (registryTestBackend) ListTools() []ToolDef         { return nil }
func (registryTestBackend) ListClusterModels() []ModelInfo {
	return nil
}
func (registryTestBackend) Stats() AgentStats { return AgentStats{} }
func (registryTestBackend) ExecuteTool(context.Context, string, string, map[string]any) (ToolResult, error) {
	return ToolResult{}, nil
}
func (registryTestBackend) Close() error { return nil }

type registryRestorableBackend struct{ registryTestBackend }

func (registryRestorableBackend) RestoreSession(*Session) error { return nil }

func TestRegistryRequiresRestorerForResumeCapability(t *testing.T) {
	registry := NewRegistry()
	err := registry.Register(BackendRegistration{
		Backend:      registryTestBackend{name: "external"},
		Capabilities: Capabilities{SessionResume: true},
	})
	if err == nil {
		t.Fatal("registered session-resume capability without a restorer")
	}
	if err := registry.Register(BackendRegistration{
		Backend:      registryRestorableBackend{registryTestBackend{name: "external"}},
		Capabilities: Capabilities{SessionResume: true, Streaming: true},
	}); err != nil {
		t.Fatalf("register restorable backend: %v", err)
	}
	if _, ok := registry.Get("external"); !ok {
		t.Fatal("registered backend was not discoverable")
	}
}

func TestRegistryValidatesNamesAndRejectsDuplicates(t *testing.T) {
	for _, name := range []string{"", "Pi", "a b", "-backend", "backend/child", "backend\n"} {
		if ValidBackendName(name) {
			t.Errorf("ValidBackendName(%q) = true", name)
		}
	}
	for _, name := range []string{"pi", "deepseek", "claude-code", "backend_2"} {
		if !ValidBackendName(name) {
			t.Errorf("ValidBackendName(%q) = false", name)
		}
	}

	registry := NewRegistry()
	registration := BackendRegistration{Backend: registryTestBackend{name: "external"}}
	if err := registry.Register(registration); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(registration); err == nil {
		t.Fatal("duplicate backend registration succeeded")
	}
}
