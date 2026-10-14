package nodeagent

import (
	"fmt"
	"sync"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
)

func TestExecutorCapacityClampsAtZero(t *testing.T) {
	state := &agentState{}
	executor := newExecutorComponent(2, state)

	for _, tc := range []struct {
		active    int
		available int
	}{
		{active: 0, available: 2},
		{active: 1, available: 1},
		{active: 2, available: 0},
		{active: 3, available: 0},
	} {
		state.mu.Lock()
		state.activeTasks = tc.active
		state.mu.Unlock()
		if got := executor.availableCapacity(2); got != tc.available {
			t.Errorf("active=%d: availableCapacity = %d, want %d", tc.active, got, tc.available)
		}
	}
}

func TestPeerComponentSelection(t *testing.T) {
	state := &agentState{
		defaultModel: "local",
		peers: []cluster.NodeStatus{
			{ID: "alpha", Address: "https://alpha", Healthy: true, Leader: true, FastScore: 2},
			{ID: "beta", Address: "https://beta", Healthy: true, FastScore: 10},
			{ID: "gamma", Address: "https://gamma", Healthy: true, FastScore: 10},
			{ID: "offline", Healthy: false, Leader: true, FastScore: 100},
		},
	}
	telemetry := &telemetryComponent{}
	models := &engineComponent{state: state, telemetry: telemetry}
	executor := newExecutorComponent(4, state)
	cfg := config.NodeConfig{NodeID: "self", AdvertiseAddr: "https://self"}
	state.cfg = cfg
	peers := &peerComponent{
		state:     state,
		telemetry: telemetry,
		models:    models,
		executor:  executor,
	}

	if got := peers.mostCapableNode().ID; got != "alpha" {
		t.Fatalf("elected leader = %q, want alpha", got)
	}
	if got := peers.fastestNode().ID; got != "beta" {
		t.Fatalf("fastest node = %q, want beta (lexical tie-break)", got)
	}
	if got, ok := peers.peerAddress("alpha"); !ok || got != "https://alpha" {
		t.Fatalf("peer address = (%q, %v), want (https://alpha, true)", got, ok)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			state.mu.Lock()
			state.cfg.AdvertiseAddr = fmt.Sprintf("https://self-%d", i)
			state.mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = peers.mostCapableNode()
			_, _ = peers.peerAddress("self")
		}
	}()
	wg.Wait()
}

func TestDialogueSnapshotsDoNotExposeMutableState(t *testing.T) {
	component := &dialogueComponent{
		dialogues: map[string]*Dialogue{
			"dlg": {
				ID:       "dlg",
				Messages: []engine.ChatMessage{{Role: "user", Content: "original"}},
			},
		},
	}

	got, ok := component.get("dlg")
	if !ok {
		t.Fatal("dialogue missing")
	}
	got.Messages[0].Content = "changed"
	list := component.list()
	list[0].Messages[0].Content = "changed again"

	stored, ok := component.get("dlg")
	if !ok || stored.Messages[0].Content != "original" {
		t.Fatalf("stored dialogue was modified through a snapshot: %+v", stored)
	}
}

func TestEngineComponentAppliesDisabledAndDefaultState(t *testing.T) {
	state := newAgentState(config.NodeConfig{
		DefaultModel:   "enabled-model",
		DisabledModels: []string{"disabled-model"},
	})
	models := []engine.Engine{componentTestEngine{models: []engine.Model{
		{Name: "enabled-model"},
		{Name: "disabled-model"},
	}}}
	component := newEngineComponent(models, func() []engine.Engine { return nil }, &telemetryComponent{}, state)

	if got := component.defaultModelName(); got != "enabled-model" {
		t.Fatalf("default model = %q, want enabled-model", got)
	}
	if got := component.enabledModels(); len(got) != 1 || got[0].Name != "enabled-model" {
		t.Fatalf("enabled models = %#v, want only enabled-model", got)
	}
	states := component.allModelsWithState()
	if len(states) != 2 || !states[0].Enabled || !states[0].Default || states[1].Enabled || states[1].Default {
		t.Fatalf("model states = %#v", states)
	}
}

type componentTestEngine struct {
	models []engine.Model
}

func (componentTestEngine) Name() string                          { return "test" }
func (componentTestEngine) Available() bool                       { return true }
func (e componentTestEngine) ListModels() ([]engine.Model, error) { return e.models, nil }
func (componentTestEngine) Generate(string, string) (string, error) {
	return "", nil
}
func (componentTestEngine) Chat(string, []engine.ChatMessage) (string, error) {
	return "", nil
}
