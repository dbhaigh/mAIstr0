package nodeagent

import (
	"fmt"
	"sync"
	"testing"
	"time"

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

func TestEngineInventoryRefreshTracksChangesAndRetainsLastGoodModelsOnError(t *testing.T) {
	eng := &inventoryTestEngine{models: []engine.Model{{Name: "alpha"}}}
	state := newAgentState(config.NodeConfig{DefaultModel: "alpha"})
	component := newEngineComponent(
		[]engine.Engine{eng}, func() []engine.Engine { return nil }, &telemetryComponent{}, state,
	)
	assertModels := func(want ...string) {
		t.Helper()
		got := component.reportedModelList()
		if len(got) != len(want) {
			t.Fatalf("reported models = %#v, want names %v", got, want)
		}
		for i, name := range want {
			if got[i].Name != name {
				t.Fatalf("reported model %d = %q, want %q", i, got[i].Name, name)
			}
		}
	}
	assertStatus := func(wantState string, wantReady bool, wantCount int) engine.Status {
		t.Helper()
		statuses := component.reportedEngineList()
		if len(statuses) != 1 {
			t.Fatalf("reported engine statuses = %#v, want one status", statuses)
		}
		status := statuses[0]
		if status.InventoryState != wantState || status.Ready != wantReady || status.ModelCount != wantCount {
			t.Fatalf("reported engine status = %+v, want state=%q ready=%v count=%d", status, wantState, wantReady, wantCount)
		}
		return status
	}

	component.refreshReportedModels()
	assertModels("alpha")
	assertStatus(engine.ModelInventoryAvailable, true, 1)

	eng.setInventory([]engine.Model{{Name: "alpha"}, {Name: "beta"}}, nil)
	component.refreshReportedModels()
	assertModels("alpha", "beta")
	assertStatus(engine.ModelInventoryAvailable, true, 2)

	eng.setInventory([]engine.Model{{Name: "beta"}}, nil)
	component.refreshReportedModels()
	assertModels("beta")
	assertStatus(engine.ModelInventoryAvailable, true, 1)

	eng.setInventory(nil, nil)
	component.refreshReportedModels()
	assertModels()
	emptyStatus := assertStatus(engine.ModelInventoryEmpty, true, 0)
	if emptyStatus.LastSuccessfulAt.IsZero() {
		t.Fatal("successful empty inventory has no last-success timestamp")
	}

	eng.setInventory([]engine.Model{{Name: "alpha"}, {Name: "beta"}}, nil)
	component.refreshReportedModels()
	assertModels("alpha", "beta")
	lastSuccess := assertStatus(engine.ModelInventoryAvailable, true, 2).LastSuccessfulAt

	eng.setInventory(nil, fmt.Errorf("engine temporarily unavailable"))
	component.refreshReportedModels()
	assertModels("alpha", "beta")
	failedStatus := assertStatus(engine.ModelInventoryError, false, 2)
	if failedStatus.LastError != "engine temporarily unavailable" {
		t.Fatalf("last inventory error = %q", failedStatus.LastError)
	}
	if failedStatus.LastCheckedAt.IsZero() || !failedStatus.LastSuccessfulAt.Equal(lastSuccess) {
		t.Fatalf("failed inventory timestamps = checked %v, successful %v; want checked time and retained success %v",
			failedStatus.LastCheckedAt, failedStatus.LastSuccessfulAt, lastSuccess)
	}
}

func TestPeriodicEngineScanRetainsUnavailableEngineDuringGracePeriod(t *testing.T) {
	eng := &inventoryTestEngine{models: []engine.Model{{Name: "cached-model"}}}
	detected := []engine.Engine{eng}
	component := newEngineComponent(
		nil, func() []engine.Engine { return detected }, &telemetryComponent{}, newAgentState(config.NodeConfig{}),
	)
	component.rescanDetectedEngines()
	component.refreshReportedModels()

	eng.setInventory(nil, fmt.Errorf("endpoint unavailable"))
	detected = nil
	component.enginesMu.Lock()
	component.lastEngineScan = time.Now().Add(-engineDetectionInterval)
	component.enginesMu.Unlock()
	component.refreshDetectedEngines()
	component.refreshReportedModels()

	if got := component.engineSnapshot(); len(got) != 1 || got[0].Name() != eng.Name() {
		t.Fatalf("recently unavailable engine snapshot = %#v, want retained engine", got)
	}
	models := component.reportedModelList()
	if len(models) != 1 || models[0].Name != "cached-model" {
		t.Fatalf("models after availability scan failed = %#v, want retained cached model", models)
	}
	statuses := component.reportedEngineList()
	if len(statuses) != 1 || statuses[0].InventoryState != engine.ModelInventoryError {
		t.Fatalf("status after availability scan failed = %#v, want inventory error", statuses)
	}

	component.enginesMu.Lock()
	component.detectedSeenAt[engineIdentity(eng)] = time.Now().Add(-engineDetectionGracePeriod - time.Second)
	component.lastEngineScan = time.Now().Add(-engineDetectionInterval)
	component.enginesMu.Unlock()
	component.refreshDetectedEngines()
	if got := component.engineSnapshot(); len(got) != 0 {
		t.Fatalf("engine snapshot after detection grace expired = %#v, want no retained engine", got)
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

type inventoryTestEngine struct {
	mu     sync.Mutex
	models []engine.Model
	err    error
}

func (e *inventoryTestEngine) Name() string    { return "test" }
func (e *inventoryTestEngine) Available() bool { return true }
func (e *inventoryTestEngine) ListModels() ([]engine.Model, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]engine.Model(nil), e.models...), e.err
}
func (e *inventoryTestEngine) Generate(string, string) (string, error) { return "", nil }
func (e *inventoryTestEngine) Chat(string, []engine.ChatMessage) (string, error) {
	return "", nil
}
func (e *inventoryTestEngine) setInventory(models []engine.Model, err error) {
	e.mu.Lock()
	e.models = append([]engine.Model(nil), models...)
	e.err = err
	e.mu.Unlock()
}
