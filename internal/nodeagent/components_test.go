package nodeagent

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
)

func TestExecutorCapacityClampsAtZero(t *testing.T) {
	for _, tc := range []struct {
		active    int
		available int
	}{
		{active: 0, available: 2},
		{active: 1, available: 1},
		{active: 2, available: 0},
		{active: 3, available: 0},
	} {
		executor := newExecutorComponent(2)
		for range tc.active {
			t.Cleanup(executor.beginActiveTask())
		}
		if got := executor.availableCapacity(2); got != tc.available {
			t.Errorf("active=%d: availableCapacity = %d, want %d", tc.active, got, tc.available)
		}
	}
}

func TestPeerComponentSelection(t *testing.T) {
	state := &agentState{
		peers: []cluster.NodeStatus{
			{ID: "alpha", Address: "https://alpha", Healthy: true, Leader: true, FastScore: 2},
			{ID: "beta", Address: "https://beta", Healthy: true, FastScore: 10},
			{ID: "gamma", Address: "https://gamma", Healthy: true, FastScore: 10},
			{ID: "offline", Healthy: false, Leader: true, FastScore: 100},
		},
	}
	telemetry := &telemetryComponent{}
	models := &engineComponent{defaultModel: "local", telemetry: telemetry}
	executor := newExecutorComponent(4)
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
	cfg := config.NodeConfig{
		DefaultModel:   "enabled-model",
		DisabledModels: []string{"disabled-model"},
	}
	models := []engine.Engine{componentTestEngine{models: []engine.Model{
		{Name: "enabled-model"},
		{Name: "disabled-model"},
	}}}
	component := newEngineComponent(models, func() []engine.Engine { return nil }, &telemetryComponent{}, cfg.DisabledModels, cfg.DefaultModel)

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
	component := newEngineComponent(
		[]engine.Engine{eng}, func() []engine.Engine { return nil }, &telemetryComponent{}, nil, "alpha",
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
	if failedStatus.LastError != "model inventory request failed; check engine health and configuration" {
		t.Fatalf("last inventory error = %q", failedStatus.LastError)
	}
	if failedStatus.LastCheckedAt.IsZero() || !failedStatus.LastSuccessfulAt.Equal(lastSuccess) {
		t.Fatalf("failed inventory timestamps = checked %v, successful %v; want checked time and retained success %v",
			failedStatus.LastCheckedAt, failedStatus.LastSuccessfulAt, lastSuccess)
	}
}

func TestModelSelectionUpdatedDuringInventoryProbeIsPublished(t *testing.T) {
	backend := &blockingInventoryEngine{started: make(chan struct{}), release: make(chan struct{})}
	component := newEngineComponent([]engine.Engine{backend}, func() []engine.Engine { return nil },
		&telemetryComponent{}, nil, "")
	done := make(chan struct{})
	go func() {
		defer close(done)
		component.refreshReportedModels()
	}()
	var release sync.Once
	unblock := func() { release.Do(func() { close(backend.release) }) }
	defer func() { unblock(); <-done }()
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatal("inventory probe did not start")
	}
	component.configureModels(map[string]bool{"model": true}, "model")
	unblock()
	<-done
	models := component.reportedAllModelList()
	if len(models) != 1 || models[0].Enabled || !models[0].Default {
		t.Fatalf("inventory published stale model selection: %+v", models)
	}
}

func TestModelSelectionUpdateSemantics(t *testing.T) {
	for _, test := range []struct {
		name           string
		body           string
		defaultModel   string
		disabled       bool
		modelsEndpoint bool
	}{
		{"properties omitted retains selection", `{"default_model":"beta"}`, "beta", true, false},
		{"properties null retains selection", `{"default_model":"beta","disabled_models":null}`, "beta", true, false},
		{"properties empty clears selection", `{"default_model":"beta","disabled_models":[]}`, "beta", false, false},
		{"properties empty default retains default", `{"default_model":""}`, "alpha", true, false},
		{"models omitted clears selection", `{}`, "alpha", false, true},
		{"models null clears selection", `{"disabled_models":null}`, "alpha", false, true},
		{"models empty clears selection", `{"disabled_models":[]}`, "alpha", false, true},
		{"models default updates selection", `{"default_model":"beta"}`, "beta", false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := newAgentState(config.NodeConfig{DefaultModel: "alpha", DisabledModels: []string{"alpha"}})
			component := newEngineComponent([]engine.Engine{componentTestEngine{models: []engine.Model{{Name: "alpha"}, {Name: "beta"}}}},
				func() []engine.Engine { return nil }, &telemetryComponent{}, state.cfg.DisabledModels, state.cfg.DefaultModel)
			component.refreshReportedModels()
			a := &Agent{agentState: state, engineComponent: component, orchestratorLink: &orchestratorLink{}}
			response := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPatch, "/", strings.NewReader(test.body))
			if test.modelsEndpoint {
				a.handleModelsPatch(response, request)
			} else {
				a.handleProperties(response, request)
			}
			if response.Code != http.StatusOK {
				t.Fatalf("selection patch = %d: %s", response.Code, response.Body.String())
			}
			if !test.modelsEndpoint {
				var properties nodeProperties
				if err := json.Unmarshal(response.Body.Bytes(), &properties); err != nil {
					t.Fatal(err)
				}
				wantDisabled := 0
				if test.disabled {
					wantDisabled = 1
				}
				if properties.DefaultModel != test.defaultModel || len(properties.DisabledModels) != wantDisabled ||
					(wantDisabled == 1 && properties.DisabledModels[0] != "alpha") {
					t.Fatalf("properties selection = %+v", properties)
				}
			}
			models := component.reportedAllModelList()
			if len(models) != 2 || models[0].Enabled == test.disabled ||
				models[0].Default != (test.defaultModel == "alpha") || models[1].Default != (test.defaultModel == "beta") {
				t.Fatalf("properties and inventory selection disagree: %+v", models)
			}
		})
	}
}

func TestModelSelectionSnapshotsAndConcurrentPublication(t *testing.T) {
	disabled := []string{"beta"}
	component := newEngineComponent([]engine.Engine{componentTestEngine{models: []engine.Model{{Name: "alpha"}, {Name: "beta"}}}},
		func() []engine.Engine { return nil }, &telemetryComponent{}, disabled, "alpha")
	disabled[0] = "alpha"
	snapshot, defaultModel := component.modelSelectionSnapshot()
	if len(snapshot) != 1 || snapshot[0] != "beta" || defaultModel != "alpha" {
		t.Fatalf("initial selection was aliased: %v %q", snapshot, defaultModel)
	}
	snapshot[0] = "alpha"
	snapshot, _ = component.modelSelectionSnapshot()
	if len(snapshot) != 1 || snapshot[0] != "beta" {
		t.Fatalf("snapshot exposed mutable selection: %v", snapshot)
	}
	component.refreshReportedModels()
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		for i := range 100 {
			if i%2 == 0 {
				component.configureModels(map[string]bool{"alpha": true}, "beta")
			} else {
				component.configureModels(map[string]bool{"beta": true}, "alpha")
			}
		}
	}()
	go func() {
		defer workers.Done()
		for range 100 {
			component.refreshReportedModels()
			models := component.reportedAllModelList()
			if len(models) != 2 || models[0].Default == models[1].Default ||
				models[0].Enabled != models[0].Default || models[1].Enabled != models[1].Default {
				t.Errorf("non-atomic selection publication: %+v", models)
				return
			}
			names, selected := component.modelSelectionSnapshot()
			if len(names) != 1 || names[0] == selected {
				t.Errorf("non-atomic selection snapshot: %v %q", names, selected)
				return
			}
		}
	}()
	workers.Wait()
}

func TestEngineInventorySurfacesModelAndEngineCapabilities(t *testing.T) {
	eng := &capabilityInventoryTestEngine{
		inventoryTestEngine: &inventoryTestEngine{models: []engine.Model{{
			Name: "accelerated-model", Device: "npu",
		}}},
		capabilities: engine.Capabilities{Devices: []string{"cpu"}, Backends: []string{"cann"}},
	}
	component := newEngineComponent(
		[]engine.Engine{eng}, func() []engine.Engine { return nil }, &telemetryComponent{},
		nil, "",
	)

	component.refreshReportedModels()
	models := component.reportedModelList()
	if len(models) != 1 || models[0].Device != "npu" {
		t.Fatalf("reported models = %+v, want model running on NPU", models)
	}
	statuses := component.reportedEngineList()
	if len(statuses) != 1 || !statuses[0].Ready || statuses[0].ModelCount != 1 {
		t.Fatalf("reported engine status = %+v", statuses)
	}
	if !containsEngineCapability(statuses[0].Capabilities.Devices, "cpu") ||
		!containsEngineCapability(statuses[0].Capabilities.Devices, "npu") ||
		!containsEngineCapability(statuses[0].Capabilities.Backends, "cann") {
		t.Fatalf("reported capabilities = %+v", statuses[0].Capabilities)
	}

	statuses[0].Capabilities.Devices[0] = "mutated"
	if got := component.reportedEngineList()[0].Capabilities.Devices[0]; got == "mutated" {
		t.Fatal("mutating a returned engine status changed cached capabilities")
	}
}

func TestModelInventoryTTLAndHardwareGenerationCaches(t *testing.T) {
	eng := &inventoryTestEngine{models: []engine.Model{{Name: "alpha"}}}
	telemetry := &telemetryComponent{}
	component := newEngineComponent(
		[]engine.Engine{eng}, func() []engine.Engine { return nil }, telemetry, nil, "",
	)

	if got := component.enabledModels(); len(got) != 1 || got[0].Name != "alpha" {
		t.Fatalf("initial inventory = %#v, want alpha", got)
	}

	if got := component.enabledModels(); len(got) != 1 || got[0].Name != "alpha" {
		t.Fatalf("cached inventory = %#v, want alpha", got)
	}
	if calls := eng.listModelCalls(); calls != 1 {
		t.Fatalf("ListModels calls before cache expiry = %d, want 1", calls)
	}

	eng.setInventory([]engine.Model{{Name: "beta"}}, nil)
	component.modelMu.Lock()
	component.reportedAt = time.Now().Add(-modelInventoryCacheTTL - time.Millisecond)
	component.modelMu.Unlock()
	if got := component.enabledModels(); len(got) != 1 || got[0].Name != "beta" {
		t.Fatalf("inventory after TTL expiry = %#v, want beta", got)
	}
	if calls := eng.listModelCalls(); calls != 2 {
		t.Fatalf("ListModels calls after cache expiry = %d, want 2", calls)
	}

	eng.setInventory([]engine.Model{{Name: "gamma"}}, nil)
	component.invalidateReportedModels()
	if got := component.enabledModels(); len(got) != 1 || got[0].Name != "gamma" {
		t.Fatalf("inventory after invalidation = %#v, want gamma", got)
	}
	if calls := eng.listModelCalls(); calls != 3 {
		t.Fatalf("ListModels calls after invalidation = %d, want 3", calls)
	}

	component.engineSnapshot()
	telemetry.hwMu.Lock()
	telemetry.hw.HasGPU = true
	telemetry.hwGeneration++
	telemetry.hwMu.Unlock()
	component.engineSnapshot()
	component.enginesMu.RLock()
	gotGeneration := component.engineSnapshotHardwareGen
	component.enginesMu.RUnlock()
	if gotGeneration != 1 {
		t.Fatalf("engine snapshot hardware generation = %d, want 1", gotGeneration)
	}
}

func TestInventoryInvalidationDoesNotWaitForEngineProbe(t *testing.T) {
	eng := &blockingInventoryEngine{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	component := newEngineComponent(
		[]engine.Engine{eng}, func() []engine.Engine { return nil }, &telemetryComponent{}, nil, "",
	)
	refreshed := make(chan struct{})
	go func() {
		component.refreshReportedModels()
		close(refreshed)
	}()
	select {
	case <-eng.started:
	case <-time.After(time.Second):
		t.Fatal("inventory probe did not start")
	}

	invalidated := make(chan struct{})
	go func() {
		component.invalidateReportedModels()
		close(invalidated)
	}()
	select {
	case <-invalidated:
	case <-time.After(100 * time.Millisecond):
		close(eng.release)
		<-invalidated
		<-refreshed
		t.Fatal("inventory invalidation waited for the engine probe")
	}

	close(eng.release)
	select {
	case <-refreshed:
	case <-time.After(time.Second):
		t.Fatal("inventory refresh did not finish after the probe was released")
	}
}

func TestPeriodicEngineScanRetainsUnavailableEngineDuringGracePeriod(t *testing.T) {
	eng := &inventoryTestEngine{models: []engine.Model{{Name: "cached-model"}}}
	detected := []engine.Engine{eng}
	component := newEngineComponent(
		nil, func() []engine.Engine { return detected }, &telemetryComponent{}, nil, "",
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

func TestConfiguredEngineProfileReplacesDetectedEngineAtSameEndpoint(t *testing.T) {
	endpoint := "http://127.0.0.1:8080"
	configured := engine.NewConfigured(engine.TemplateLlamaCPPXDNA2NPU, endpoint)
	detected := engine.NewLlamaCPP(endpoint)
	component := newEngineComponent(
		[]engine.Engine{configured},
		func() []engine.Engine { return []engine.Engine{detected} },
		&telemetryComponent{},
		nil, "",
	)
	component.rescanDetectedEngines()

	engines := component.engineSnapshot()
	if len(engines) != 1 || engines[0].Name() != engine.TemplateLlamaCPPXDNA2NPU {
		t.Fatalf("engine snapshot = %#v, want the configured XDNA profile only", engines)
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
	// mu protects the fake's probe result and count across test goroutines.
	mu             sync.Mutex
	models         []engine.Model
	err            error
	listModelCount int
}

type capabilityInventoryTestEngine struct {
	*inventoryTestEngine
	capabilities engine.Capabilities
}

func (e *capabilityInventoryTestEngine) Capabilities() engine.Capabilities {
	return e.capabilities
}

type blockingInventoryEngine struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (*blockingInventoryEngine) Name() string    { return "blocking" }
func (*blockingInventoryEngine) Available() bool { return true }
func (e *blockingInventoryEngine) ListModels() ([]engine.Model, error) {
	e.once.Do(func() { close(e.started) })
	<-e.release
	return []engine.Model{{Name: "model"}}, nil
}
func (*blockingInventoryEngine) Generate(string, string) (string, error) { return "", nil }
func (*blockingInventoryEngine) Chat(string, []engine.ChatMessage) (string, error) {
	return "", nil
}

func (e *inventoryTestEngine) Name() string    { return "test" }
func (e *inventoryTestEngine) Available() bool { return true }
func (e *inventoryTestEngine) ListModels() ([]engine.Model, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.listModelCount++
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
func (e *inventoryTestEngine) listModelCalls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.listModelCount
}

func containsEngineCapability(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
