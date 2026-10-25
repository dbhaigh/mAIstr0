package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/nodeagent/supervisor"
	"github.com/maistr0/maistr0/internal/shard"
)

func TestAutomaticShardHeadRestoresSavedModel(t *testing.T) {
	expected := testShardRequest().Model
	expected.Name, expected.Architecture = "Llama-3.2-3B", "llama"
	other := expected
	other.Name, other.Architecture, other.LayerCount = "gemma4:26b", "gemma4", 30
	for _, test := range []struct {
		name                     string
		restore, available, busy bool
		wantErr                  string
	}{
		{"inference rejects wrong weights", false, true, false, "head is configured"},
		{"start restores correct weights", true, true, false, ""},
		{"missing weights", true, false, false, "no longer available"},
		{"active engine cannot be reconfigured", true, true, true, "restore saved head model"},
	} {
		t.Run(test.name, func(t *testing.T) {
			actual := other
			configured := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/engines/managed/maistr0-auto-head/profile":
					if r.URL.Query().Get("requested_context") != "128" {
						t.Errorf("context=%s", r.URL.RawQuery)
					}
					_ = json.NewEncoder(w).Encode(actual)
				case "/api/engines/managed/head-models":
					models := []supervisor.LocalHeadModel{}
					if test.available {
						models = append(models, supervisor.LocalHeadModel{ID: "opaque-id", Name: expected.Name, Profile: expected})
					}
					_ = json.NewEncoder(w).Encode(supervisor.LocalHeadModels{Models: models})
				case "/api/engines/managed/configure-head":
					if test.busy {
						http.Error(w, "active engines", http.StatusConflict)
						return
					}
					var body struct {
						ID      string `json:"model_id"`
						Context int    `json:"requested_context"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.ID != "opaque-id" || body.Context != expected.RequestedContext {
						t.Errorf("configuration=%+v error=%v", body, err)
					}
					configured++
					actual = expected
					_ = json.NewEncoder(w).Encode(supervisor.Status{ID: "maistr0-auto-head", State: "stopped"})
				default:
					t.Errorf("unexpected model request: %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client := newNodeClient(nil, nil, nil)
			err := client.VerifyModel(context.Background(), server.URL, "maistr0-auto-head", expected, test.restore)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("error=%v", err)
				}
				if configured != 0 {
					t.Fatal("failed verification reconfigured the head")
				}
			} else {
				if err != nil || configured != 1 {
					t.Fatalf("configured=%d err=%v", configured, err)
				}
				if err := client.VerifyModel(context.Background(), server.URL, "maistr0-auto-head", expected, false); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestShardModelMetadataMustMatch(t *testing.T) {
	expected := testShardRequest().Model
	expected.Name, expected.Architecture = "llama", "llama"
	expected.FileSizeBytes, expected.VocabSize = 1000, 1024
	for _, test := range []struct {
		name   string
		change func(*shard.ModelProfile)
	}{
		{"name", func(p *shard.ModelProfile) { p.Name = "gemma" }},
		{"architecture", func(p *shard.ModelProfile) { p.Architecture = "gemma4" }},
		{"layers", func(p *shard.ModelProfile) { p.LayerCount++ }},
		{"hidden", func(p *shard.ModelProfile) { p.HiddenSize++ }},
		{"heads", func(p *shard.ModelProfile) { p.KVHeads++ }},
		{"head dimension", func(p *shard.ModelProfile) { p.HeadDim++ }},
		{"file size", func(p *shard.ModelProfile) { p.FileSizeBytes++ }},
		{"quantization", func(p *shard.ModelProfile) { p.Quantization = "f16" }},
		{"vocabulary", func(p *shard.ModelProfile) { p.VocabSize++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			actual := expected
			test.change(&actual)
			if matchingShardModel(expected, actual) {
				t.Fatal("mismatched weights accepted")
			}
		})
	}
	if !matchingShardModel(expected, expected) {
		t.Fatal("matching profile rejected")
	}
}

func TestShardGroupModelMismatchBlocksLaunchAndDegradesRunningGroup(t *testing.T) {
	m, _, engines, capacity, _ := newTestShardManager(t, "")
	engines.modelError = errors.New("head configured for gemma, expected llama")
	group, err := m.Create(context.Background(), testShardRequest())
	if err == nil || group.State != ShardGroupFailed || len(engines.events) != 0 || capacity.releases != 1 {
		t.Fatalf("mismatched launch=%+v err=%v events=%v", group, err, engines.events)
	}
	engines.modelError = nil
	group, err = m.Start(context.Background(), group.ID)
	if err != nil {
		t.Fatal(err)
	}
	wantMembers := append([]ShardGroupMember(nil), group.Members...)
	engines.modelError = errors.New("head configured for wrong weights")
	if err := m.CheckHealth(context.Background()); err == nil {
		t.Fatal("health check accepted incorrect weights")
	}
	current, _ := m.Get(group.ID)
	if current.State != ShardGroupDegraded || !reflect.DeepEqual(current.Members, wantMembers) {
		t.Fatalf("mismatched runtime not degraded: %+v", current)
	}
}

func TestShardGroupRecoveryRejectsReconfiguredHead(t *testing.T) {
	manager, registry, engines, _, store := newTestShardManager(t, "")
	group, err := manager.Create(context.Background(), testShardRequest())
	if err != nil {
		t.Fatal(err)
	}

	engines.modelError = errors.New("expected llama, head configured for gemma")
	recovered, err := NewShardManager(store, registry, engines, &testShardCapacity{}, testShardPlanner)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.Recover(context.Background()); err == nil {
		t.Fatal("recovery accepted a different head model")
	}
	current, _ := recovered.Get(group.ID)
	if current.State != ShardGroupFailed || !strings.Contains(current.Error, "head configured for gemma") {
		t.Fatalf("recovered mismatched group: %+v", current)
	}
}

func BenchmarkAutomaticShardHeadVerification(b *testing.B) {
	profile := shard.ModelProfile{Name: "llama", Architecture: "llama", LayerCount: 28}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(profile)
	}))
	defer server.Close()
	client := newNodeClient(nil, nil, nil)
	b.ReportAllocs()
	for b.Loop() {
		if err := client.VerifyModel(context.Background(), server.URL, "maistr0-auto-head", profile, false); err != nil {
			b.Fatal(err)
		}
	}
}
