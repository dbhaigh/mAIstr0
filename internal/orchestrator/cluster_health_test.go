package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/workpool"
)

func TestClusterHealthEndpointAndSnapshotShareCapacityView(t *testing.T) {
	srv := newTestServer(t)
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "worker", Address: "https://worker", MaxConcurrentTasks: 2,
		AvailableCapacity: 2, Engines: []engine.Status{
			{Name: "ready-engine", Ready: true},
			{Name: "stopped-engine", Ready: false},
		},
		TelemetryAt: time.Now(),
	})
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "overloaded", Address: "https://overloaded", MaxConcurrentTasks: 1,
		ActiveTasks: 2, AvailableCapacity: 0,
	})
	reservation, err := srv.dispatcher.reservations().Reserve(context.Background(), []workpool.CapacityTarget{{
		NodeID: "worker", Address: "https://worker",
	}})
	if err != nil {
		t.Fatalf("reserve worker capacity: %v", err)
	}
	defer reservation.Release()

	response := httptest.NewRecorder()
	srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/cluster/health", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("health endpoint returned %d: %s", response.Code, response.Body.String())
	}
	var endpoint ClusterHealth
	if err := json.NewDecoder(response.Body).Decode(&endpoint); err != nil {
		t.Fatalf("decode cluster health: %v", err)
	}
	if len(endpoint.Nodes) != 2 {
		t.Fatalf("health view contains %d nodes, want 2", len(endpoint.Nodes))
	}
	var worker, overloaded *NodeCapacityHealth
	for i := range endpoint.Nodes {
		switch endpoint.Nodes[i].NodeID {
		case "worker":
			worker = &endpoint.Nodes[i]
		case "overloaded":
			overloaded = &endpoint.Nodes[i]
		}
	}
	if worker == nil || worker.Active != 1 || worker.Max != 2 || worker.Remaining != 1 {
		t.Fatalf("worker capacity = %+v, want 1 active, 2 max, 1 remaining", worker)
	}
	if worker.TelemetryState != "fresh" {
		t.Fatalf("worker telemetry state = %q, want fresh", worker.TelemetryState)
	}
	if overloaded == nil || !overloaded.Oversubscribed || overloaded.Active != 2 || overloaded.Remaining != 0 {
		t.Fatalf("overloaded node health = %+v, want oversubscription warning", overloaded)
	}
	if endpoint.GlobalDispatch.ActiveSlots != 1 || endpoint.GlobalDispatch.QueueDepth != 0 {
		t.Fatalf("global dispatch view = %+v, want one active slot and empty queue", endpoint.GlobalDispatch)
	}
	if endpoint.EngineReadiness.ReadyEngines != 1 || endpoint.EngineReadiness.TotalEngines != 2 {
		t.Fatalf("engine readiness = %+v, want 1 of 2 engines ready", endpoint.EngineReadiness)
	}
	if endpoint.TelemetryFreshness.FreshNodes != 1 || endpoint.TelemetryFreshness.UnavailableNodes != 1 {
		t.Fatalf("telemetry freshness = %+v", endpoint.TelemetryFreshness)
	}
	if len(endpoint.Warnings) != 1 || !strings.Contains(endpoint.Warnings[0], "overloaded") {
		t.Fatalf("capacity warnings = %v, want warning for overloaded node", endpoint.Warnings)
	}

	snapshot := srv.snapshot()
	if snapshot.ClusterHealth.GlobalDispatch != endpoint.GlobalDispatch {
		t.Fatalf("snapshot global dispatch = %+v, endpoint = %+v", snapshot.ClusterHealth.GlobalDispatch, endpoint.GlobalDispatch)
	}
	for _, node := range snapshot.ClusterHealth.Nodes {
		if node.NodeID == "worker" && (node.Active != worker.Active || node.Remaining != worker.Remaining) {
			t.Fatalf("snapshot worker capacity = %+v, endpoint = %+v", node, worker)
		}
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal SSE snapshot: %v", err)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		t.Fatalf("decode SSE snapshot envelope: %v", err)
	}
	if _, ok := envelope["cluster_health"]; !ok {
		t.Fatal("SSE snapshot omitted cluster_health")
	}
}
