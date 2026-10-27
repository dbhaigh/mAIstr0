package orchestrator

import (
	"net/http"
	"reflect"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/inferroute"
)

func TestCopyRouterHeadersRemovesConnectionTokens(t *testing.T) {
	dst := http.Header{
		"Connection":   {"X-Old"},
		"X-Old":        {"remove"},
		"X-Existing":   {"preserve"},
		"Content-Type": {"existing"},
	}
	src := http.Header{
		"Connection":         {" X-Internal , x-extra", "Keep-Alive"},
		"Keep-Alive":         {"timeout=5"},
		"X-Internal":         {"remove"},
		"x-extra":            {"remove"},
		"Proxy-Authenticate": {"remove"},
		"Content-Type":       {"application/json"},
	}

	copyRouterHeaders(dst, src)

	want := http.Header{
		"X-Existing":   {"preserve"},
		"Content-Type": {"existing", "application/json"},
	}
	if !reflect.DeepEqual(dst, want) {
		t.Fatalf("copied headers = %#v, want %#v", dst, want)
	}
}

func TestRouterCandidateFilteringDoesNotMutateRegistry(t *testing.T) {
	server := newPerformanceOrchestrator(t, nil, 4)
	server.routerBreakers = newRouterCircuitBreakers()
	server.registry.Upsert(cluster.NodeStatus{
		ID: "router-test-node", Address: "http://worker", Healthy: true,
		MaxConcurrentTasks: 4, AvailableCapacity: 4,
		Engines: []engine.Status{{Name: "simulated", Ready: true}},
		Models: []engine.Model{
			{Name: "router-test-model", Engine: "simulated", Protocols: []string{"openai"}},
			{Name: "other-openai-model", Engine: "simulated", Protocols: []string{"openai"}},
			{Name: "other-ollama-model", Engine: "simulated", Protocols: []string{"ollama"}},
		},
	})

	assignment, err := server.assignRouterRequest("router-test-model", inferroute.ProtocolOpenAI, map[string]bool{})
	if err != nil {
		t.Fatal(err)
	}
	if assignment.NodeID != "router-test-node" || assignment.Model != "router-test-model" {
		t.Fatalf("assignment = %+v, want router-test-model on router-test-node", assignment)
	}
	node, ok := server.registry.Get("router-test-node")
	if !ok || len(node.Models) != 3 {
		t.Fatalf("registry models = %d, found=%t; want all 3 models retained", len(node.Models), ok)
	}
}
