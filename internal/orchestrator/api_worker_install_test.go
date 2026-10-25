package orchestrator

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
)

func TestWorkerInstallationUsesLongRunningNodeClient(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/engines/managed/install-worker" {
			t.Errorf("forwarded request = %s %s", r.Method, r.URL.Path)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != `{"source":"official"}` {
			t.Errorf("forwarded body = %q: %v", body, err)
		}
		time.Sleep(50 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"source":"official","version":"test","updated":true,"engines":[],"devices":[{"kind":"gpu","free_memory_mb":6000}]}`)
	}))
	defer upstream.Close()
	registry := cluster.NewRegistry()
	registry.Upsert(cluster.NodeStatus{ID: "worker", Address: upstream.URL})
	client := newNodeClient(nil, &http.Client{Timeout: 10 * time.Millisecond}, nil)
	client.modelPull = &http.Client{Timeout: time.Second}
	server := &Server{registry: registry, nodeClient: client}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/nodes/{id}/engines/managed/install-worker", server.handleNodeControl)
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/nodes/worker/engines/managed/install-worker",
		strings.NewReader(`{"source":"official"}`))
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"updated":true`) {
		t.Fatalf("worker installation proxy = %d: %s", response.Code, response.Body.String())
	}
	node, _ := registry.Get("worker")
	if len(node.Hardware.Devices) != 1 || node.Hardware.Devices[0].FreeMemoryMB != 6000 {
		t.Fatalf("installer did not publish measured memory: %+v", node.Hardware.Devices)
	}
}
