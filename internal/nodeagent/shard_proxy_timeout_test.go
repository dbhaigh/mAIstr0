package nodeagent

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/config"
)

type shardStartupTransport func(*http.Request) (*http.Response, error)

func (f shardStartupTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestTaskSubmissionProxyDoesNotOverrideOrchestratorBudget(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{})
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Error(err)
		}
	})
	agent.orchestratorAddr = "http://orchestrator"
	agent.dispatchClient = &http.Client{Timeout: 10 * time.Millisecond, Transport: shardStartupTransport(func(r *http.Request) (*http.Response, error) {
		if _, ok := r.Context().Deadline(); ok {
			t.Error("task submission inherited a fixed proxy timeout")
		}
		select {
		case <-r.Context().Done():
			return nil, r.Context().Err()
		case <-time.After(60 * time.Millisecond):
		}
		return &http.Response{StatusCode: http.StatusCreated, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"id":"task-1"}`))}, nil
	})}
	w := httptest.NewRecorder()
	agent.handleAPIOrchestratorProxy(w, httptest.NewRequest(http.MethodPost, "/api/tasks", strings.NewReader(`{"plan_mode":"auto"}`)))
	if w.Code != http.StatusCreated {
		t.Fatalf("slow planner proxy=%d %s", w.Code, w.Body.String())
	}
	if agent.dispatchClient.Timeout != 10*time.Millisecond {
		t.Fatal("shared dispatch timeout was mutated")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w = httptest.NewRecorder()
	agent.handleAPIOrchestratorProxy(w, httptest.NewRequest(http.MethodPost, "/api/tasks", nil).WithContext(ctx))
	if w.Code != http.StatusBadGateway {
		t.Fatalf("canceled proxy=%d", w.Code)
	}
}

func TestFailedShardDeletionProxyPreservesMethodAndErrors(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{})
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Error(err)
		}
	})
	agent.orchestratorAddr = "http://orchestrator"
	for _, status := range []int{http.StatusNoContent, http.StatusBadGateway} {
		agent.dispatchClient = &http.Client{Transport: shardStartupTransport(func(r *http.Request) (*http.Response, error) {
			if r.Method != http.MethodDelete || r.URL.String() != "http://orchestrator/api/shards/failed-group" {
				t.Errorf("delete proxy request = %s %s", r.Method, r.URL)
			}
			body := ""
			if status != http.StatusNoContent {
				body = `{"error":"worker cleanup failed"}`
			}
			return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
		})}
		response := httptest.NewRecorder()
		agent.handleAPIOrchestratorProxy(response, httptest.NewRequest(http.MethodDelete, "/api/shards/failed-group", nil))
		if response.Code != status {
			t.Fatalf("delete status = %d, want %d: %s", response.Code, status, response.Body.String())
		}
		if status == http.StatusBadGateway && !strings.Contains(response.Body.String(), "worker cleanup failed") {
			t.Fatalf("cleanup error was lost: %s", response.Body.String())
		}
	}
}

func TestShardCreationProxyAllowsLongModelStartupAndPreservesCancellation(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{})
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Error(err)
		}
	})
	agent.orchestratorAddr = "http://orchestrator"
	agent.peerClient = &http.Client{Timeout: 10 * time.Millisecond, Transport: shardStartupTransport(func(r *http.Request) (*http.Response, error) {
		t.Error("shard creation used short peer client")
		return nil, errors.New("wrong client")
	})}
	agent.dispatchClient = &http.Client{Timeout: time.Second, Transport: shardStartupTransport(func(r *http.Request) (*http.Response, error) {
		if err := r.Context().Err(); err != nil {
			return nil, err
		}
		deadline, ok := r.Context().Deadline()
		if !ok || time.Until(deadline) < 14*time.Minute || time.Until(deadline) > 15*time.Minute {
			t.Errorf("shard creation deadline=%v", deadline)
		}
		return &http.Response{StatusCode: http.StatusCreated, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"state":"ready"}`))}, nil
	})}
	response := httptest.NewRecorder()
	agent.handleAPIOrchestratorProxy(response, httptest.NewRequest(http.MethodPost, "/api/shards", strings.NewReader(`{}`)))
	if response.Code != http.StatusCreated {
		t.Fatalf("shard proxy=%d: %s", response.Code, response.Body.String())
	}
	if agent.dispatchClient.Timeout != time.Second {
		t.Fatal("shard proxy mutated shared HTTP client")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	response = httptest.NewRecorder()
	agent.handleAPIOrchestratorProxy(response, httptest.NewRequest(http.MethodPost, "/api/shards", strings.NewReader(`{}`)).WithContext(ctx))
	if response.Code != http.StatusBadGateway {
		t.Fatalf("canceled shard proxy=%d", response.Code)
	}
}

func TestShardLifecycleActionsProxyWithStartupBudget(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{})
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Error(err)
		}
	})
	agent.orchestratorAddr = "http://orchestrator"
	agent.peerClient = &http.Client{Transport: shardStartupTransport(func(r *http.Request) (*http.Response, error) {
		t.Error("lifecycle action used short peer client")
		return nil, errors.New("wrong client")
	})}
	for _, action := range []string{"start", "stop", "restart", "members"} {
		t.Run(action, func(t *testing.T) {
			path := "/api/shards/group/" + action
			agent.dispatchClient = &http.Client{Timeout: time.Second, Transport: shardStartupTransport(func(r *http.Request) (*http.Response, error) {
				if r.Method != http.MethodPost || r.URL.String() != "http://orchestrator"+path {
					t.Errorf("action forwarding: %s %s", r.Method, r.URL)
				}
				deadline, ok := r.Context().Deadline()
				if action == "members" && ok {
					t.Errorf("membership drain inherited a fixed proxy deadline: %v", deadline)
				}
				if action != "members" && (!ok || time.Until(deadline) < 14*time.Minute) {
					t.Errorf("short action deadline: %v", deadline)
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"id":"group","state":"ready"}`))}, nil
			})}
			w := httptest.NewRecorder()
			agent.handleAPIOrchestratorProxy(w, httptest.NewRequest(http.MethodPost, path, nil))
			if w.Code != http.StatusOK {
				t.Fatalf("action response: %d %s", w.Code, w.Body.String())
			}
		})
	}
}
