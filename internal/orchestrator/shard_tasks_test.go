package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/inferroute"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/shard"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

func addTaskTestShardGroup(s *Server, address string) ShardGroup {
	group := ShardGroup{
		ID: "workflow-group", State: ShardGroupReady,
		Model:   shard.ModelProfile{Name: "cluster-model", RequestedContext: 4096},
		Members: []ShardGroupMember{{NodeID: "head", Address: address, Role: "head", EngineID: "managed-head"}},
	}
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	s.shards.mu.Lock()
	s.shards.groups[group.ID] = group
	s.shards.requestGates[group.ID] = gate
	s.shards.mu.Unlock()
	s.registry.Upsert(cluster.NodeStatus{
		ID: "head", Address: address, Healthy: true, MaxConcurrentTasks: 1,
		Engines: []engine.Status{{Name: "unrelated", Ready: false}},
	})
	return group
}

func TestShardOnlyClusterPlansExecutesAndSynthesizes(t *testing.T) {
	var calls atomic.Int32
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/engines/managed/managed-head/proxy/v1/chat/completions" || r.Method != http.MethodPost {
			t.Errorf("unexpected standalone request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		var request struct {
			Model    string `json:"model"`
			Stream   bool   `json:"stream"`
			Messages []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Model != "cluster-model" || len(request.Messages) != 1 || !request.Stream {
			t.Errorf("invalid managed request: %+v, error %v", request, err)
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		call := calls.Add(1)
		if call == 2 || call == 3 {
			if !strings.HasPrefix(r.Header.Get(inferroute.HeaderRequestID), "task-1:") ||
				!strings.HasPrefix(r.Header.Get(inferroute.HeaderAttemptID), "task-1:") {
				t.Errorf("managed task request lost dispatch IDs: %v", r.Header)
			}
		}
		output := "Completed composer research"
		if strings.Contains(request.Messages[0].Content, "mAIstr0 workflow planner") {
			output = `{"subtasks":[{"id":"research","description":"Research ten composers","task_type":"general","expected_output":"Composer biographies"},{"id":"rank","description":"Rank the composers","task_type":"general","depends_on":["research"],"expected_output":"Ranked biographies"}]}`
		}
		chunk, _ := json.Marshal(map[string]any{
			"choices": []any{map[string]any{"delta": map[string]string{"content": output}}},
		})
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", chunk)
	}))
	defer worker.Close()
	s := newTestServer(t)
	addTaskTestShardGroup(s, worker.URL)
	// Simulate the persistent reservation held by the ready shard group.
	gate := s.dispatcher.capacityGate.(*dispatchCapacityGate).nodeDispatchGate("head", 1)
	gate.mu.Lock()
	gate.active = 1
	gate.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	task, err := s.createTask(ctx, createTaskRequest{
		Description: "Generate biographies of the ten best classical composers, ranked with examples",
		PlanMode:    "auto",
	})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := s.tasks.Wait(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != taskmgr.StatusCompleted || completed.Result != "Completed composer research" {
		t.Fatalf("shard workflow failed: %+v", completed)
	}
	s.dispatcher.pool.Close()
	if calls.Load() != 4 {
		t.Fatalf("managed calls = %d, want planning, two subtasks, synthesis", calls.Load())
	}
	if s.dispatcher.capacityGate.(*dispatchCapacityGate).Active() != 0 {
		t.Fatal("global capacity leaked")
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.active != 1 {
		t.Fatalf("group member reservation changed: %d", gate.active)
	}
}

func TestShardTaskAvailabilityAndNoStandaloneFallback(t *testing.T) {
	for _, state := range []ShardGroupState{ShardGroupFailed, ShardGroupDegraded, ShardGroupStartingHead} {
		t.Run(string(state), func(t *testing.T) {
			s := newTestServer(t)
			group := addTaskTestShardGroup(s, "http://127.0.0.1:1")
			s.shards.mu.Lock()
			group.State = state
			s.shards.groups[group.ID] = group
			s.shards.mu.Unlock()
			node, _ := s.registry.Get("head")
			node.Models = []engine.Model{{Name: group.Model.Name, Tags: []string{"general"}}}
			s.registry.Upsert(node)
			_, err := s.createTask(context.Background(), createTaskRequest{Description: "Write composer biographies", PlanMode: "auto"})
			if err == nil || !strings.Contains(err.Error(), "shard group is ready") {
				t.Fatalf("missing actionable availability error: %v", err)
			}
			_, err = s.nodeClient.callNodeGenerateWithOptionsContext(context.Background(), node.Address, group.Model.Name, "hello", 0, 0)
			if err == nil || !strings.Contains(err.Error(), string(state)) {
				t.Fatalf("unavailable group fell through to standalone generation: %v", err)
			}
		})
	}
}

func TestShardTaskRequestCancellation(t *testing.T) {
	s := newTestServer(t)
	group := addTaskTestShardGroup(s, "http://127.0.0.1:1")
	release, err := s.shards.AcquireRequest(context.Background(), group.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = s.nodeClient.callNodeGenerateWithOptionsContext(ctx, group.Members[0].Address, group.Model.Name, "hello", 0, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled queued generation: %v", err)
	}
}

func TestShardTaskUpstreamFailureClassification(t *testing.T) {
	var status atomic.Int32
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if code := int(status.Load()); code != 0 {
			http.Error(w, "upstream failure", code)
			return
		}
		_, _ = w.Write([]byte(`{"choices":[]}`))
	}))
	defer worker.Close()
	s := newTestServer(t)
	group := addTaskTestShardGroup(s, worker.URL)
	assignment := scheduler.Assignment{NodeID: "head", Address: worker.URL, Model: group.Model.Name, Engine: "shard-group:" + group.ID}
	for _, code := range []int{http.StatusBadRequest, http.StatusServiceUnavailable, 0} {
		status.Store(int32(code))
		out := s.dispatcher.executeSubtaskWithIDs(context.Background(), assignment, "hello", "request", "attempt")
		if out.err == nil {
			t.Fatalf("upstream %d unexpectedly succeeded", code)
		}
		if code == 0 {
			if !strings.Contains(out.err.Error(), "no assistant content") {
				t.Fatalf("empty response error = %v", out.err)
			}
			continue
		}
		_, retryable := classifyDispatchStatus(code, []byte("upstream failure"))
		if out.httpStatus != code || out.retryable != retryable {
			t.Fatalf("upstream %d classification = %+v", code, out)
		}
	}
}

func TestShardGenerationHonorsThirtyMinuteConfiguration(t *testing.T) {
	s, err := NewWithConfig(config.OrchestratorConfig{
		Harness: "deepseek", MemoryPath: filepath.Join(t.TempDir(), "orchestrator.db"),
		DiscoveryEnabled:       true,
		DispatchTimeoutSeconds: 1800,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if s.nodeClient.dispatcher.Timeout != 30*time.Minute || s.dispatcher.dispatchTimeout != 30*time.Minute {
		t.Fatalf("configured timeout was not propagated: client=%s dispatch=%s", s.nodeClient.dispatcher.Timeout, s.dispatcher.dispatchTimeout)
	}
	group := addTaskTestShardGroup(s, "http://head")
	s.nodeClient.dispatcher.Transport = shardGenerationTransport(func(r *http.Request) (*http.Response, error) {
		deadline, ok := r.Context().Deadline()
		if !ok || time.Until(deadline) < 29*time.Minute || time.Until(deadline) > 30*time.Minute {
			t.Errorf("generation deadline = %v, want configured thirty minutes", deadline)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(strings.NewReader("data: {\"choices\":[{\"delta\":{\"content\":\"spring\"}}]}\n\ndata: [DONE]\n\n")),
		}, nil
	})
	output, err := s.nodeClient.generateShardGroup(context.Background(), group, "http://head", "hello", 0, 100)
	if err != nil || output != "spring" {
		t.Fatalf("configured generation=%q %v", output, err)
	}
}

type shardGenerationTransport func(*http.Request) (*http.Response, error)

func (f shardGenerationTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestShardGenerationStreamValidation(t *testing.T) {
	for _, test := range []struct {
		name, stream, want, wantErr string
	}{
		{"content and reasoning", "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"private reasoning\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"hello \"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"spring\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", "hello spring", ""},
		{"CRLF and comments", ": keepalive\r\n\r\ndata: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\r\n\r\ndata: [DONE]\r\n\r\n", "hi", ""},
		{"multiline data", "data: {\"choices\":\ndata: [{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]", "hi", ""},
		{"truncated stream", "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n", "", "ended before completion"},
		{"token limit", "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"},\"finish_reason\":\"length\"}]}\n\n", "", "token limit"},
		{"upstream error", "data: {\"error\":{\"message\":\"private upstream details\"}}\n\n", "", "stream error"},
		{"invalid JSON", "data: invalid\n\n", "", "decode shard stream event"},
		{"reasoning without answer", "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"private reasoning\"}}]}\n\ndata: [DONE]\n\n", "", "no assistant content"},
		{"oversized event", "data: " + strings.Repeat("x", 1<<20) + "\n\n", "", "read shard generation stream"},
		{"oversized response", strings.Repeat(": "+strings.Repeat("x", 1020)+"\n\n", 21000), "", "response size limit"},
	} {
		t.Run(test.name, func(t *testing.T) {
			completed := false
			got, err := readShardGenerationStream(strings.NewReader(test.stream), func(chunks, output, reasoning int, done bool) { completed = done })
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) || got != "" {
					t.Fatalf("result=%q error=%v", got, err)
				}
				return
			}
			if err != nil || got != test.want || !completed {
				t.Fatalf("result=%q completed=%v error=%v", got, completed, err)
			}
		})
	}
}

func TestShardGenerationReadsProgressBeforeCompletion(t *testing.T) {
	reader, writer := io.Pipe()
	defer closeutil.Discard(reader)
	defer closeutil.Discard(writer)
	progress := make(chan int, 2)
	done := make(chan error, 1)
	go func() {
		output, err := readShardGenerationStream(reader, func(chunks, output, reasoning int, finished bool) {
			if !finished {
				progress <- output
			}
		})
		if err == nil && output != "spring" {
			err = fmt.Errorf("output=%q", output)
		}
		done <- err
	}()
	_, err := io.WriteString(writer, "data: {\"choices\":[{\"delta\":{\"content\":\"spring\"}}]}\n\n")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case n := <-progress:
		if n != 6 {
			t.Fatalf("progress bytes=%d", n)
		}
	case <-time.After(time.Second):
		t.Fatal("stream buffered until generation finished")
	}
	if _, err := io.WriteString(writer, "data: [DONE]\n\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("stream did not complete")
	}
}

func TestShardGenerationBudgetIncludesQueueAndCallerCancellation(t *testing.T) {
	s := newTestServer(t)
	group := addTaskTestShardGroup(s, "http://127.0.0.1:1")
	s.nodeClient.dispatcher.Timeout = 25 * time.Millisecond
	release, err := s.shards.AcquireRequest(context.Background(), group.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	start := time.Now()
	_, err = s.nodeClient.generateShardGroup(context.Background(), group, group.Members[0].Address, "hi", 0, 0)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "request slot") || time.Since(start) > time.Second {
		t.Fatalf("queue was not bounded: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = s.nodeClient.generateShardGroup(ctx, group, group.Members[0].Address, "hi", 0, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation=%v", err)
	}
}

func TestShardGenerationSlowHeadAndReadDeadline(t *testing.T) {
	var hold atomic.Bool
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
		if hold.Load() {
			_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(60 * time.Millisecond):
		}
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"spring\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer worker.Close()
	s := newTestServer(t)
	group := addTaskTestShardGroup(s, worker.URL)
	s.nodeClient.dispatcher.Timeout = time.Second
	output, err := s.nodeClient.generateShardGroup(context.Background(), group, worker.URL, "hello", 0, 100)
	if err != nil || output != "spring" {
		t.Fatalf("slow successful head=%q %v", output, err)
	}
	hold.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	output, err = s.nodeClient.generateShardGroup(ctx, group, worker.URL, "hello", 0, 100)
	if !errors.Is(err, context.DeadlineExceeded) || output != "" || !strings.Contains(err.Error(), "dispatch_timeout_seconds") {
		t.Fatalf("stream read timeout=%q %v", output, err)
	}
	release, err := s.shards.AcquireRequest(context.Background(), group.ID)
	if err != nil {
		t.Fatalf("request gate leaked: %v", err)
	}
	release()
}

func BenchmarkShardGenerationStream(b *testing.B) {
	payload := "data: {\"choices\":[{\"delta\":{\"content\":\"spring\"}}]}\n\ndata: [DONE]\n\n"
	b.ReportAllocs()
	for b.Loop() {
		if _, err := readShardGenerationStream(strings.NewReader(payload), func(int, int, int, bool) {}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkShardSchedulerNodes(b *testing.B) {
	groups := []ShardGroup{{ID: "group", State: ShardGroupReady, Model: shard.ModelProfile{Name: "model"}, Members: []ShardGroupMember{{Role: "head", NodeID: "head", Address: "http://head"}}}}
	node := cluster.NodeStatus{ID: "head", Address: "http://head", Healthy: true}
	b.ReportAllocs()
	for b.Loop() {
		shardSchedulerNodes([]cluster.NodeStatus{node}, groups)
	}
}
