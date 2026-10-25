package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hub"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

const benchmarkModel = "simulated-general"

func silenceBenchmarkLogs(b *testing.B) {
	b.Helper()
	previous := log.Writer()
	log.SetOutput(io.Discard)
	b.Cleanup(func() { log.SetOutput(previous) })
}

func newPerformanceWorker(b *testing.B) *httptest.Server {
	b.Helper()
	simulated := engine.NewSimulated()
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/execute":
			var request struct {
				Model  string `json:"model"`
				Prompt string `json:"prompt"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			output, err := simulated.GenerateWithOptions(r.Context(), request.Model, request.Prompt, engine.GenerationOptions{})
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"output": output})
		case "/generate":
			var request generateAPIRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			output, err := simulated.GenerateWithOptions(r.Context(), request.Model, request.Prompt, engine.GenerationOptions{})
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if r.URL.Query().Get("stream") == "true" {
				writeBenchmarkStreamEvent(w, map[string]string{"output": output})
				return
			}
			_ = json.NewEncoder(w).Encode(generateAPIResponse{Output: output})
		case "/proxy/v1/chat/completions":
			var request struct {
				Model    string `json:"model"`
				Messages []struct {
					Content string `json:"content"`
				} `json:"messages"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			prompt := ""
			if len(request.Messages) > 0 {
				prompt = request.Messages[len(request.Messages)-1].Content
			}
			output, err := simulated.GenerateWithOptions(r.Context(), request.Model, prompt, engine.GenerationOptions{})
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			chunk, _ := json.Marshal(map[string]any{
				"id": "chatcmpl-benchmark", "object": "chat.completion.chunk",
				"choices": []map[string]any{{"index": 0, "delta": map[string]string{"content": output}}},
			})
			_, _ = fmt.Fprintf(w, "data: %s\n\n", chunk)
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
		default:
			http.NotFound(w, r)
		}
	}))
	b.Cleanup(worker.Close)
	return worker
}

func writeBenchmarkStreamEvent(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "text/event-stream")
	data, err := json.Marshal(value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
}

func newPerformanceOrchestrator(b *testing.B, worker *httptest.Server, capacity int) *Server {
	b.Helper()
	registry := cluster.NewRegistry()
	taskStore, err := taskmgr.NewManagerWithStore(taskmgr.NewMemoryStore(), nil)
	if err != nil {
		b.Fatal(err)
	}
	memoryStore := memory.NewInMemoryStore()
	client := &http.Client{Timeout: 2 * time.Minute}
	node := &nodeClient{client: client, dispatcher: client, status: client}
	metrics := &operationalMetrics{}
	dispatcher := newDispatcherComponent(
		registry, taskStore, memoryStore, node, node, capacity, 2*time.Minute,
	)
	dispatcher.metrics = metrics
	server := &Server{
		registry: registry, tasks: taskStore, nodeClient: node, dispatcher: dispatcher,
		shardedModels: make(map[string]config.ShardedModelConfig),
		election:      newElectionComponent(registry, nil, "benchmark-orchestrator", 0, false),
		events:        hub.New(), routerEvents: newRouterEventLog(200),
		metrics: metrics, mem: memoryStore, mode: config.OrchestratorModeMinimal,
	}
	b.Cleanup(func() {
		server.background.Stop()
		server.dispatcher.close()
		if err := server.tasks.Close(); err != nil {
			b.Errorf("close benchmark task manager: %v", err)
		}
		if err := server.mem.Close(); err != nil {
			b.Errorf("close benchmark memory store: %v", err)
		}
	})
	if worker != nil {
		registry.Upsert(cluster.NodeStatus{
			ID: "simulated-node", Address: worker.URL, MaxConcurrentTasks: capacity,
			AvailableCapacity: capacity,
			Engines:           []engine.Status{{Name: "simulated", Ready: true}},
			Models: []engine.Model{{
				Name: benchmarkModel, Engine: "simulated", Tags: []string{"general"},
				Protocols: []string{"openai"},
			}},
		})
	}
	return server
}

func BenchmarkDispatchExecuteSubtaskSimulated(b *testing.B) {
	silenceBenchmarkLogs(b)
	worker := newPerformanceWorker(b)
	dispatcher := &dispatcherComponent{
		executeClient: worker.Client(), metrics: &operationalMetrics{},
	}
	assignment := scheduler.Assignment{
		Subtask: scheduler.Subtask{ID: "benchmark-subtask", TaskType: "general"},
		NodeID:  "simulated-node", Address: worker.URL, Model: benchmarkModel, Engine: "simulated",
	}
	const prompt = "Measure a simulated dispatch execution."

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		output, err, retryable := dispatcher.executeSubtask(context.Background(), assignment, prompt)
		if err != nil || retryable || output == "" {
			b.Fatalf("simulated execution failed: output=%q retryable=%t err=%v", output, retryable, err)
		}
	}
}

func BenchmarkDispatchWithContextSimulated(b *testing.B) {
	silenceBenchmarkLogs(b)
	worker := newPerformanceWorker(b)
	server := newPerformanceOrchestrator(b, worker, 1)
	assignment := scheduler.Assignment{
		Subtask: scheduler.Subtask{
			ID: "benchmark-subtask", Description: "Measure dispatch", TaskType: "general",
		},
		NodeID: "simulated-node", Address: worker.URL, Model: benchmarkModel, Engine: "simulated",
	}

	lastTaskID := ""
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		task, err := server.tasks.CreateChecked("dispatch benchmark", []scheduler.Assignment{assignment})
		if err != nil {
			b.Fatal(err)
		}
		lastTaskID = task.ID
		server.dispatcher.dispatchWithContext(context.Background(), task.ID, assignment)
	}
	b.StopTimer()
	task, ok := server.tasks.Get(lastTaskID)
	if !ok || len(task.Subtasks) != 1 || task.Subtasks[0].Status != taskmgr.StatusCompleted {
		b.Fatalf("simulated dispatch did not complete successfully: task=%+v", task)
	}
}

func BenchmarkTransparentRouterStreamingConcurrent100(b *testing.B) {
	silenceBenchmarkLogs(b)
	worker := newPerformanceWorker(b)
	server := newPerformanceOrchestrator(b, worker, 100)
	mux := server.Mux()
	requestBody := []byte(`{"model":"simulated-general","stream":true,"messages":[{"role":"user","content":"benchmark router stream"}]}`)
	const streams = 100

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		start := make(chan struct{})
		errors := make(chan error, streams)
		var wait sync.WaitGroup
		wait.Add(streams)
		for range streams {
			go func() {
				defer wait.Done()
				<-start
				request := httptest.NewRequest(http.MethodPost, "/router/v1/chat/completions", bytes.NewReader(requestBody))
				request.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				mux.ServeHTTP(response, request)
				if response.Code != http.StatusOK {
					errors <- fmt.Errorf("router returned %d: %s", response.Code, response.Body.String())
				}
			}()
		}
		close(start)
		wait.Wait()
		close(errors)
		for err := range errors {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(streams), "streams/op")
	b.ReportMetric(float64(b.N*streams)/b.Elapsed().Seconds(), "streams/s")
}

func BenchmarkCompatAPIStreamingSimulated(b *testing.B) {
	silenceBenchmarkLogs(b)
	worker := newPerformanceWorker(b)
	server := newPerformanceOrchestrator(b, worker, 16)
	mux := server.Mux()
	requestBody := []byte(`{"model":"simulated-general","stream":true,"messages":[{"role":"user","content":"benchmark compatibility stream"}]}`)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(requestBody))
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			b.Fatalf("compat API returned %d: %s", response.Code, response.Body.String())
		}
	}
}

func newSnapshotBenchmarkServer(b *testing.B) *Server {
	b.Helper()
	silenceBenchmarkLogs(b)
	server := newPerformanceOrchestrator(b, nil, 16)
	for nodeIndex := range 100 {
		models := make([]engine.Model, 4)
		for modelIndex := range models {
			models[modelIndex] = engine.Model{
				Name: "benchmark-model-" + strconv.Itoa(modelIndex), Engine: "simulated",
				Tags: []string{"general"}, Protocols: []string{"openai"},
			}
		}
		server.registry.Upsert(cluster.NodeStatus{
			ID: "node-" + strconv.Itoa(nodeIndex), Address: "http://simulated-node",
			MaxConcurrentTasks: 8, AvailableCapacity: 8,
			Engines: []engine.Status{{Name: "simulated", Ready: true}}, Models: models,
		})
	}
	for taskIndex := range 50 {
		assignment := scheduler.Assignment{
			Subtask: scheduler.Subtask{
				ID: "subtask-" + strconv.Itoa(taskIndex), Description: "snapshot fixture", TaskType: "general",
			},
			NodeID: "node-" + strconv.Itoa(taskIndex), Address: "http://simulated-node",
			Model: benchmarkModel, Engine: "simulated",
		}
		if _, err := server.tasks.CreateChecked("snapshot fixture task", []scheduler.Assignment{assignment}); err != nil {
			b.Fatal(err)
		}
	}
	return server
}

func BenchmarkBuildSnapshot(b *testing.B) {
	server := newSnapshotBenchmarkServer(b)
	b.ReportAllocs()
	b.ReportMetric(100, "nodes/op")
	b.ReportMetric(50, "tasks/op")
	b.ResetTimer()
	for range b.N {
		if _, err := json.Marshal(server.snapshot()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPublishSnapshotRequests(b *testing.B) {
	server := newSnapshotBenchmarkServer(b)
	b.Run("NoSubscribers", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			server.publishSnapshot()
		}
	})
	b.Run("SubscribedHandler", func(b *testing.B) {
		stream := httptest.NewServer(server.events)
		defer stream.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, stream.URL, nil)
		if err != nil {
			b.Fatal(err)
		}
		response, err := stream.Client().Do(request)
		if err != nil {
			b.Fatal(err)
		}
		defer response.Body.Close()
		drained := make(chan struct{})
		go func() {
			defer close(drained)
			_, _ = io.Copy(io.Discard, response.Body)
		}()
		defer func() {
			cancel()
			_ = response.Body.Close()
			<-drained
		}()
		deadline := time.Now().Add(time.Second)
		for !server.events.HasSubscribers() {
			if time.Now().After(deadline) {
				b.Fatal("snapshot subscriber was not registered")
			}
			time.Sleep(time.Millisecond)
		}
		handlerRequest := httptest.NewRequest(http.MethodPost, "/benchmark/snapshot", nil)
		for _, fixture := range []struct {
			name    string
			publish func()
		}{
			{name: "SynchronousBuildBaseline", publish: func() {
				data, err := json.Marshal(server.snapshot())
				if err != nil {
					b.Fatal(err)
				}
				server.events.Broadcast(data)
			}},
			{name: "QueuedPublisherRequest", publish: server.publishSnapshot},
		} {
			b.Run(fixture.name, func(b *testing.B) {
				handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					fixture.publish()
					w.WriteHeader(http.StatusNoContent)
				})
				response := httptest.NewRecorder()
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					handler.ServeHTTP(response, handlerRequest)
				}
			})
		}
	})
}
