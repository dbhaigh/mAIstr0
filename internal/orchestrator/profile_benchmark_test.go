package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
	"github.com/maistr0/maistr0/internal/memory"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/shard"
	"github.com/maistr0/maistr0/internal/testutil"
)

func profileWorker(b *testing.B, id string, delay time.Duration, fail bool, calls *atomic.Int64) *httptest.Server {
	b.Helper()
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/status" {
			_ = json.NewEncoder(w).Encode(cluster.NodeStatus{
				ID: id, Address: "http://" + r.Host, Healthy: true,
				MaxConcurrentTasks: 16, AvailableCapacity: 16,
				Engines: []engine.Status{{Name: "simulated", Ready: true}},
				Models:  []engine.Model{{Name: benchmarkModel, Engine: "simulated", Tags: []string{"general"}, Protocols: []string{"openai"}}},
			})
			return
		}
		if !fail || r.URL.Path == "/execute" {
			calls.Add(1)
		}
		if delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-r.Context().Done():
				return
			case <-timer.C:
			}
		}
		if fail && r.URL.Path == "/execute" {
			http.Error(w, "synthetic retry", http.StatusServiceUnavailable)
			return
		}
		if r.URL.Path == "/execute" || r.URL.Path == "/generate" {
			_, _ = io.WriteString(w, `{"output":"synthetic result"}`)
			return
		}
		var request struct {
			Stream bool `json:"stream"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid synthetic request", http.StatusBadRequest)
			return
		}
		if !request.Stream {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"synthetic result"}}]}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		for range 4 {
			_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"synthetic\"}}]}\n\n")
			w.(http.Flusher).Flush()
			if delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-r.Context().Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	b.Cleanup(worker.Close)
	return worker
}

func BenchmarkProfileRouting(b *testing.B) {
	for _, fixture := range []struct {
		name, path string
		stream     bool
		delay      time.Duration
	}{{"Direct", "/api/generate", false, 0},
		{"Transparent", "/router/v1/chat/completions", false, 0},
		{"SlowStream", "/router/v1/chat/completions", true, 2 * time.Millisecond}} {
		b.Run(fixture.name, func(b *testing.B) {
			silenceBenchmarkLogs(b)
			var calls atomic.Int64
			worker := profileWorker(b, "simulated-node", fixture.delay, false, &calls)
			server := newPerformanceOrchestrator(b, worker, 16)
			mux := server.Mux()
			body := []byte(fmt.Sprintf(`{"model":"simulated-general","stream":%t,"prompt":"synthetic","messages":[{"role":"user","content":"synthetic"}]}`, fixture.stream))
			latencies := &testutil.Latencies{}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				request := httptest.NewRequest(http.MethodPost, fixture.path, bytes.NewReader(body))
				response := httptest.NewRecorder()
				start := time.Now()
				mux.ServeHTTP(response, request)
				latencies.Record(time.Since(start))
				if response.Code != http.StatusOK || response.Body.Len() == 0 {
					b.Fatalf("inference returned %d: %s", response.Code, response.Body.String())
				}
				if fixture.stream && !strings.Contains(response.Body.String(), "[DONE]") {
					b.Fatal("stream lost terminal event")
				}
			}
			latencies.Report(b, 1)
			if server.dispatcher.reservations().Active() != 0 {
				b.Fatal("router leaked capacity")
			}
		})
	}
}

func BenchmarkProfileHeterogeneousRouting(b *testing.B) {
	silenceBenchmarkLogs(b)
	var calls atomic.Int64
	first := profileWorker(b, "simulated-node", 3*time.Millisecond, false, &calls)
	server := newPerformanceOrchestrator(b, first, 16)
	for i, limit := range []int{1, 2, 4} {
		id, address := "simulated-node", first.URL
		if i > 0 {
			id = "synthetic-" + strconv.Itoa(i)
			address = profileWorker(b, id, time.Duration(2-i)*time.Millisecond, false, &calls).URL
		}
		server.registry.Upsert(cluster.NodeStatus{
			ID: id, Address: address, MaxConcurrentTasks: limit, AvailableCapacity: limit,
			Hardware: hardware.Info{Score: float64(20 + i*30)},
			Engines:  []engine.Status{{Name: "simulated", Ready: true}},
			Models:   []engine.Model{{Name: benchmarkModel, Engine: "simulated", Tags: []string{"general"}, Protocols: []string{"openai"}}},
		})
	}
	mux := server.Mux()
	body := []byte(`{"model":"simulated-general","messages":[{"role":"user","content":"synthetic"}]}`)
	latencies := &testutil.Latencies{}
	type result struct {
		elapsed time.Duration
		err     error
	}
	const requests = 16
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		start := make(chan struct{})
		results := make(chan result, requests)
		for range requests {
			go func() {
				<-start
				request := httptest.NewRequest(http.MethodPost, "/router/v1/chat/completions", bytes.NewReader(body))
				response := httptest.NewRecorder()
				at := time.Now()
				mux.ServeHTTP(response, request)
				r := result{elapsed: time.Since(at)}
				if response.Code != http.StatusOK {
					r.err = fmt.Errorf("router status %d", response.Code)
				}
				results <- r
			}()
		}
		close(start)
		for range requests {
			got := <-results
			if got.err != nil {
				b.Fatal(got.err)
			}
			latencies.Record(got.elapsed)
		}
	}
	latencies.Report(b, requests)
	if server.dispatcher.reservations().Active() != 0 {
		b.Fatal("concurrent router leaked capacity")
	}
}

func BenchmarkProfileWorkflow(b *testing.B) {
	silenceBenchmarkLogs(b)
	var goodCalls, failedCalls atomic.Int64
	good := profileWorker(b, "simulated-node", 0, false, &goodCalls)
	server := newPerformanceOrchestrator(b, good, 4)
	bad := profileWorker(b, "synthetic-retry", 0, true, &failedCalls)
	server.registry.Upsert(cluster.NodeStatus{
		ID: "synthetic-retry", Address: bad.URL, MaxConcurrentTasks: 4, AvailableCapacity: 4,
		Hardware: hardware.Info{Score: 100}, Models: []engine.Model{{Name: benchmarkModel, Tags: []string{"general"}}},
	})
	server.dispatcher.retryPolicy = dispatchRetryPolicy{maxAttempts: 2, backoff: time.Millisecond, maxBackoff: time.Millisecond}
	server.tasks.SetFinalizer(server.synthesizeTask)
	server.planner = newTaskPlanner(schedulerAdapter{}, server.tasks, server.dispatcher, server, server.schedulerNodes, nil)
	latencies := &testutil.Latencies{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		start := time.Now()
		output, err := server.executeWorkflow(ctx, "synthetic workflow", []scheduler.Subtask{
			{ID: "first", TaskType: "general", Description: "synthetic first"},
			{ID: "second", TaskType: "general", Description: "synthetic second", DependsOn: []string{"first"}},
		}, false)
		latencies.Record(time.Since(start))
		cancel()
		if err != nil || output != "synthetic result" {
			b.Fatalf("workflow: %q %v", output, err)
		}
	}
	latencies.Report(b, 1)
	if failedCalls.Load() < int64(b.N) {
		b.Fatal("workflow did not exercise retries")
	}
	b.ReportMetric(float64(failedCalls.Load())/float64(b.N), "failed-attempts/op")
}

func BenchmarkProfileMemorySyncHTTP(b *testing.B) {
	for _, consumed := range []int{0, 90, 99} {
		b.Run(strconv.Itoa(consumed)+"PercentConsumed", func(b *testing.B) {
			silenceBenchmarkLogs(b)
			server := newPerformanceOrchestrator(b, nil, 4)
			server.syncMaxBytes = 1 << 20
			snapshot := memory.Snapshot{SyncVersion: memory.SyncProtocolVersion,
				Watermarks: map[string]uint64{"synthetic": 10000}}
			at := time.Unix(1700000000, 0).UTC()
			for i := range 10000 {
				snapshot.Experiences = append(snapshot.Experiences, memory.Experience{
					ID: uint64(i + 1), Origin: "synthetic", Sequence: uint64(i + 1),
					Kind: "result", Description: strings.Repeat("x", 80) + strconv.Itoa(i), CreatedAt: at,
				})
			}
			if err := server.mem.MergeSnapshotFrom(snapshot, "synthetic"); err != nil {
				b.Fatal(err)
			}
			template := httptest.NewRequest(http.MethodGet, "/api/memory/sync", nil)
			if err := memory.SetSyncRequestHeaders(template, nil); err != nil {
				b.Fatal(err)
			}
			if err := memory.SetSyncPageRequestHeaders(template, map[string]uint64{"synthetic": uint64(consumed * 100)}, snapshot.Watermarks); err != nil {
				b.Fatal(err)
			}
			latencies := &testutil.Latencies{}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				request := template.Clone(context.Background())
				response := httptest.NewRecorder()
				start := time.Now()
				server.handleMemorySyncGet(response, request)
				latencies.Record(time.Since(start))
				if response.Code != http.StatusOK {
					b.Fatalf("sync status %d: %s", response.Code, response.Body.String())
				}
				var page memory.Snapshot
				if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil || len(page.Experiences) == 0 {
					b.Fatalf("sync page invalid: %v", err)
				}
				if response.Body.Len() > 1<<20 {
					b.Fatal("sync exceeded byte limit")
				}
			}
			latencies.Report(b, 1)
		})
	}
}

func BenchmarkProfileShardLifecycle(b *testing.B) {
	silenceBenchmarkLogs(b)
	manager, registry, engines, capacity, store := newTestShardManager(b, "")
	manager.planner = shard.Plan
	engines.nodeStatuses = make(map[string]cluster.NodeStatus)
	for id, node := range registry.nodes {
		node.Hardware.Devices = []hardware.Device{{Kind: "gpu", Vendor: "nvidia", MemoryMB: 16 << 10, FreeMemoryMB: 12 << 10}}
		node.TelemetryAt = time.Now()
		registry.nodes[id] = node
		engines.nodeStatuses[node.Address] = node
	}
	request := testShardRequest()
	request.Model.FileSizeBytes = 16 << 30
	latencies := &testutil.Latencies{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		start := time.Now()
		group, err := manager.Create(context.Background(), request)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := manager.Stop(context.Background(), group.ID); err != nil {
			b.Fatal(err)
		}
		if _, err := manager.Start(context.Background(), group.ID); err != nil {
			b.Fatal(err)
		}
		if err := manager.Delete(context.Background(), group.ID); err != nil {
			b.Fatal(err)
		}
		latencies.Record(time.Since(start))
		engines.events = engines.events[:0]
	}
	latencies.Report(b, 1)
	if len(store.groups) != 0 || capacity.reservations != capacity.releases {
		b.Fatal("shard lifecycle leaked persisted groups or reservations")
	}
}
