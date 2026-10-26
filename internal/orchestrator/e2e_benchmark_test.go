package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/benchrun"
	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/hardware"
)

type e2eWorker struct {
	server                    *httptest.Server
	capacity                  int
	index                     int
	delay                     time.Duration
	slots                     chan struct{}
	active, peak, calls, busy atomic.Int64
	loaded                    atomic.Bool
	failNext                  atomic.Bool
	mu                        sync.Mutex
	service                   []float64
	handlers                  sync.WaitGroup
}

type e2eJoinedHandler struct {
	http.Handler
	handlers sync.WaitGroup
}

func (h *e2eJoinedHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.handlers.Add(1)
	defer h.handlers.Done()
	h.Handler.ServeHTTP(w, r)
}

func newE2EWorker(t *testing.T, index, capacity int, delay time.Duration) *e2eWorker {
	t.Helper()
	worker := &e2eWorker{index: index, capacity: capacity, delay: delay, slots: make(chan struct{}, capacity)}
	worker.server = httptest.NewServer(http.HandlerFunc(worker.serve))
	t.Cleanup(worker.server.Close)
	return worker
}

func e2eWait(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (w *e2eWorker) serve(out http.ResponseWriter, request *http.Request) {
	w.handlers.Add(1)
	defer w.handlers.Done()
	if request.Method == http.MethodGet && request.URL.Path == "/api/status" {
		status := e2eNode(w, w.index)
		status.ActiveTasks = int(w.active.Load())
		status.AvailableCapacity = max(0, w.capacity-status.ActiveTasks)
		out.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(out).Encode(status); err != nil {
			panic(err)
		}
		return
	}
	if !e2eWait(request.Context(), 2*time.Millisecond) {
		return
	}
	select {
	case w.slots <- struct{}{}:
	case <-request.Context().Done():
		return
	}
	defer func() { <-w.slots }()
	active := w.active.Add(1)
	for peak := w.peak.Load(); active > peak; peak = w.peak.Load() {
		if w.peak.CompareAndSwap(peak, active) {
			break
		}
	}
	w.calls.Add(1)
	started := time.Now()
	defer func() {
		w.active.Add(-1)
		elapsed := time.Since(started)
		w.busy.Add(int64(elapsed))
		w.mu.Lock()
		w.service = append(w.service, float64(elapsed)/float64(time.Millisecond))
		w.mu.Unlock()
	}()
	if w.failNext.Swap(false) {
		http.Error(out, "synthetic transient failure", http.StatusServiceUnavailable)
		return
	}
	var payload struct {
		Model    string `json:"model"`
		Stream   bool   `json:"stream"`
		Messages []struct {
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil || len(payload.Messages) != 1 {
		http.Error(out, "invalid benchmark payload", http.StatusBadRequest)
		return
	}
	generation := w.delay
	if strings.Contains(payload.Model, "large") {
		generation *= 4
	}
	generation += time.Duration(len(strings.Fields(payload.Messages[0].Content))/64) * time.Millisecond
	if !w.loaded.Swap(true) {
		generation += 30 * time.Millisecond
	}
	if !e2eWait(request.Context(), generation) {
		return
	}
	if !payload.Stream {
		out.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(out, `{"choices":[{"message":{"content":"synthetic"}}]}`)
		return
	}
	out.Header().Set("Content-Type", "text/event-stream")
	for range 4 {
		_, _ = io.WriteString(out, "data: {\"choices\":[{\"delta\":{\"content\":\"synthetic\"}}]}\n\n")
		out.(http.Flusher).Flush()
		if !e2eWait(request.Context(), time.Millisecond) {
			return
		}
	}
	_, _ = io.WriteString(out, "data: [DONE]\n\n")
}

func (w *e2eWorker) reset(cold, fail bool) {
	w.calls.Store(0)
	w.peak.Store(0)
	w.busy.Store(0)
	w.loaded.Store(!cold)
	w.failNext.Store(fail)
	w.mu.Lock()
	w.service = nil
	w.mu.Unlock()
}

func e2eNode(worker *e2eWorker, index int) cluster.NodeStatus {
	return cluster.NodeStatus{ID: fmt.Sprintf("synthetic-%d", index), Address: worker.server.URL,
		MaxConcurrentTasks: worker.capacity, AvailableCapacity: worker.capacity, Healthy: true,
		Hardware: hardware.Info{Score: float64(100 - index*10)},
		Engines:  []engine.Status{{Name: "synthetic", Ready: true}},
		Models: []engine.Model{{Name: "synthetic-small", Engine: "synthetic", Protocols: []string{"openai"}},
			{Name: "synthetic-large", Engine: "synthetic", Protocols: []string{"openai"}}}}
}

// An explicit output path opts into recorded measurements; ordinary CI keeps the
// same matrix but uses three samples, never downloads models or probes hardware.
func TestEndToEndInferenceBenchmarks(t *testing.T) {
	requests := 3
	if text := os.Getenv("MAISTR0_E2E_REQUESTS"); text != "" {
		value, err := strconv.Atoi(text)
		if err != nil || value < 1 || value > 2000 {
			t.Fatal("MAISTR0_E2E_REQUESTS must be 1..2000")
		}
		requests = value
	}
	workers := []*e2eWorker{newE2EWorker(t, 0, 2, 2*time.Millisecond), newE2EWorker(t, 1, 1, 4*time.Millisecond), newE2EWorker(t, 2, 3, time.Millisecond)}
	single := newPerformanceOrchestrator(t, nil, 6)
	multi := newPerformanceOrchestrator(t, nil, 6)
	single.registry.Upsert(e2eNode(workers[0], 0))
	for i, worker := range workers {
		multi.registry.Upsert(e2eNode(worker, i))
	}
	singleHandler, multiHandler := &e2eJoinedHandler{Handler: single.Mux()}, &e2eJoinedHandler{Handler: multi.Mux()}
	singleHTTP, multiHTTP := httptest.NewServer(singleHandler), httptest.NewServer(multiHandler)
	defer singleHTTP.Close()
	defer multiHTTP.Close()
	targets := []benchrun.Target{
		{Name: "engine-direct", URL: workers[0].server.URL + "/v1/chat/completions", Nodes: 1},
		{Name: "router-single", URL: singleHTTP.URL + "/router/v1/chat/completions", Nodes: 1},
		{Name: "router-multi", URL: multiHTTP.URL + "/router/v1/chat/completions", Nodes: 3},
	}
	report := benchrun.NewReport(5000)
	report.Scope = "Synthetic HTTP client -> real router/capacity/node client -> fake node/engine facade; no mTLS, real model, GPU or subprocess. Node service includes modeled inference, not measured hardware generation."
	var workloads []benchrun.Workload
	for _, model := range []string{"synthetic-small", "synthetic-large"} {
		for _, words := range []int{32, 512} {
			for _, concurrency := range []int{1, 4} {
				for _, stream := range []bool{false, true} {
					workloads = append(workloads, benchrun.Workload{Name: fmt.Sprintf("%s-words%d-c%d-stream%t", model, words, concurrency, stream),
						Model: model, ModelRevision: "fixture-v1", PromptWords: words, OutputTokens: 16, Concurrency: concurrency,
						Requests: requests, Stream: stream, LoadState: "warm"})
				}
			}
		}
	}
	workloads = append(workloads,
		benchrun.Workload{Name: "cold", Model: "synthetic-large", ModelRevision: "fixture-v1", PromptWords: 512, OutputTokens: 16,
			Concurrency: 1, Requests: 1, Stream: true, LoadState: "operator-cold"},
		benchrun.Workload{Name: "transient-failure", Model: "synthetic-small", ModelRevision: "fixture-v1", PromptWords: 32, OutputTokens: 16,
			Concurrency: 1, Requests: 3, LoadState: "uncontrolled"})
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer client.CloseIdleConnections()
	for _, workload := range workloads {
		for index, target := range targets {
			target.Engine, target.Settings = "synthetic-fixture-v1", "temperature=0; max_tokens=16; 2ms modeled network; fixed modeled service"
			target.Hardware = []string{"fake CPU worker: 2 slots, base 2ms"}
			if index == 2 {
				target.Hardware = append(target.Hardware, "fake worker: 1 slot/base 4ms", "fake worker: 3 slots/base 1ms")
			}
			for i, worker := range workers {
				worker.reset(workload.Name == "cold", workload.Name == "transient-failure" && i == 0)
			}
			var server *Server
			switch index {
			case 1:
				server = single
			case 2:
				server = multi
			}
			var before operationalMetricsSnapshot
			if server != nil {
				for i, worker := range workers[:target.Nodes] {
					server.registry.Upsert(e2eNode(worker, i))
				}
				server.routerEvents = newRouterEventLog(20000)
				before = server.metrics.snapshot()
			}
			result, err := benchrun.RunWorkload(context.Background(), client, target, workload, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			singleHandler.handlers.Wait()
			multiHandler.handlers.Wait()
			for _, worker := range workers {
				worker.handlers.Wait()
			}
			wantErrors := 0
			if workload.Name == "transient-failure" && index < 2 {
				wantErrors = 1
			}
			if result.Errors != wantErrors {
				var events []routerRequestEvent
				if server != nil {
					events = server.routerEvents.snapshot()
					events = events[max(0, len(events)-6):]
				}
				t.Fatalf("%s/%s errors = %d, want %d; first=%+v; terminal events=%+v", target.Name, workload.Name, result.Errors, wantErrors, result.Samples[0], events)
			}
			observation := &benchrun.ServerObservations{Scope: "fixture engine-service wall time and busy-slot fraction (not CPU/GPU utilization); actual router event timestamps; scheduler wall time (not CPU)"}
			for i, worker := range workers[:target.Nodes] {
				if worker.active.Load() != 0 || len(worker.slots) != 0 {
					t.Fatal("synthetic engine leaked capacity")
				}
				if peak := worker.peak.Load(); peak > int64(worker.capacity) {
					t.Fatalf("worker peak %d exceeds capacity", peak)
				}
				observation.Nodes = append(observation.Nodes, benchrun.NodeObservation{Name: fmt.Sprintf("synthetic-%d", i),
					Capacity: worker.capacity, PeakActive: worker.peak.Load(), Requests: worker.calls.Load(),
					BusySlotFraction: float64(worker.busy.Load()) / (result.DurationMS * float64(time.Millisecond) * float64(worker.capacity))})
				observation.Service.Samples += len(worker.service)
			}
			var service []float64
			for _, worker := range workers[:target.Nodes] {
				service = append(service, worker.service...)
			}
			observation.Service = benchrun.Summarize(service)
			if server != nil {
				after := server.metrics.snapshot()
				assignments, schedulerNS, retries := after.assignments-before.assignments,
					after.assignmentNanos-before.assignmentNanos, after.retries-before.retries
				observation.Assignments, observation.SchedulerWallNS, observation.Retries = &assignments, &schedulerNS, &retries
				if assignments == 0 {
					t.Fatal("router scheduler measurements missing")
				}
				var waits, setup []float64
				queued, admitted := make(map[string]time.Time), make(map[string]time.Time)
				for _, event := range server.routerEvents.snapshot() {
					switch event.Phase {
					case "queued":
						queued[event.AttemptID] = event.Timestamp
					case "attempt_started":
						if at, ok := queued[event.AttemptID]; ok {
							waits = append(waits, float64(event.Timestamp.Sub(at))/float64(time.Millisecond))
						}
						admitted[event.AttemptID] = event.Timestamp
					case "dispatched":
						if at, ok := admitted[event.AttemptID]; ok {
							setup = append(setup, float64(event.Timestamp.Sub(at))/float64(time.Millisecond))
						}
					}
				}
				observation.QueueWait, observation.DispatchSetup = benchrun.Summarize(waits), benchrun.Summarize(setup)
				if server.dispatcher.reservations().Active() != 0 {
					t.Fatal("router leaked capacity")
				}
				if workload.Name == "transient-failure" && index == 2 && *observation.Retries == 0 {
					t.Fatal("multi-node recovery did not retry")
				}
			}
			if workload.Name == "transient-failure" {
				elapsed := 0.0
				for _, sample := range result.Samples {
					elapsed += sample.ElapsedMS
					if sample.Outcome == "completed" {
						observation.RecoveryMS = &elapsed
						break
					}
				}
			}
			result.Server = observation
			report.Results = append(report.Results, result)
		}
	}
	if path := os.Getenv("MAISTR0_E2E_REPORT"); path != "" {
		output, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		err = json.NewEncoder(output).Encode(report)
		closeErr := output.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("write report: %v; close: %v", err, closeErr)
		}
	}
}
