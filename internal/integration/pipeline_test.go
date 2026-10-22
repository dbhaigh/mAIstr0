package integration

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/orchestrator"
)

func TestShardedChatCompletionAcrossThreeSimulatedNodes(t *testing.T) {
	model := config.ShardedModelConfig{
		LogicalID: "toy-chat-32", SourceModelID: "toy-source-32", Revision: "test-revision",
		TokenizerID: "toy-tokenizer", ModelConfigID: "toy-config", TotalLayers: 32,
	}
	cfg := config.DefaultOrchestratorConfig()
	cfg.MemoryPath = filepath.Join(t.TempDir(), "orchestrator.db")
	cfg.ShardedModels = []config.ShardedModelConfig{model}
	server, err := orchestrator.NewWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close orchestrator: %v", err)
		}
	})

	orchestratorHTTP := httptest.NewServer(server.Mux())
	t.Cleanup(orchestratorHTTP.Close)

	nodeSpecs := []struct {
		id        string
		maxLayers int
		fastScore float64
	}{
		{id: "pipeline-node-0", maxLayers: 11, fastScore: 3},
		{id: "pipeline-node-1", maxLayers: 11, fastScore: 2},
		{id: "pipeline-node-2", maxLayers: 10, fastScore: 1},
	}
	calls := &pipelineInvocationLog{}
	nodes := make([]*simulatedPipelineNode, len(nodeSpecs))
	for i, spec := range nodeSpecs {
		nodes[i] = startSimulatedPipelineNode(t, spec.id, calls)
		status := cluster.NodeStatus{
			ID: spec.id, Address: nodes[i].server.URL,
			Models: []engine.Model{{Name: model.SourceModelID, Engine: "simulated"}},
			Engines: []engine.Status{{
				Name: "simulated", Ready: true,
				PipelineStage: &engine.PipelineStageCapabilities{
					ProtocolVersion: engine.PipelineStageProtocolVersion,
					Runtime:         "simulated",
					Models:          []engine.PipelineStageModelCapability{{ModelID: model.SourceModelID, MaxLayers: spec.maxLayers}},
					TensorDTypes:    []engine.TensorDType{engine.TensorUint8},
				},
			}},
			SupportedShardModels: []string{model.SourceModelID},
			MaxLayersItCanHost:   spec.maxLayers,
			MaxConcurrentTasks:   2,
			AvailableCapacity:    2,
			TelemetryAt:          time.Now(),
			FastScore:            spec.fastScore,
		}
		registerPipelineNode(t, orchestratorHTTP.Client(), orchestratorHTTP.URL, status)
	}

	requestBody, err := json.Marshal(map[string]any{
		"model":  model.LogicalID,
		"stream": true,
		"messages": []map[string]string{
			{"role": "user", "content": "Say hello from the distributed toy model."},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, orchestratorHTTP.URL+"/v1/chat/completions", bytes.NewReader(requestBody))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := orchestratorHTTP.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	responseBody, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("chat completion returned %d: %s", response.StatusCode, responseBody)
	}
	if !strings.Contains(response.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("chat completion content type = %q, want event stream", response.Header.Get("Content-Type"))
	}
	streamBody := string(responseBody)
	if !strings.Contains(streamBody, "[simulated ") ||
		!strings.Contains(streamBody, "from ") ||
		!strings.Contains(streamBody, model.SourceModelID) ||
		!strings.Contains(streamBody, "data: [DONE]") {
		t.Fatalf("chat completion stream omitted generated text or [DONE]: %s", responseBody)
	}

	invocations := calls.snapshot()
	wantStages := []pipelineStageInvocation{
		{nodeID: "pipeline-node-0", startLayer: 0, endLayer: 11},
		{nodeID: "pipeline-node-1", startLayer: 11, endLayer: 22},
		{nodeID: "pipeline-node-2", startLayer: 22, endLayer: 32},
	}
	if len(invocations) < len(wantStages) || len(invocations)%len(wantStages) != 0 {
		t.Fatalf("stage invocation count = %d, want prefill/decode groups of %d: %#v",
			len(invocations), len(wantStages), invocations)
	}
	for offset := 0; offset < len(invocations); offset += len(wantStages) {
		wantPhase := "decode"
		if offset == 0 {
			wantPhase = "prefill"
		}
		for i, want := range wantStages {
			got := invocations[offset+i]
			if got.nodeID != want.nodeID || got.startLayer != want.startLayer ||
				got.endLayer != want.endLayer || got.phase != wantPhase {
				t.Fatalf("stage invocation %d = %+v, want %+v in %s order",
					offset+i, got, want, wantPhase)
			}
		}
	}
}

func registerPipelineNode(t *testing.T, client *http.Client, address string, status cluster.NodeStatus) {
	t.Helper()
	body, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, address+"/api/nodes/register", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("register node %q returned %d", status.ID, response.StatusCode)
	}
}

type simulatedPipelineNode struct {
	server *httptest.Server
}

type pipelineStageInvocation struct {
	nodeID     string
	startLayer int
	endLayer   int
	phase      string
}

type pipelineInvocationLog struct {
	mu    sync.Mutex
	calls []pipelineStageInvocation
}

func (log *pipelineInvocationLog) add(call pipelineStageInvocation) {
	log.mu.Lock()
	log.calls = append(log.calls, call)
	log.mu.Unlock()
}

func (log *pipelineInvocationLog) snapshot() []pipelineStageInvocation {
	log.mu.Lock()
	defer log.mu.Unlock()
	return append([]pipelineStageInvocation(nil), log.calls...)
}

func startSimulatedPipelineNode(t *testing.T, nodeID string, calls *pipelineInvocationLog) *simulatedPipelineNode {
	t.Helper()
	simulated := engine.NewSimulated()
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+engine.StageLoadPath, func(w http.ResponseWriter, r *http.Request) {
		var assignment engine.StageAssignment
		if err := json.NewDecoder(r.Body).Decode(&assignment); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := simulated.LoadStage(r.Context(), assignment); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST "+engine.StageUnloadPath, func(w http.ResponseWriter, r *http.Request) {
		var assignment engine.StageAssignment
		if err := json.NewDecoder(r.Body).Decode(&assignment); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := simulated.UnloadStage(r.Context(), assignment); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST "+engine.StageOpenPath, func(w http.ResponseWriter, r *http.Request) {
		var request engine.PipelineStageControlRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := simulated.OpenPipelineRequest(r.Context(), request.Assignment, request.RequestID); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST "+engine.StageClosePath, func(w http.ResponseWriter, r *http.Request) {
		var request engine.PipelineStageControlRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := simulated.ClosePipelineRequest(r.Context(), request.Assignment, request.RequestID); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST "+engine.StagePrefillPath, func(w http.ResponseWriter, r *http.Request) {
		handleSimulatedPipelineStep(t, w, r, nodeID, calls, simulated, true)
	})
	mux.HandleFunc("POST "+engine.StageDecodePath, func(w http.ResponseWriter, r *http.Request) {
		handleSimulatedPipelineStep(t, w, r, nodeID, calls, simulated, false)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return &simulatedPipelineNode{server: server}
}

func handleSimulatedPipelineStep(
	t *testing.T,
	w http.ResponseWriter,
	r *http.Request,
	nodeID string,
	calls *pipelineInvocationLog,
	simulated *engine.Simulated,
	prefill bool,
) {
	message, err := engine.ReadStageMessage(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	phase := "decode"
	if prefill {
		phase = "prefill"
	}
	calls.add(pipelineStageInvocation{
		nodeID: nodeID, startLayer: message.Request.Assignment.StartLayer,
		endLayer: message.Request.Assignment.EndLayer, phase: phase,
	})
	var output engine.PipelineStageOutput
	if prefill {
		output, err = simulated.PrefillPipelineStage(r.Context(), message.Request.Assignment, message.Request)
	} else {
		output, err = simulated.DecodePipelineStage(r.Context(), message.Request.Assignment, message.Request)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	responseRequest := message.Request
	responseRequest.Activation = nil
	w.Header().Set("Content-Type", engine.PipelineStageV2ContentType)
	if err := engine.WriteStageMessage(w, engine.StageMessageHeader{
		Request: responseRequest, Output: &output,
	}, output.Activation); err != nil {
		t.Errorf("write stage output for %s: %v", nodeID, err)
	}
}
