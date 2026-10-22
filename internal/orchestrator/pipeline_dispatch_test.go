package orchestrator

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
)

func TestShardedOpenAIChatCompletionMatchesSingleNodeResponseShape(t *testing.T) {
	srv, model := newPipelineTestServer(t)
	var stage0, stage1 pipelineTestNode
	node0 := newPipelineTestNodeServer(t, "stage-0", false, &stage0)
	defer node0.Close()
	node1 := newPipelineTestNodeServer(t, "stage-1", true, &stage1)
	defer node1.Close()
	single := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.URL.Query().Get("stream"), "true") {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"output\":\"single response\"}\n\n")
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"output": "single response"})
	}))
	defer single.Close()

	srv.registry.Upsert(pipelineTestStatus("stage-0", node0.URL, model.SourceModelID, 4, 2))
	srv.registry.Upsert(pipelineTestStatus("stage-1", node1.URL, model.SourceModelID, 4, 1))
	srv.registry.Upsert(cluster.NodeStatus{
		ID: "single", Address: single.URL, Healthy: true,
		Models:             []engine.Model{{Name: "single-model", Engine: "simulated", Tags: []string{"general"}}},
		MaxConcurrentTasks: 2, AvailableCapacity: 2,
	})
	modelList := httptest.NewRecorder()
	srv.Mux().ServeHTTP(modelList, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if modelList.Code != http.StatusOK {
		t.Fatalf("model list returned %d: %s", modelList.Code, modelList.Body.String())
	}
	var listed struct {
		Data []openAIModelObj `json:"data"`
	}
	if err := json.Unmarshal(modelList.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	logicalCount, sourceCount := 0, 0
	for _, model := range listed.Data {
		switch model.ID {
		case "large-logical-model":
			logicalCount++
		case "source-model":
			sourceCount++
		}
	}
	if logicalCount != 1 || sourceCount != 0 {
		t.Fatalf("OpenAI model list exposed sharded stage models: logical=%d source=%d list=%+v",
			logicalCount, sourceCount, listed.Data)
	}

	nonStreaming := openAIChatRequestBody(t, "large-logical-model", false)
	shardedResponse := serveOpenAIChat(t, srv, nonStreaming)
	if shardedResponse.Code != http.StatusOK {
		t.Fatalf("sharded completion returned %d: %s", shardedResponse.Code, shardedResponse.Body.String())
	}
	shardedBody := decodeJSONMap(t, shardedResponse.Body.Bytes())
	assertOpenAICompletionShape(t, shardedBody)
	if got := shardedBody["model"]; got != model.LogicalID {
		t.Fatalf("sharded response model = %v, want logical ID %q", got, model.LogicalID)
	}
	if got := completionContent(t, shardedBody); got != "from pipeline" {
		t.Fatalf("sharded output = %q", got)
	}
	if stage0.lastPrompt != "<|im_start|>user\nhello<|im_end|>\n<|im_start|>assistant\n" {
		t.Fatalf("stage 0 prompt = %q", stage0.lastPrompt)
	}
	if stage1.lastActivation != stage0.lastPrompt {
		t.Fatalf("stage 1 prefill input = %q, want %q", stage1.lastActivation, stage0.lastPrompt)
	}

	normalResponse := serveOpenAIChat(t, srv, openAIChatRequestBody(t, "single-model", false))
	if normalResponse.Code != http.StatusOK {
		t.Fatalf("single-node completion returned %d: %s", normalResponse.Code, normalResponse.Body.String())
	}
	normalBody := decodeJSONMap(t, normalResponse.Body.Bytes())
	assertOpenAICompletionShape(t, normalBody)
	if got, want := sortedMapKeys(normalBody), sortedMapKeys(shardedBody); !reflect.DeepEqual(got, want) {
		t.Fatalf("response top-level shape differs: single=%v sharded=%v", got, want)
	}

	streamResponse := serveOpenAIChat(t, srv, openAIChatRequestBody(t, model.LogicalID, true))
	if streamResponse.Code != http.StatusOK {
		t.Fatalf("sharded stream returned %d: %s", streamResponse.Code, streamResponse.Body.String())
	}
	streamEvents := parseOpenAISSE(t, streamResponse.Body.String())
	if len(streamEvents) != 4 || streamEvents[len(streamEvents)-1] != "[DONE]" {
		t.Fatalf("stream events = %#v", streamEvents)
	}
	streamChunk := decodeJSONMap(t, []byte(streamEvents[0]))
	assertOpenAIStreamChunkShape(t, streamChunk)
	if got := streamChunk["model"]; got != model.LogicalID {
		t.Fatalf("stream model = %v, want logical ID %q", got, model.LogicalID)
	}

	normalStream := serveOpenAIChat(t, srv, openAIChatRequestBody(t, "single-model", true))
	if normalStream.Code != http.StatusOK {
		t.Fatalf("single-node stream returned %d: %s", normalStream.Code, normalStream.Body.String())
	}
	normalEvents := parseOpenAISSE(t, normalStream.Body.String())
	normalChunk := decodeJSONMap(t, []byte(normalEvents[0]))
	assertOpenAIStreamChunkShape(t, normalChunk)
	if got, want := sortedMapKeys(normalChunk), sortedMapKeys(streamChunk); !reflect.DeepEqual(got, want) {
		t.Fatalf("stream chunk shape differs: single=%v sharded=%v", got, want)
	}

	if srv.dispatcher.capacityGate.Active() != 0 {
		t.Fatalf("pipeline capacity leaked: active=%d", srv.dispatcher.capacityGate.Active())
	}
	persisted, err := openPipelinePlanStore(filepath.Join(filepath.Dir(srv.mem.Path()), filepath.Base(srv.mem.Path())+".pipelines.json"))
	if err != nil {
		t.Fatal(err)
	}
	if plan, ok := persisted.get(model.LogicalID); !ok || len(plan.Stages) != 2 {
		t.Fatalf("pipeline plan was not persisted by logical ID: plan=%+v exists=%v", plan, ok)
	}
	if stage0.loadCount != 1 || stage1.loadCount != 1 {
		t.Fatalf("warm stages were loaded more than once: stage0=%d stage1=%d", stage0.loadCount, stage1.loadCount)
	}
	snapshot := srv.snapshot()
	if len(snapshot.ModelInventory) != 2 {
		t.Fatalf("dashboard model inventory = %+v, want sharded and whole models", snapshot.ModelInventory)
	}
	if snapshot.ModelInventory[0].Kind == snapshot.ModelInventory[1].Kind {
		t.Fatalf("dashboard did not distinguish whole and sharded models: %+v", snapshot.ModelInventory)
	}
	if len(snapshot.PipelinePlans) != 1 || snapshot.PipelinePlans[0].Status != "ready" ||
		len(snapshot.PipelinePlans[0].Stages) != 2 {
		t.Fatalf("dashboard pipeline plan = %+v", snapshot.PipelinePlans)
	}
	transfer := snapshot.PipelinePlans[0].Stages[1]
	if transfer.ActivationTransferMS == nil || transfer.ActivationBytes == 0 {
		t.Fatalf("dashboard did not record activation transfer: %+v", transfer)
	}
}

func TestPipelineReshardEndpointRecomputesAndLoadsStages(t *testing.T) {
	srv, model := newPipelineTestServer(t)
	var stage0, stage1 pipelineTestNode
	node0 := newPipelineTestNodeServer(t, "stage-0", false, &stage0)
	defer node0.Close()
	node1 := newPipelineTestNodeServer(t, "stage-1", true, &stage1)
	defer node1.Close()
	status0 := pipelineTestStatus("stage-0", node0.URL, model.SourceModelID, 4, 2)
	status0.Models = append(status0.Models, engine.Model{Name: "whole-model", Engine: "simulated"})
	srv.registry.Upsert(status0)
	srv.registry.Upsert(pipelineTestStatus("stage-1", node1.URL, model.SourceModelID, 4, 1))
	initialResponse := serveOpenAIChat(t, srv, openAIChatRequestBody(t, model.LogicalID, false))
	if initialResponse.Code != http.StatusOK {
		t.Fatalf("initial pipeline request returned %d: %s", initialResponse.Code, initialResponse.Body.String())
	}
	oldPlan, exists := srv.pipelinePlans.get(model.LogicalID)
	if !exists {
		t.Fatal("initial pipeline request did not persist a plan")
	}

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/pipelines/"+model.LogicalID+"/reshard", nil)
	srv.Mux().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("reshard returned %d: %s", response.Code, response.Body.String())
	}
	var body struct {
		Plan config.PipelinePlan `json:"plan"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode reshard response: %v", err)
	}
	if body.Plan.PlanVersion != oldPlan.PlanVersion+1 {
		t.Fatalf("reshard plan version = %d, want %d", body.Plan.PlanVersion, oldPlan.PlanVersion+1)
	}
	if stage0.loadCount != 2 || stage1.loadCount != 2 ||
		stage0.unloadCount != 1 || stage1.unloadCount != 1 {
		t.Fatalf("reshard did not reload all stages: loads=(%d,%d) unloads=(%d,%d)",
			stage0.loadCount, stage1.loadCount, stage0.unloadCount, stage1.unloadCount)
	}
	if srv.dispatcher.capacityGate.Active() != 0 {
		t.Fatalf("reshard leaked capacity: active=%d", srv.dispatcher.capacityGate.Active())
	}
	snapshot := srv.snapshot()
	if len(snapshot.ModelInventory) != 2 || snapshot.ModelInventory[0].Kind == snapshot.ModelInventory[1].Kind {
		t.Fatalf("dashboard model inventory does not distinguish whole and sharded models: %+v", snapshot.ModelInventory)
	}
	if len(snapshot.PipelinePlans) != 1 || snapshot.PipelinePlans[0].Plan == nil ||
		snapshot.PipelinePlans[0].Plan.PlanVersion != body.Plan.PlanVersion {
		t.Fatalf("dashboard did not publish resharded plan: %+v", snapshot.PipelinePlans)
	}
}

func TestShardedOpenAIStageFailureReturnsGatewayErrorAndReleasesCapacity(t *testing.T) {
	srv, model := newPipelineTestServer(t)
	var stage0, stage1 pipelineTestNode
	node0 := newPipelineTestNodeServer(t, "stage-0", false, &stage0)
	defer node0.Close()
	node1 := newPipelineTestNodeServer(t, "stage-1", true, &stage1)
	defer node1.Close()
	stage1.failForward = true
	srv.registry.Upsert(pipelineTestStatus("stage-0", node0.URL, model.SourceModelID, 4, 2))
	srv.registry.Upsert(pipelineTestStatus("stage-1", node1.URL, model.SourceModelID, 4, 1))

	response := serveOpenAIChat(t, srv, openAIChatRequestBody(t, model.LogicalID, false))
	if response.Code != http.StatusBadGateway {
		t.Fatalf("failed pipeline response status = %d, want 502: %s", response.Code, response.Body.String())
	}
	if srv.dispatcher.capacityGate.Active() != 0 {
		t.Fatalf("failed pipeline leaked global capacity: %d", srv.dispatcher.capacityGate.Active())
	}
	for _, nodeID := range []string{"stage-0", "stage-1"} {
		gate := srv.dispatcher.capacityGate.(*dispatchCapacityGate).nodeDispatchGate(nodeID, 2)
		if active, _ := gate.occupancy(); active != 0 {
			t.Fatalf("failed pipeline leaked capacity on %s: %d", nodeID, active)
		}
	}
	if _, exists := srv.pipelinePlans.get(model.LogicalID); exists {
		t.Fatal("failed pipeline plan remained cached")
	}
}

func TestShardedOpenAIStreamingFailureAfterFirstTokenEmitsError(t *testing.T) {
	srv, model := newPipelineTestServer(t)
	var stage0, stage1 pipelineTestNode
	node0 := newPipelineTestNodeServer(t, "stage-0", false, &stage0)
	defer node0.Close()
	node1 := newPipelineTestNodeServer(t, "stage-1", true, &stage1)
	defer node1.Close()
	stage1.failAfterToken = true
	srv.registry.Upsert(pipelineTestStatus("stage-0", node0.URL, model.SourceModelID, 4, 2))
	srv.registry.Upsert(pipelineTestStatus("stage-1", node1.URL, model.SourceModelID, 4, 1))

	response := serveOpenAIChat(t, srv, openAIChatRequestBody(t, model.LogicalID, true))
	if response.Code != http.StatusOK {
		t.Fatalf("stream failure status = %d, want committed SSE status 200: %s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"content":"from "`) ||
		!strings.Contains(response.Body.String(), `"error":{"message":`) ||
		strings.Contains(response.Body.String(), "data: [DONE]") {
		t.Fatalf("stream failure did not preserve partial content and surface an error: %s", response.Body.String())
	}
	if srv.dispatcher.capacityGate.Active() != 0 {
		t.Fatalf("failed stream leaked pipeline capacity: %d", srv.dispatcher.capacityGate.Active())
	}
}

type pipelineTestNode struct {
	mu             sync.Mutex
	id             string
	isFinal        bool
	failForward    bool
	failAfterToken bool
	loaded         map[string]engine.StageAssignment
	requests       map[string]engine.StageAssignment
	loadCount      int
	unloadCount    int
	controlCalls   int
	lastPrompt     string
	lastActivation string
}

func newPipelineTestNodeServer(t *testing.T, id string, isFinal bool, state *pipelineTestNode) *httptest.Server {
	t.Helper()
	state.id = id
	state.isFinal = isFinal
	state.loaded = make(map[string]engine.StageAssignment)
	state.requests = make(map[string]engine.StageAssignment)
	mux := http.NewServeMux()
	mux.HandleFunc(engine.StageLoadPath, func(w http.ResponseWriter, r *http.Request) {
		var assignment engine.StageAssignment
		if err := json.NewDecoder(r.Body).Decode(&assignment); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		assignment.Normalize()
		if err := assignment.Validate(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		key := pipelineTestAssignmentKey(assignment)
		state.mu.Lock()
		state.controlCalls++
		if _, exists := state.loaded[key]; !exists {
			state.loaded[key] = assignment
			state.loadCount++
		}
		state.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc(engine.StageUnloadPath, func(w http.ResponseWriter, r *http.Request) {
		var assignment engine.StageAssignment
		if err := json.NewDecoder(r.Body).Decode(&assignment); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		assignment.Normalize()
		if err := assignment.Validate(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		key := pipelineTestAssignmentKey(assignment)
		state.mu.Lock()
		state.controlCalls++
		if _, exists := state.loaded[key]; exists {
			delete(state.loaded, key)
			state.unloadCount++
		} else {
			state.mu.Unlock()
			http.Error(w, "stage not loaded", http.StatusNotFound)
			return
		}
		state.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc(engine.StageOpenPath, func(w http.ResponseWriter, r *http.Request) {
		var request engine.PipelineStageControlRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := request.Validate(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		state.mu.Lock()
		state.controlCalls++
		_, loaded := state.loaded[pipelineTestAssignmentKey(request.Assignment)]
		if loaded {
			state.requests[request.RequestID] = request.Assignment
		}
		state.mu.Unlock()
		if !loaded {
			http.Error(w, "stage not loaded", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"opened": true})
	})
	mux.HandleFunc(engine.StageClosePath, func(w http.ResponseWriter, r *http.Request) {
		var request engine.PipelineStageControlRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		state.mu.Lock()
		state.controlCalls++
		_, exists := state.requests[request.RequestID]
		delete(state.requests, request.RequestID)
		state.mu.Unlock()
		if !exists {
			http.Error(w, "request not open", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"closed": true})
	})
	mux.HandleFunc(engine.StagePrefillPath, func(w http.ResponseWriter, r *http.Request) {
		state.serveV2Step(w, r, true)
	})
	mux.HandleFunc(engine.StageDecodePath, func(w http.ResponseWriter, r *http.Request) {
		state.serveV2Step(w, r, false)
	})
	return httptest.NewServer(mux)
}

func (state *pipelineTestNode) serveV2Step(w http.ResponseWriter, r *http.Request, prefill bool) {
	message, err := engine.ReadStageMessage(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := message.Request.Validate(prefill); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	state.mu.Lock()
	state.controlCalls++
	assignment, open := state.requests[message.Request.RequestID]
	_, loaded := state.loaded[pipelineTestAssignmentKey(message.Request.Assignment)]
	fail := state.failForward && prefill
	failAfterToken := state.failAfterToken && !prefill
	state.mu.Unlock()
	if !open || !loaded || assignment != message.Request.Assignment {
		http.Error(w, "request or stage not open", http.StatusNotFound)
		return
	}
	if fail || failAfterToken {
		http.Error(w, "stage runtime failed", http.StatusServiceUnavailable)
		return
	}

	activation := message.Request.Activation
	if prefill && message.Request.Assignment.StartLayer == 0 {
		prompt := []byte(message.Request.Prompt)
		activation = &engine.ActivationTensor{
			DType: engine.TensorUint8, Shape: []int64{int64(len(prompt))}, Data: prompt,
		}
	}
	if !prefill && message.Request.Assignment.StartLayer == 0 {
		tokenData := make([]byte, 8)
		binary.LittleEndian.PutUint64(tokenData, uint64(*message.Request.InputTokenID))
		activation = &engine.ActivationTensor{
			DType: engine.TensorUint8, Shape: []int64{8}, Data: tokenData,
		}
	}
	state.mu.Lock()
	if prefill && message.Request.Assignment.StartLayer == 0 {
		state.lastPrompt = string(activation.Data)
	}
	state.mu.Unlock()

	output := engine.PipelineStageOutput{}
	if state.isFinal {
		if prefill {
			state.mu.Lock()
			state.lastActivation = string(activation.Data)
			state.mu.Unlock()
			output.PromptTokens = len(activation.Data)
			output.Token = &engine.PipelineToken{ID: 1, Text: "from "}
		} else {
			output.Token = &engine.PipelineToken{ID: 2, Text: "pipeline", Done: true, FinishReason: "stop"}
		}
	} else {
		output.Activation = activation
	}
	responseRequest := message.Request
	responseRequest.Activation = nil
	w.Header().Set("Content-Type", engine.PipelineStageV2ContentType)
	w.WriteHeader(http.StatusOK)
	if err := engine.WriteStageMessage(w, engine.StageMessageHeader{
		Request: responseRequest, Output: &output,
	}, output.Activation); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func newPipelineTestServer(t *testing.T) (*Server, config.ShardedModelConfig) {
	t.Helper()
	memoryPath := filepath.Join(t.TempDir(), "orchestrator.db")
	model := config.ShardedModelConfig{
		LogicalID: "large-logical-model", SourceModelID: "source-model", Revision: "revision-1",
		TokenizerID: "tokenizer-1", ModelConfigID: "model-config-1", TotalLayers: 8,
	}
	cfg := config.DefaultOrchestratorConfig()
	cfg.MemoryPath = memoryPath
	cfg.ShardedModels = []config.ShardedModelConfig{model}
	server, err := NewWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close pipeline test server: %v", err)
		}
	})
	return server, model
}

func pipelineTestStatus(id, address, modelID string, maxLayers int, fastScore float64) cluster.NodeStatus {
	return cluster.NodeStatus{
		ID: id, Address: address, Healthy: true, FastScore: fastScore,
		SupportedShardModels: []string{modelID}, MaxLayersItCanHost: maxLayers,
		Engines: []engine.Status{{
			Name: "simulated", Ready: true,
			PipelineStage: &engine.PipelineStageCapabilities{
				ProtocolVersion: engine.PipelineStageProtocolVersion,
				Runtime:         "simulated",
				Models:          []engine.PipelineStageModelCapability{{ModelID: modelID, MaxLayers: maxLayers}},
				TensorDTypes:    []engine.TensorDType{engine.TensorUint8, engine.TensorFloat16},
			},
		}},
		Models:             []engine.Model{{Name: modelID, Engine: "simulated"}},
		MaxConcurrentTasks: 2, AvailableCapacity: 2,
	}
}

func openAIChatRequestBody(t *testing.T, model string, stream bool) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"model": model, "stream": stream,
		"messages": []map[string]string{{"role": "user", "content": "hello"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func serveOpenAIChat(t *testing.T, server *Server, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.Mux().ServeHTTP(response, request)
	return response
}

func decodeJSONMap(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode JSON response: %v: %s", err, body)
	}
	return decoded
}

func assertOpenAICompletionShape(t *testing.T, body map[string]any) {
	t.Helper()
	want := []string{"choices", "created", "id", "model", "object", "system_fingerprint", "usage"}
	if got := sortedMapKeys(body); !reflect.DeepEqual(got, want) {
		t.Fatalf("completion fields = %v, want %v", got, want)
	}
	choices, ok := body["choices"].([]any)
	if !ok || len(choices) != 1 {
		t.Fatalf("completion choices = %#v", body["choices"])
	}
	choice, ok := choices[0].(map[string]any)
	if !ok || !reflect.DeepEqual(sortedMapKeys(choice), []string{"finish_reason", "index", "message"}) {
		t.Fatalf("completion choice shape = %#v", choices[0])
	}
	message, ok := choice["message"].(map[string]any)
	if !ok || !reflect.DeepEqual(sortedMapKeys(message), []string{"content", "role"}) {
		t.Fatalf("completion message shape = %#v", choice["message"])
	}
	usage, ok := body["usage"].(map[string]any)
	if !ok || !reflect.DeepEqual(sortedMapKeys(usage), []string{"completion_tokens", "prompt_tokens", "total_tokens"}) {
		t.Fatalf("completion usage shape = %#v", body["usage"])
	}
}

func assertOpenAIStreamChunkShape(t *testing.T, body map[string]any) {
	t.Helper()
	if got := sortedMapKeys(body); !reflect.DeepEqual(got, []string{"choices", "created", "id", "model", "object"}) {
		t.Fatalf("stream chunk fields = %v", got)
	}
	choices, ok := body["choices"].([]any)
	if !ok || len(choices) != 1 {
		t.Fatalf("stream chunk choices = %#v", body["choices"])
	}
	choice, ok := choices[0].(map[string]any)
	if !ok || !reflect.DeepEqual(sortedMapKeys(choice), []string{"delta", "finish_reason", "index"}) {
		t.Fatalf("stream choice shape = %#v", choices[0])
	}
	delta, ok := choice["delta"].(map[string]any)
	if !ok || !reflect.DeepEqual(sortedMapKeys(delta), []string{"content"}) {
		t.Fatalf("stream delta shape = %#v", choice["delta"])
	}
}

func completionContent(t *testing.T, body map[string]any) string {
	t.Helper()
	choices := body["choices"].([]any)
	choice := choices[0].(map[string]any)
	message := choice["message"].(map[string]any)
	content, ok := message["content"].(string)
	if !ok {
		t.Fatalf("completion content is %T", message["content"])
	}
	return content
}

func parseOpenAISSE(t *testing.T, body string) []string {
	t.Helper()
	var events []string
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		events = append(events, strings.TrimPrefix(line, "data: "))
	}
	return events
}

func sortedMapKeys(value map[string]any) []string {
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func pipelineTestAssignmentKey(assignment engine.StageAssignment) string {
	return fmt.Sprintf("%s:%d:%d", assignment.ModelID, assignment.StartLayer, assignment.EndLayer)
}
