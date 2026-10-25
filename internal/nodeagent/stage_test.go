package nodeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/taskmgr"
)

func TestStageLoadForwardAndUnloadWithSimulatedEngine(t *testing.T) {
	agent := newStageTestAgent(t, 2, func() engine.PipelineStageEngine {
		return engine.NewSimulated()
	})
	assignment := engine.StageAssignment{ModelID: "simulated-general", StartLayer: 0, EndLayer: 4}
	loadBody, err := json.Marshal(assignment)
	if err != nil {
		t.Fatal(err)
	}
	loadResponse := httptest.NewRecorder()
	agent.handleStageLoad(loadResponse, httptest.NewRequest(http.MethodPost, "/stage/load", bytes.NewReader(loadBody)))
	if loadResponse.Code != http.StatusOK {
		t.Fatalf("stage load returned %d: %s", loadResponse.Code, loadResponse.Body.String())
	}

	status := agent.status()
	supportsAssignedModel := false
	for _, modelID := range status.SupportedShardModels {
		if modelID == assignment.ModelID {
			supportsAssignedModel = true
			break
		}
	}
	if !supportsAssignedModel || status.MaxLayersItCanHost < assignment.EndLayer-assignment.StartLayer ||
		status.CurrentStageAssignments != 1 {
		t.Fatalf("status does not advertise loaded stage: %+v", status)
	}

	forwardBody := stageForwardRequest(t, assignment, []byte("old-cache"), []byte("activation"))
	forwardRequest := httptest.NewRequest(http.MethodPost, "/stage/forward", bytes.NewReader(forwardBody))
	forwardRequest.Header.Set("Content-Type", "application/x-ndjson")
	forwardResponse := httptest.NewRecorder()
	agent.handleStageForward(forwardResponse, forwardRequest)
	if forwardResponse.Code != http.StatusOK {
		t.Fatalf("stage forward returned %d: %s", forwardResponse.Code, forwardResponse.Body.String())
	}
	if got := forwardResponse.Header().Get("Content-Type"); got != "application/x-ndjson" {
		t.Fatalf("stage forward content type = %q", got)
	}
	var frames []engine.StageForwardFrame
	decoder := json.NewDecoder(forwardResponse.Body)
	for {
		var frame engine.StageForwardFrame
		err := decoder.Decode(&frame)
		if err != nil {
			break
		}
		frames = append(frames, frame)
	}
	if len(frames) != 2 || frames[0].Type != engine.StageFrameActivation ||
		string(frames[0].Data) != "activation" || frames[1].Type != engine.StageFrameComplete ||
		string(frames[1].KVCache) != "old-cacheactivation" {
		t.Fatalf("stage forward frames = %#v", frames)
	}
	if agent.status().ActiveTasks != 0 {
		t.Fatal("stage forward left a node execution slot active")
	}

	finalAssignment := engine.StageAssignment{
		ModelID: assignment.ModelID, StartLayer: 4, EndLayer: 8, IsFinal: true,
	}
	if err := agent.stageComponent.load(context.Background(), finalAssignment, agent.resolveStageEngine); err != nil {
		t.Fatal(err)
	}
	promptBody := stagePromptForwardRequest(t, finalAssignment, "hello")
	promptRequest := httptest.NewRequest(http.MethodPost, "/stage/forward", bytes.NewReader(promptBody))
	promptRequest.Header.Set("Content-Type", "application/x-ndjson")
	promptResponse := httptest.NewRecorder()
	agent.handleStageForward(promptResponse, promptRequest)
	if promptResponse.Code != http.StatusOK {
		t.Fatalf("final stage forward returned %d: %s", promptResponse.Code, promptResponse.Body.String())
	}
	var tokenFrame engine.StageForwardFrame
	if err := json.NewDecoder(promptResponse.Body).Decode(&tokenFrame); err != nil {
		t.Fatal(err)
	}
	if tokenFrame.Type != engine.StageFrameToken || !strings.Contains(string(tokenFrame.Data), "simulated response") {
		t.Fatalf("final stage token frame = %+v", tokenFrame)
	}

	unloadBody, err := json.Marshal(assignment)
	if err != nil {
		t.Fatal(err)
	}
	unloadResponse := httptest.NewRecorder()
	agent.handleStageUnload(unloadResponse, httptest.NewRequest(http.MethodPost, "/stage/unload", bytes.NewReader(unloadBody)))
	if unloadResponse.Code != http.StatusOK {
		t.Fatalf("stage unload returned %d: %s", unloadResponse.Code, unloadResponse.Body.String())
	}
	unloadFinalBody, err := json.Marshal(finalAssignment)
	if err != nil {
		t.Fatal(err)
	}
	unloadFinalResponse := httptest.NewRecorder()
	agent.handleStageUnload(unloadFinalResponse, httptest.NewRequest(http.MethodPost, "/stage/unload", bytes.NewReader(unloadFinalBody)))
	if unloadFinalResponse.Code != http.StatusOK {
		t.Fatalf("final stage unload returned %d: %s", unloadFinalResponse.Code, unloadFinalResponse.Body.String())
	}
	status = agent.status()
	if status.CurrentStageAssignments != 0 || status.MaxLayersItCanHost == 0 {
		t.Fatalf("status did not clear unloaded assignment or retain verified capability: %+v", status)
	}
}

func TestPipelineStageV2KeepsCapacityUntilRequestClose(t *testing.T) {
	agent := newStageTestAgent(t, 2, nil)
	assignments := []engine.StageAssignment{
		{ModelID: "simulated-general", StartLayer: 0, EndLayer: 4},
		{ModelID: "simulated-general", StartLayer: 4, EndLayer: 8, IsFinal: true},
	}
	for _, assignment := range assignments {
		if err := agent.stageComponent.load(context.Background(), assignment, agent.resolveStageEngine); err != nil {
			t.Fatal(err)
		}
		response := httptest.NewRecorder()
		agent.handleStageOpen(response, stageControlRequest(t, engine.StageOpenPath, assignment, "request-v2"))
		if response.Code != http.StatusOK {
			t.Fatalf("stage open returned %d: %s", response.Code, response.Body.String())
		}
	}
	if status := agent.status(); status.ActiveTasks != 2 || status.AvailableCapacity != 0 {
		t.Fatalf("open stage requests did not reserve node capacity: %+v", status)
	}

	prefillRequest := engine.PipelineStageRequest{
		ProtocolVersion: engine.PipelineStageProtocolVersion, RequestID: "request-v2",
		Assignment: assignments[0], Prompt: "typed activation test",
	}
	first := stageV2Request(t, agent.handleStagePrefill, prefillRequest, nil)
	if first.Code != http.StatusOK || first.message.Output == nil || first.message.Output.Activation == nil ||
		!bytes.Equal(first.message.Output.Activation.Data, []byte(prefillRequest.Prompt)) {
		t.Fatalf("first prefill response = %d, %+v, %s", first.Code, first.message, first.body)
	}
	lastRequest := engine.PipelineStageRequest{
		ProtocolVersion: engine.PipelineStageProtocolVersion, RequestID: "request-v2",
		Assignment: assignments[1], Activation: first.message.Output.Activation,
	}
	last := stageV2Request(t, agent.handleStagePrefill, lastRequest, lastRequest.Activation)
	if last.Code != http.StatusOK || last.message.Output == nil || last.message.Output.Token == nil ||
		last.message.Output.PromptTokens != len(prefillRequest.Prompt) {
		t.Fatalf("final prefill response = %d, %+v, %s", last.Code, last.message, last.body)
	}

	tokenID := last.message.Output.Token.ID
	decodeFirst := engine.PipelineStageRequest{
		ProtocolVersion: engine.PipelineStageProtocolVersion, RequestID: "request-v2",
		Assignment: assignments[0], InputTokenID: &tokenID, Position: int64(last.message.Output.PromptTokens),
	}
	activation := stageV2Request(t, agent.handleStageDecode, decodeFirst, nil)
	if activation.Code != http.StatusOK || activation.message.Output == nil || activation.message.Output.Activation == nil {
		t.Fatalf("first decode response = %d, %+v, %s", activation.Code, activation.message, activation.body)
	}
	decodeLast := engine.PipelineStageRequest{
		ProtocolVersion: engine.PipelineStageProtocolVersion, RequestID: "request-v2",
		Assignment: assignments[1], Activation: activation.message.Output.Activation,
		Position: decodeFirst.Position,
	}
	token := stageV2Request(t, agent.handleStageDecode, decodeLast, decodeLast.Activation)
	if token.Code != http.StatusOK || token.message.Output == nil || token.message.Output.Token == nil {
		t.Fatalf("final decode response = %d, %+v, %s", token.Code, token.message, token.body)
	}

	for _, assignment := range assignments {
		response := httptest.NewRecorder()
		agent.handleStageClose(response, stageControlRequest(t, engine.StageClosePath, assignment, "request-v2"))
		if response.Code != http.StatusOK {
			t.Fatalf("stage close returned %d: %s", response.Code, response.Body.String())
		}
	}
	if status := agent.status(); status.ActiveTasks != 0 || status.AvailableCapacity != 2 {
		t.Fatalf("close did not release stage capacity: %+v", status)
	}
}

func TestPipelineStageOpenExpiresAndReleasesCapacity(t *testing.T) {
	agent := newStageTestAgent(t, 1, nil)
	agent.stageComponent.requestIdleTimeout = 25 * time.Millisecond
	assignment := engine.StageAssignment{ModelID: "simulated-general", StartLayer: 0, EndLayer: 4}
	if err := agent.stageComponent.load(context.Background(), assignment, agent.resolveStageEngine); err != nil {
		t.Fatal(err)
	}
	releaseCapacity, err := agent.executorComponent.beginExecution(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	released := make(chan struct{})
	if err := agent.stageComponent.openPipelineRequest(
		context.Background(), assignment, "orphaned-request",
		func() {
			releaseCapacity()
			close(released)
		},
	); err != nil {
		releaseCapacity()
		t.Fatal(err)
	}

	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("expired pipeline request did not release executor capacity")
	}
	if status := agent.status(); status.ActiveTasks != 0 || status.AvailableCapacity != 1 {
		t.Fatalf("expired pipeline request kept capacity reserved: %+v", status)
	}
	agent.stageComponent.mu.Lock()
	requestCount := len(agent.stageComponent.requests)
	agent.stageComponent.mu.Unlock()
	if requestCount != 0 {
		t.Fatalf("expired pipeline request remains registered: %d", requestCount)
	}
	if err := agent.stageComponent.unload(context.Background(), assignment); err != nil {
		t.Fatalf("expired pipeline request kept its stage lease: %v", err)
	}
}

type blockingPipelineStageEngine struct {
	*engine.Simulated
	started chan struct{}
	release chan struct{}
}

func (e *blockingPipelineStageEngine) ForwardStage(
	ctx context.Context,
	assignment engine.StageAssignment,
	activations io.Reader,
	kvCache []byte,
	emit func([]byte) error,
) ([]byte, error) {
	e.started <- struct{}{}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.release:
		return e.Simulated.ForwardStage(ctx, assignment, activations, kvCache, emit)
	}
}

func (e *blockingPipelineStageEngine) ForwardStageRequest(
	ctx context.Context,
	assignment engine.StageAssignment,
	request engine.StageForwardRequest,
	emit func(engine.StageForwardFrame) error,
) ([]byte, error) {
	e.started <- struct{}{}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.release:
		return e.Simulated.ForwardStageRequest(ctx, assignment, request, emit)
	}
}

func TestStageForwardUsesConcurrentTaskCapacity(t *testing.T) {
	stageEngine := &blockingPipelineStageEngine{
		Simulated: engine.NewSimulated(),
		started:   make(chan struct{}, 2),
		release:   make(chan struct{}),
	}
	agent := newStageTestAgent(t, 1, func() engine.PipelineStageEngine { return stageEngine })
	assignment := engine.StageAssignment{ModelID: "simulated-general", StartLayer: 0, EndLayer: 4}
	if err := agent.stageComponent.load(context.Background(), assignment, agent.resolveStageEngine); err != nil {
		t.Fatal(err)
	}
	makeRequest := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(
			http.MethodPost, "/stage/forward",
			bytes.NewReader(stageForwardRequest(t, assignment, nil, []byte("activation"))),
		)
		request.Header.Set("Content-Type", "application/x-ndjson")
		response := httptest.NewRecorder()
		agent.handleStageForward(response, request)
		return response
	}

	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { firstDone <- makeRequest() }()
	select {
	case <-stageEngine.started:
	case <-time.After(time.Second):
		t.Fatal("first stage forward did not start")
	}
	secondDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { secondDone <- makeRequest() }()
	select {
	case <-stageEngine.started:
		t.Fatal("second stage forward started while node capacity was full")
	case <-time.After(30 * time.Millisecond):
	}
	if status := agent.status(); status.ActiveTasks != 1 || status.AvailableCapacity != 0 {
		t.Fatalf("status does not report occupied stage capacity: %+v", status)
	}

	close(stageEngine.release)
	for range 2 {
		select {
		case response := <-firstDone:
			if response.Code != http.StatusOK {
				t.Fatalf("first stage forward returned %d: %s", response.Code, response.Body.String())
			}
		case response := <-secondDone:
			if response.Code != http.StatusOK {
				t.Fatalf("second stage forward returned %d: %s", response.Code, response.Body.String())
			}
		case <-time.After(time.Second):
			t.Fatal("stage forwards did not finish after capacity was released")
		}
	}
}

func TestNodeStatusDoesNotAdvertiseConfiguredReadinessWithoutProbe(t *testing.T) {
	agent := newStageTestAgent(t, 1, nil, "simulated-general")
	status := agent.status()
	if len(status.SupportedShardModels) != 0 || status.MaxLayersItCanHost != 0 {
		t.Fatalf("configured stage readiness was advertised without a runtime probe: %+v", status)
	}
}

type stageV2Response struct {
	Code    int
	message engine.StageMessageHeader
	body    string
}

func stageControlRequest(
	t *testing.T,
	path string,
	assignment engine.StageAssignment,
	requestID string,
) *http.Request {
	t.Helper()
	body, err := json.Marshal(engine.PipelineStageControlRequest{
		ProtocolVersion: engine.PipelineStageProtocolVersion,
		RequestID:       requestID, Assignment: assignment,
	})
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
}

func stageV2Request(
	t *testing.T,
	handler http.HandlerFunc,
	request engine.PipelineStageRequest,
	tensor *engine.ActivationTensor,
) stageV2Response {
	t.Helper()
	var body bytes.Buffer
	if err := engine.WriteStageMessage(&body, engine.StageMessageHeader{Request: request}, tensor); err != nil {
		t.Fatal(err)
	}
	httpRequest := httptest.NewRequest(http.MethodPost, "/stage", bytes.NewReader(body.Bytes()))
	httpRequest.Header.Set("Content-Type", engine.PipelineStageV2ContentType)
	response := httptest.NewRecorder()
	handler(response, httpRequest)
	result := stageV2Response{Code: response.Code, body: response.Body.String()}
	if response.Code == http.StatusOK {
		message, err := engine.ReadStageMessage(bytes.NewReader(response.Body.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		result.message = message
	}
	return result
}

func newStageTestAgent(
	t *testing.T,
	maxConcurrent int,
	stageEngine func() engine.PipelineStageEngine,
	supportedShardModels ...string,
) *Agent {
	t.Helper()
	var local engine.Engine
	if stageEngine != nil {
		if value, ok := stageEngine().(engine.Engine); ok {
			local = value
		}
	}
	if local == nil {
		local = engine.NewSimulated()
	}
	cfg := config.NodeConfig{
		NodeID: "pipeline-stage-test", MaxConcurrentTasks: maxConcurrent,
		MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	}
	if len(supportedShardModels) > 0 {
		cfg.SupportedShardModels = supportedShardModels
		cfg.MaxLayersItCanHost = 16
	}
	agent := mustNewTestAgentWithEngineDetector(t, cfg, taskmgr.NewManager(), func() []engine.Engine {
		return []engine.Engine{local}
	})
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("close stage test agent: %v", err)
		}
	})
	return agent
}

func stageForwardRequest(t *testing.T, assignment engine.StageAssignment, kvCache, activations []byte) []byte {
	t.Helper()
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	if err := encoder.Encode(engine.StageForwardFrame{
		Type: engine.StageFrameStart, Assignment: &assignment, KVCache: kvCache,
	}); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Encode(engine.StageForwardFrame{Type: engine.StageFrameActivation, Data: activations}); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Encode(engine.StageForwardFrame{Type: engine.StageFrameEnd}); err != nil {
		t.Fatal(err)
	}
	return body.Bytes()
}

func stagePromptForwardRequest(t *testing.T, assignment engine.StageAssignment, prompt string) []byte {
	t.Helper()
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(engine.StageForwardFrame{
		Type: engine.StageFrameStart, Assignment: &assignment, Prompt: prompt,
	}); err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(&body).Encode(engine.StageForwardFrame{Type: engine.StageFrameEnd}); err != nil {
		t.Fatal(err)
	}
	return body.Bytes()
}
