package orchestrator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/scheduler"
	"github.com/maistr0/maistr0/internal/workpool"
)

type resolvedPipelineStage struct {
	spec       config.StageSpec
	nodeID     string
	address    string
	assignment engine.StageAssignment
}

const maxPipelineGeneratedTokens = 4096

var errPipelinePlanPersistence = errors.New("pipeline plan persistence failed")

func (s *Server) shardedModel(logicalID string) (config.ShardedModelConfig, bool) {
	model, ok := s.shardedModels[strings.TrimSpace(logicalID)]
	return model, ok
}

func (s *Server) shardedSourceModel(sourceModelID string) (config.ShardedModelConfig, bool) {
	for _, model := range s.shardedModels {
		if strings.EqualFold(sourceModelID, model.SourceModelID) {
			return model, true
		}
	}
	return config.ShardedModelConfig{}, false
}

func (s *Server) pipelineForModel(
	model config.ShardedModelConfig,
) (config.PipelinePlan, []resolvedPipelineStage, error) {
	unlock := s.pipelineLocker.lock(model.LogicalID)
	defer unlock()
	return s.pipelineForModelLocked(model)
}

func (s *Server) pipelineForModelLocked(
	model config.ShardedModelConfig,
) (config.PipelinePlan, []resolvedPipelineStage, error) {
	nodes := s.dispatcher.schedulerNodes()
	cached, hasCached := s.pipelinePlans.get(model.LogicalID)
	if hasCached && cached.Model == model {
		if stages, err := resolvePipelineStages(cached, nodes); err == nil {
			if cached.CoordinatorEpoch != s.orchestratorEpoch {
				cached.CoordinatorEpoch = s.orchestratorEpoch
				if err := s.pipelinePlans.set(cached); err != nil {
					return config.PipelinePlan{}, nil, fmt.Errorf("%w: persist current epoch: %v", errPipelinePlanPersistence, err)
				}
			}
			return cached, stages, nil
		}
	}

	plan, err := scheduler.PlacePipeline(model, nodes, s.orchestratorEpoch)
	if err != nil {
		return config.PipelinePlan{}, nil, err
	}
	if hasCached {
		plan.PlanVersion = cached.PlanVersion + 1
	}
	stages, err := resolvePipelineStages(plan, nodes)
	if err != nil {
		return config.PipelinePlan{}, nil, fmt.Errorf("%w: %v", errPipelineNodeUnavailable, err)
	}
	if err := s.pipelinePlans.set(plan); err != nil {
		return config.PipelinePlan{}, nil, fmt.Errorf("%w: %v", errPipelinePlanPersistence, err)
	}
	return plan, stages, nil
}

func (s *Server) reshardPipeline(
	ctx context.Context,
	model config.ShardedModelConfig,
) (config.PipelinePlan, error) {
	releaseRequests, err := s.pipelineRequests.beginReshard(ctx, model.LogicalID)
	if err != nil {
		return config.PipelinePlan{}, err
	}
	defer releaseRequests()

	unlock := s.pipelineLocker.lock(model.LogicalID)
	nodes := s.dispatcher.schedulerNodes()
	previous, hadPrevious := s.pipelinePlans.get(model.LogicalID)
	plan, err := scheduler.PlacePipeline(model, nodes, s.orchestratorEpoch)
	if err != nil {
		unlock()
		return config.PipelinePlan{}, err
	}
	if hadPrevious {
		plan.PlanVersion = previous.PlanVersion + 1
	}
	stages, err := resolvePipelineStages(plan, nodes)
	if err != nil {
		unlock()
		return config.PipelinePlan{}, fmt.Errorf("%w: %v", errPipelineNodeUnavailable, err)
	}
	unlock()

	gate, ok := s.dispatcher.capacityGate.(*dispatchCapacityGate)
	if !ok {
		return config.PipelinePlan{}, errors.New("pipeline capacity gate is unavailable")
	}
	targetsByNode := make(map[string]workpool.CapacityTarget, len(stages))
	for _, stage := range stages {
		targetsByNode[stage.nodeID] = workpool.CapacityTarget{NodeID: stage.nodeID, Address: stage.address}
	}
	if hadPrevious {
		for _, stage := range previous.Stages {
			node, exists := findNodeStatus(nodes, stage.NodeID)
			if exists && node.Healthy && node.Address != "" {
				targetsByNode[node.ID] = workpool.CapacityTarget{NodeID: node.ID, Address: node.Address}
			}
		}
	}
	targets := make([]workpool.CapacityTarget, 0, len(targetsByNode))
	for _, target := range targetsByNode {
		targets = append(targets, target)
	}
	reservation, err := gate.ReservePipeline(ctx, targets)
	if err != nil {
		return config.PipelinePlan{}, fmt.Errorf("reserve pipeline capacity for reshard: %w", err)
	}
	defer reservation.Release()

	if hadPrevious {
		for i, stage := range previous.Stages {
			node, exists := findNodeStatus(nodes, stage.NodeID)
			if !exists || !node.Healthy || node.Address == "" {
				continue
			}
			assignment := engine.StageAssignment{
				ModelID: previous.Model.SourceModelID, StartLayer: stage.StartLayer,
				EndLayer: stage.EndLayer, Engine: stage.Engine, IsFinal: i == len(previous.Stages)-1,
			}
			if err := s.nodeClient.unloadPipelineStage(ctx, node.ID, node.Address, assignment); err != nil {
				var stageErr *pipelineStageHTTPError
				if errors.As(err, &stageErr) && strings.HasPrefix(stageErr.status, "404 ") {
					continue
				}
				return config.PipelinePlan{}, fmt.Errorf("unload prior pipeline stage on node %s: %w", node.ID, err)
			}
		}
	}
	for _, stage := range stages {
		if err := s.nodeClient.loadPipelineStage(ctx, stage.nodeID, stage.address, stage.assignment); err != nil {
			return config.PipelinePlan{}, fmt.Errorf("load pipeline stage on node %s: %w", stage.nodeID, err)
		}
	}
	if err := s.pipelinePlans.set(plan); err != nil {
		return config.PipelinePlan{}, fmt.Errorf("%w: %v", errPipelinePlanPersistence, err)
	}
	return plan, nil
}

func findNodeStatus(nodes []cluster.NodeStatus, nodeID string) (cluster.NodeStatus, bool) {
	for _, node := range nodes {
		if node.ID == nodeID {
			return node, true
		}
	}
	return cluster.NodeStatus{}, false
}

func resolvePipelineStages(
	plan config.PipelinePlan,
	nodes []cluster.NodeStatus,
) ([]resolvedPipelineStage, error) {
	if err := plan.Validate(); err != nil {
		return nil, err
	}
	byID := make(map[string]cluster.NodeStatus, len(nodes))
	for _, node := range nodes {
		byID[node.ID] = node
	}
	stages := make([]resolvedPipelineStage, len(plan.Stages))
	for i, spec := range plan.Stages {
		node, exists := byID[spec.NodeID]
		if !exists || !node.Healthy || node.Address == "" ||
			node.MaxLayersItCanHost < spec.EndLayer-spec.StartLayer ||
			!nodeSupportsShardedModel(node, plan.Model) ||
			!nodeHasReadyStageEngine(node, spec.Engine, plan.Model.SourceModelID, spec.EndLayer-spec.StartLayer) {
			return nil, fmt.Errorf("pipeline stage node %q is no longer ready", spec.NodeID)
		}
		nextAddress := ""
		if i+1 < len(plan.Stages) {
			nextNode, ok := byID[plan.Stages[i+1].NodeID]
			if !ok || !nextNode.Healthy || nextNode.Address == "" {
				return nil, fmt.Errorf("next pipeline stage node %q is no longer available", plan.Stages[i+1].NodeID)
			}
			if strings.HasPrefix(strings.ToLower(nextNode.Address), "https://") {
				nextAddress = strings.TrimRight(nextNode.Address, "/") + engine.StageForwardPath
			}
		}
		stages[i] = resolvedPipelineStage{
			spec: spec, nodeID: node.ID, address: strings.TrimRight(node.Address, "/"),
			assignment: engine.StageAssignment{
				ModelID: plan.Model.SourceModelID, StartLayer: spec.StartLayer, EndLayer: spec.EndLayer,
				Engine: spec.Engine, NextStageAddr: nextAddress, IsFinal: i == len(plan.Stages)-1,
			},
		}
	}
	return stages, nil
}

func nodeSupportsShardedModel(node cluster.NodeStatus, model config.ShardedModelConfig) bool {
	for _, candidate := range node.SupportedShardModels {
		if strings.EqualFold(candidate, model.SourceModelID) ||
			strings.EqualFold(candidate, model.LogicalID) {
			return true
		}
	}
	return false
}

func nodeHasReadyStageEngine(node cluster.NodeStatus, engineName, modelID string, layers int) bool {
	for _, status := range node.Engines {
		if status.Ready && strings.EqualFold(status.Name, engineName) &&
			status.PipelineStage != nil &&
			status.PipelineStage.ProtocolVersion == engine.PipelineStageProtocolVersion &&
			status.PipelineStage.SupportsModel(modelID, layers) {
			return true
		}
	}
	return false
}

func (s *Server) runShardedPipeline(
	requestCtx context.Context,
	model config.ShardedModelConfig,
	prompt string,
	options engine.GenerationOptions,
	onToken func(string) error,
) (string, error) {
	ctx, cancel := context.WithCancel(requestCtx)
	defer cancel()

	releasePipelineRequest, err := s.pipelineRequests.begin(ctx, model.LogicalID)
	if err != nil {
		return "", err
	}
	defer releasePipelineRequest()

	_, stages, err := s.pipelineForModel(model)
	if err != nil {
		return "", err
	}
	gate, ok := s.dispatcher.capacityGate.(*dispatchCapacityGate)
	if !ok {
		return "", errors.New("pipeline capacity gate is unavailable")
	}
	targets := make([]workpool.CapacityTarget, len(stages))
	for i, stage := range stages {
		targets[i] = workpool.CapacityTarget{NodeID: stage.nodeID, Address: stage.address}
	}
	reservation, err := gate.ReservePipeline(ctx, targets)
	if err != nil {
		return "", fmt.Errorf("reserve pipeline capacity: %w", err)
	}
	defer reservation.Release()

	for _, stage := range stages {
		if err := s.nodeClient.loadPipelineStage(ctx, stage.nodeID, stage.address, stage.assignment); err != nil {
			if ctx.Err() == nil {
				s.invalidatePipelinePlan(model.LogicalID, err)
			}
			return "", fmt.Errorf("load pipeline stage on node %s: %w", stage.nodeID, err)
		}
	}

	requestID, err := newPipelineRequestID()
	if err != nil {
		return "", fmt.Errorf("create pipeline request ID: %w", err)
	}
	opened := make([]resolvedPipelineStage, 0, len(stages))
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := len(opened) - 1; i >= 0; i-- {
			stage := opened[i]
			if err := s.nodeClient.closePipelineStageRequest(
				closeCtx, stage.nodeID, stage.address, stage.assignment, requestID,
			); err != nil {
				log.Printf("orchestrator: close pipeline request %s on node %s failed: %v", requestID, stage.nodeID, err)
			}
		}
	}()
	for _, stage := range stages {
		if err := s.nodeClient.openPipelineStageRequest(
			ctx, stage.nodeID, stage.address, stage.assignment, requestID,
		); err != nil {
			if ctx.Err() == nil {
				s.invalidatePipelinePlan(model.LogicalID, err)
			}
			return "", fmt.Errorf("open pipeline request on node %s: %w", stage.nodeID, err)
		}
		opened = append(opened, stage)
	}

	var output strings.Builder
	var activation *engine.ActivationTensor
	var token *engine.PipelineToken
	promptTokens := 0
	for i, stage := range stages {
		request := engine.PipelineStageRequest{
			ProtocolVersion: engine.PipelineStageProtocolVersion,
			RequestID:       requestID,
			Assignment:      stage.assignment,
			Temperature:     options.Temperature,
			MaxTokens:       options.MaxTokens,
		}
		activationBytes := 0
		if i == 0 {
			request.Prompt = prompt
			activationBytes = len(prompt)
		} else {
			request.Activation = activation
			if activation != nil {
				activationBytes = len(activation.Data)
			}
		}
		metricKey := pipelineStageMetricKey{
			logicalID: model.LogicalID, stageIndex: i, nodeID: stage.nodeID,
			startLayer: stage.spec.StartLayer, endLayer: stage.spec.EndLayer,
		}
		finishMetrics := s.pipelineMetrics.begin(metricKey)
		stageOutput, activationTransferLatency, err := s.nodeClient.prefillPipelineStage(
			ctx, stage.nodeID, stage.address, request,
		)
		finishMetrics(activationBytes, activationTransferLatency, err == nil)
		if err != nil {
			if ctx.Err() == nil {
				s.invalidatePipelinePlan(model.LogicalID, err)
			}
			return "", fmt.Errorf("prefill pipeline stage on node %s: %w", stage.nodeID, err)
		}
		if i+1 == len(stages) {
			token = stageOutput.Token
			promptTokens = stageOutput.PromptTokens
		} else {
			activation = stageOutput.Activation
		}
	}
	if token == nil || promptTokens <= 0 {
		return "", errors.New("final pipeline prefill returned no token or prompt length")
	}

	generatedTokens := 0
	position := int64(promptTokens)
	for {
		if token.Text != "" {
			output.WriteString(token.Text)
			if onToken != nil {
				if err := onToken(token.Text); err != nil {
					return "", err
				}
			}
		}
		generatedTokens++
		if token.Done || (options.MaxTokens > 0 && generatedTokens >= options.MaxTokens) {
			break
		}
		if generatedTokens >= maxPipelineGeneratedTokens {
			return "", fmt.Errorf("pipeline exceeded the %d-token safety limit", maxPipelineGeneratedTokens)
		}

		inputTokenID := token.ID
		activation = nil
		for i, stage := range stages {
			request := engine.PipelineStageRequest{
				ProtocolVersion: engine.PipelineStageProtocolVersion,
				RequestID:       requestID,
				Assignment:      stage.assignment,
				Position:        position,
				Temperature:     options.Temperature,
				MaxTokens:       options.MaxTokens,
			}
			activationBytes := 8
			if i == 0 {
				request.InputTokenID = &inputTokenID
			} else {
				request.Activation = activation
				if activation != nil {
					activationBytes = len(activation.Data)
				}
			}
			metricKey := pipelineStageMetricKey{
				logicalID: model.LogicalID, stageIndex: i, nodeID: stage.nodeID,
				startLayer: stage.spec.StartLayer, endLayer: stage.spec.EndLayer,
			}
			finishMetrics := s.pipelineMetrics.begin(metricKey)
			stageOutput, transferLatency, err := s.nodeClient.decodePipelineStage(
				ctx, stage.nodeID, stage.address, request,
			)
			finishMetrics(activationBytes, transferLatency, err == nil)
			if err != nil {
				if ctx.Err() == nil {
					s.invalidatePipelinePlan(model.LogicalID, err)
				}
				return "", fmt.Errorf("decode pipeline stage on node %s: %w", stage.nodeID, err)
			}
			if i+1 == len(stages) {
				token = stageOutput.Token
			} else {
				activation = stageOutput.Activation
			}
		}
		if token == nil {
			return "", errors.New("final pipeline decode returned no token")
		}
		position++
	}
	return output.String(), nil
}

func newPipelineRequestID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func (s *Server) invalidatePipelinePlan(logicalID string, cause error) {
	if err := s.pipelinePlans.remove(logicalID); err != nil {
		log.Printf("orchestrator: remove failed pipeline plan %s after %v: %v", logicalID, cause, err)
	}
}

func pipelineHTTPStatus(err error) int {
	var stageErr *pipelineStageHTTPError
	if errors.As(err, &stageErr) {
		return http.StatusBadGateway
	}
	if errors.Is(err, errPipelinePlanPersistence) {
		return http.StatusInternalServerError
	}
	if errors.Is(err, scheduler.ErrNoPipelinePlacement) ||
		errors.Is(err, errPipelineNodeUnavailable) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) {
		return http.StatusServiceUnavailable
	}
	return http.StatusBadGateway
}

func openAIChatCompletionResponse(model, output, prompt, identity string) map[string]any {
	return map[string]any{
		"id":                 "chatcmpl-" + identity,
		"object":             "chat.completion",
		"created":            time.Now().Unix(),
		"model":              model,
		"system_fingerprint": "maistr0-" + identity,
		"choices": []map[string]any{
			{
				"index": 0,
				"message": map[string]string{
					"role": "assistant", "content": output,
				},
				"finish_reason": "stop",
			},
		},
		"usage": map[string]int{
			"prompt_tokens": len(prompt) / 4, "completion_tokens": len(output) / 4,
			"total_tokens": (len(prompt) + len(output)) / 4,
		},
	}
}

func (s *Server) handleShardedOpenAIChatCompletion(
	w http.ResponseWriter,
	r *http.Request,
	model config.ShardedModelConfig,
	prompt string,
	options engine.GenerationOptions,
	streaming bool,
) {
	if !streaming {
		output, err := s.runShardedPipelineWithTimeout(r.Context(), model, prompt, options, nil)
		if err != nil {
			if r.Context().Err() != nil {
				return
			}
			writeOpenAIPipelineError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, openAIChatCompletionResponse(
			model.LogicalID, output, prompt, "pipeline",
		))
		return
	}

	stream := newOpenAIStreamWriter(w, model.LogicalID)
	_, err := s.runShardedPipelineWithTimeout(r.Context(), model, prompt, options, func(token string) error {
		payload, err := json.Marshal(map[string]string{"output": token})
		if err != nil {
			return err
		}
		frame := append(append([]byte("data: "), payload...), '\n', '\n')
		_, err = stream.Write(frame)
		return err
	})
	if err != nil {
		if r.Context().Err() != nil {
			return
		}
		if stream.status == 0 {
			writeOpenAIPipelineError(w, err)
			return
		}
		payload, marshalErr := json.Marshal(map[string]string{"error": err.Error()})
		if marshalErr != nil {
			log.Printf("orchestrator: encode pipeline stream error: %v", marshalErr)
			return
		}
		_, writeErr := stream.Write(append(append([]byte("data: "), payload...), '\n', '\n'))
		if writeErr != nil {
			log.Printf("orchestrator: write pipeline stream error: %v", writeErr)
		}
		return
	}
	if stream.status == 0 {
		stream.WriteHeader(http.StatusOK)
	}
	stream.finish()
}

func writeOpenAIPipelineError(w http.ResponseWriter, err error) {
	writeJSON(w, pipelineHTTPStatus(err), map[string]any{
		"error": map[string]string{"message": err.Error(), "type": "server_error"},
	})
}

func (s *Server) runShardedPipelineWithTimeout(
	requestCtx context.Context,
	model config.ShardedModelConfig,
	prompt string,
	options engine.GenerationOptions,
	onToken func(string) error,
) (string, error) {
	timeout := s.dispatcher.dispatchTimeout
	if timeout <= 0 {
		return s.runShardedPipeline(requestCtx, model, prompt, options, onToken)
	}
	ctx, cancel := context.WithTimeout(requestCtx, timeout)
	defer cancel()
	return s.runShardedPipeline(ctx, model, prompt, options, onToken)
}
