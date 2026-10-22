package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
)

func (s *Simulated) LoadStage(ctx context.Context, assignment StageAssignment) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	assignment.Normalize()
	if err := assignment.Validate(); err != nil {
		return err
	}
	key := stageAssignmentKey(assignment)
	s.stageMu.Lock()
	defer s.stageMu.Unlock()
	if s.stages == nil {
		s.stages = make(map[string]StageAssignment)
	}
	if current, exists := s.stages[key]; exists {
		if current != assignment {
			return errors.New("a different stage assignment is already loaded for this layer range")
		}
		return nil
	}
	s.stages[key] = assignment
	return nil
}

func (s *Simulated) UnloadStage(ctx context.Context, assignment StageAssignment) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	assignment.Normalize()
	key := stageAssignmentKey(assignment)
	s.stageMu.Lock()
	defer s.stageMu.Unlock()
	if _, exists := s.stages[key]; !exists {
		return errors.New("stage assignment is not loaded")
	}
	delete(s.stages, key)
	return nil
}

func (s *Simulated) ForwardStage(
	ctx context.Context,
	assignment StageAssignment,
	activations io.Reader,
	kvCache []byte,
	emit func([]byte) error,
) ([]byte, error) {
	assignment.Normalize()
	key := stageAssignmentKey(assignment)
	s.stageMu.Lock()
	current, loaded := s.stages[key]
	s.stageMu.Unlock()
	if !loaded || current != assignment {
		return nil, fmt.Errorf("stage assignment for model %q is not loaded", assignment.ModelID)
	}

	updatedCache := append([]byte(nil), kvCache...)
	buffer := make([]byte, stageForwardChunkSize)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, err := activations.Read(buffer)
		if n > 0 {
			chunk := append([]byte(nil), buffer[:n]...)
			if emitErr := emit(chunk); emitErr != nil {
				return nil, emitErr
			}
			updatedCache = append(updatedCache, chunk...)
		}
		if err == io.EOF {
			return updatedCache, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

func (s *Simulated) ForwardStageRequest(
	ctx context.Context,
	assignment StageAssignment,
	request StageForwardRequest,
	emit func(StageForwardFrame) error,
) ([]byte, error) {
	if request.Prompt != "" {
		if request.Activations != nil {
			if _, err := io.Copy(io.Discard, request.Activations); err != nil {
				return nil, fmt.Errorf("consume unused stage input frames: %w", err)
			}
		}
		request.Activations = bytes.NewReader([]byte(request.Prompt))
	}
	if request.Activations == nil {
		request.Activations = bytes.NewReader(nil)
	}
	if !assignment.IsFinal {
		return s.ForwardStage(ctx, assignment, request.Activations, request.KVCache, func(data []byte) error {
			return emit(StageForwardFrame{Type: StageFrameActivation, Data: data})
		})
	}

	assignment.Normalize()
	key := stageAssignmentKey(assignment)
	s.stageMu.Lock()
	current, loaded := s.stages[key]
	s.stageMu.Unlock()
	if !loaded || current != assignment {
		return nil, fmt.Errorf("stage assignment for model %q is not loaded", assignment.ModelID)
	}
	input, err := io.ReadAll(request.Activations)
	if err != nil {
		return nil, fmt.Errorf("read simulated stage input: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	output, err := s.GenerateWithOptions(ctx, assignment.ModelID, string(input), request.Options)
	if err != nil {
		return nil, err
	}
	if err := emit(StageForwardFrame{Type: StageFrameToken, Data: []byte(output)}); err != nil {
		return nil, err
	}
	return append(append([]byte(nil), request.KVCache...), input...), nil
}

func stageAssignmentKey(assignment StageAssignment) string {
	return fmt.Sprintf("%s\x00%d\x00%d", assignment.ModelID, assignment.StartLayer, assignment.EndLayer)
}

func (s *Simulated) OpenPipelineRequest(
	ctx context.Context,
	assignment StageAssignment,
	requestID string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	assignment.Normalize()
	if err := assignment.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(requestID) == "" {
		return errors.New("request_id must not be empty")
	}
	s.stageMu.Lock()
	defer s.stageMu.Unlock()
	key := stageAssignmentKey(assignment)
	if loaded, exists := s.stages[key]; !exists || loaded != assignment {
		return errors.New("pipeline stage assignment is not loaded")
	}
	if s.stageRequests == nil {
		s.stageRequests = make(map[string]*simulatedPipelineRequest)
	}
	requestKey := key + "\x00" + requestID
	if _, exists := s.stageRequests[requestKey]; exists {
		return errors.New("pipeline request is already open")
	}
	s.stageRequests[requestKey] = &simulatedPipelineRequest{}
	return nil
}

func (s *Simulated) PrefillPipelineStage(
	ctx context.Context,
	assignment StageAssignment,
	request PipelineStageRequest,
) (PipelineStageOutput, error) {
	if err := request.Validate(true); err != nil {
		return PipelineStageOutput{}, err
	}
	return s.runSimulatedPipelineStep(ctx, assignment, request, true)
}

func (s *Simulated) DecodePipelineStage(
	ctx context.Context,
	assignment StageAssignment,
	request PipelineStageRequest,
) (PipelineStageOutput, error) {
	if err := request.Validate(false); err != nil {
		return PipelineStageOutput{}, err
	}
	return s.runSimulatedPipelineStep(ctx, assignment, request, false)
}

func (s *Simulated) runSimulatedPipelineStep(
	ctx context.Context,
	assignment StageAssignment,
	request PipelineStageRequest,
	prefill bool,
) (PipelineStageOutput, error) {
	if err := ctx.Err(); err != nil {
		return PipelineStageOutput{}, err
	}
	assignment.Normalize()
	if request.Assignment != assignment {
		return PipelineStageOutput{}, errors.New("pipeline request assignment does not match loaded stage")
	}
	stageKey := stageAssignmentKey(assignment)
	requestKey := stageKey + "\x00" + request.RequestID
	s.stageMu.Lock()
	loaded, loadedOK := s.stages[stageKey]
	state, requestOK := s.stageRequests[requestKey]
	s.stageMu.Unlock()
	if !loadedOK || loaded != assignment || !requestOK {
		return PipelineStageOutput{}, errors.New("pipeline request or stage is not loaded")
	}

	input := request.Activation
	if assignment.StartLayer == 0 {
		if prefill {
			data := []byte(request.Prompt)
			input = &ActivationTensor{DType: TensorUint8, Shape: []int64{int64(len(data))}, Data: data}
		} else {
			data := make([]byte, 8)
			binary.LittleEndian.PutUint64(data, uint64(*request.InputTokenID))
			input = &ActivationTensor{DType: TensorUint8, Shape: []int64{8}, Data: data}
		}
	} else if input != nil {
		clone := *input
		clone.Shape = append([]int64(nil), input.Shape...)
		clone.Data = append([]byte(nil), input.Data...)
		input = &clone
	}

	if !assignment.IsFinal {
		return PipelineStageOutput{Activation: input}, nil
	}
	if input == nil || input.DType != TensorUint8 {
		return PipelineStageOutput{}, errors.New("simulated final stage requires uint8 prompt activations")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if prefill {
		output, err := s.GenerateWithOptions(ctx, assignment.ModelID, string(input.Data), GenerationOptions{
			Temperature: request.Temperature, MaxTokens: request.MaxTokens,
		})
		if err != nil {
			return PipelineStageOutput{}, err
		}
		state.tokens = simulatedTextTokens(output)
		state.nextToken = 0
	}
	token, err := simulatedNextToken(state, request.MaxTokens)
	if err != nil {
		return PipelineStageOutput{}, err
	}
	output := PipelineStageOutput{Token: &token}
	if prefill {
		output.PromptTokens = int(input.Shape[0])
	}
	return output, nil
}

func (s *Simulated) ClosePipelineRequest(
	ctx context.Context,
	assignment StageAssignment,
	requestID string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	assignment.Normalize()
	stageKey := stageAssignmentKey(assignment)
	requestKey := stageKey + "\x00" + requestID
	s.stageMu.Lock()
	defer s.stageMu.Unlock()
	if _, exists := s.stageRequests[requestKey]; !exists {
		return errors.New("pipeline request is not open")
	}
	delete(s.stageRequests, requestKey)
	return nil
}

func (s *Simulated) ProbePipelineStageCapabilities(
	ctx context.Context,
) (*PipelineStageCapabilities, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	models, err := s.ListModels()
	if err != nil {
		return nil, err
	}
	capabilities := &PipelineStageCapabilities{
		ProtocolVersion: PipelineStageProtocolVersion,
		Runtime:         s.Name(),
		Models:          make([]PipelineStageModelCapability, 0, len(models)),
		TensorDTypes: []TensorDType{
			TensorFloat16, TensorBFloat16, TensorFloat32, TensorInt64, TensorUint8,
		},
		DeviceBackends: []string{"simulated"},
	}
	for _, model := range models {
		capabilities.Models = append(capabilities.Models, PipelineStageModelCapability{
			ModelID: model.Name, MaxLayers: 256,
		})
	}
	return capabilities, capabilities.Validate()
}

func simulatedTextTokens(output string) []PipelineToken {
	words := strings.Fields(output)
	tokens := make([]PipelineToken, len(words))
	for i, word := range words {
		if i+1 < len(words) {
			word += " "
		}
		tokens[i] = PipelineToken{ID: int64(i + 1), Text: word}
	}
	return tokens
}

func simulatedNextToken(state *simulatedPipelineRequest, maxTokens int) (PipelineToken, error) {
	if state.nextToken >= len(state.tokens) {
		return PipelineToken{}, errors.New("simulated pipeline produced no more tokens")
	}
	token := state.tokens[state.nextToken]
	state.nextToken++
	switch {
	case maxTokens > 0 && state.nextToken >= maxTokens:
		token.Done = true
		token.FinishReason = "length"
	case state.nextToken >= len(state.tokens):
		token.Done = true
		token.FinishReason = "stop"
	}
	return token, nil
}
