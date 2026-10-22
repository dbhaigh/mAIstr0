package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/maistr0/maistr0/internal/engine"
)

const maxPipelineStageBytes = 64 << 20

type pipelineStageHTTPError struct {
	nodeID string
	status string
	body   string
}

type pipelineStageWriteResult struct {
	duration time.Duration
	err      error
}

func (err *pipelineStageHTTPError) Error() string {
	if err.body == "" {
		return fmt.Sprintf("pipeline stage on node %s returned %s", err.nodeID, err.status)
	}
	return fmt.Sprintf("pipeline stage on node %s returned %s: %s", err.nodeID, err.status, err.body)
}

func (c *nodeClient) loadPipelineStage(
	ctx context.Context,
	nodeID string,
	address string,
	assignment engine.StageAssignment,
) error {
	return c.pipelineStageControl(ctx, nodeID, address, engine.StageLoadPath, assignment)
}

func (c *nodeClient) unloadPipelineStage(
	ctx context.Context,
	nodeID string,
	address string,
	assignment engine.StageAssignment,
) error {
	return c.pipelineStageControl(ctx, nodeID, address, engine.StageUnloadPath, assignment)
}

func (c *nodeClient) openPipelineStageRequest(
	ctx context.Context,
	nodeID, address string,
	assignment engine.StageAssignment,
	requestID string,
) error {
	return c.pipelineStageRequestControl(ctx, nodeID, address, engine.StageOpenPath,
		engine.PipelineStageControlRequest{
			ProtocolVersion: engine.PipelineStageProtocolVersion,
			RequestID:       requestID, Assignment: assignment,
		})
}

func (c *nodeClient) closePipelineStageRequest(
	ctx context.Context,
	nodeID, address string,
	assignment engine.StageAssignment,
	requestID string,
) error {
	return c.pipelineStageRequestControl(ctx, nodeID, address, engine.StageClosePath,
		engine.PipelineStageControlRequest{
			ProtocolVersion: engine.PipelineStageProtocolVersion,
			RequestID:       requestID, Assignment: assignment,
		})
}

func (c *nodeClient) pipelineStageRequestControl(
	ctx context.Context,
	nodeID, address, path string,
	payload engine.PipelineStageControlRequest,
) error {
	if err := payload.Validate(); err != nil {
		return err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode pipeline stage request control: %w", err)
	}
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, strings.TrimRight(address, "/")+path, bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("create pipeline stage request control: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.DoDispatch(request)
	if err != nil {
		return fmt.Errorf("send pipeline stage request control to node %s: %w", nodeID, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		errorBody, readErr := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		if readErr != nil {
			return fmt.Errorf("pipeline stage request control returned %s and reading error failed: %w", response.Status, readErr)
		}
		return &pipelineStageHTTPError{
			nodeID: nodeID, status: response.Status,
			body: strings.TrimSpace(string(errorBody)),
		}
	}
	return nil
}

func (c *nodeClient) prefillPipelineStage(
	ctx context.Context,
	nodeID, address string,
	request engine.PipelineStageRequest,
) (engine.PipelineStageOutput, time.Duration, error) {
	return c.pipelineStageStep(ctx, nodeID, address, engine.StagePrefillPath, request, true)
}

func (c *nodeClient) decodePipelineStage(
	ctx context.Context,
	nodeID, address string,
	request engine.PipelineStageRequest,
) (engine.PipelineStageOutput, time.Duration, error) {
	return c.pipelineStageStep(ctx, nodeID, address, engine.StageDecodePath, request, false)
}

func (c *nodeClient) pipelineStageStep(
	ctx context.Context,
	nodeID, address, path string,
	stageRequest engine.PipelineStageRequest,
	prefill bool,
) (engine.PipelineStageOutput, time.Duration, error) {
	if err := stageRequest.Validate(prefill); err != nil {
		return engine.PipelineStageOutput{}, 0, err
	}
	reader, writer := io.Pipe()
	defer reader.Close()
	writerDone := make(chan pipelineStageWriteResult, 1)
	go func() {
		started := time.Now()
		err := engine.WriteStageMessage(writer, engine.StageMessageHeader{
			Request: stageRequest,
		}, stageRequest.Activation)
		if err != nil {
			_ = writer.CloseWithError(err)
		} else {
			err = writer.Close()
		}
		writerDone <- pipelineStageWriteResult{duration: time.Since(started), err: err}
	}()
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, strings.TrimRight(address, "/")+path, reader,
	)
	if err != nil {
		_ = reader.CloseWithError(err)
		return engine.PipelineStageOutput{}, 0, fmt.Errorf("create pipeline stage request for node %s: %w", nodeID, err)
	}
	request.Header.Set("Content-Type", engine.PipelineStageV2ContentType)
	request.Header.Set("Accept", engine.PipelineStageV2ContentType)
	response, err := c.DoDispatch(request)
	if err != nil {
		_ = reader.CloseWithError(err)
		return engine.PipelineStageOutput{}, 0, fmt.Errorf("send pipeline stage request to node %s: %w", nodeID, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_ = reader.CloseWithError(errors.New("stage endpoint rejected request"))
		errorBody, readErr := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		if readErr != nil {
			return engine.PipelineStageOutput{}, 0, fmt.Errorf("pipeline stage returned %s and reading error failed: %w", response.Status, readErr)
		}
		return engine.PipelineStageOutput{}, 0, &pipelineStageHTTPError{
			nodeID: nodeID, status: response.Status,
			body: strings.TrimSpace(string(errorBody)),
		}
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != engine.PipelineStageV2ContentType {
		return engine.PipelineStageOutput{}, 0, fmt.Errorf("pipeline stage returned unsupported content type %q", response.Header.Get("Content-Type"))
	}
	message, err := engine.ReadStageMessage(io.LimitReader(response.Body, maxPipelineStageBytes+maxPipelineStageBytes/16))
	if err != nil {
		return engine.PipelineStageOutput{}, 0, fmt.Errorf("decode pipeline stage response from node %s: %w", nodeID, err)
	}
	writeResult := <-writerDone
	if writeResult.err != nil {
		return engine.PipelineStageOutput{}, 0, fmt.Errorf("write pipeline stage request to node %s: %w", nodeID, writeResult.err)
	}
	if message.Request.RequestID != stageRequest.RequestID ||
		!samePipelineStageAssignment(message.Request.Assignment, stageRequest.Assignment) ||
		message.Output == nil {
		return engine.PipelineStageOutput{}, writeResult.duration, errors.New("pipeline stage response does not match its request")
	}
	if err := message.Output.Validate(stageRequest.Assignment.IsFinal); err != nil {
		return engine.PipelineStageOutput{}, writeResult.duration, fmt.Errorf("invalid pipeline stage output: %w", err)
	}
	if prefill && stageRequest.Assignment.IsFinal && message.Output.PromptTokens <= 0 {
		return engine.PipelineStageOutput{}, writeResult.duration, errors.New("final stage prefill omitted prompt token count")
	}
	if !prefill && message.Output.PromptTokens != 0 {
		return engine.PipelineStageOutput{}, writeResult.duration, errors.New("decode stage response unexpectedly included prompt token count")
	}
	return *message.Output, writeResult.duration, nil
}

func samePipelineStageAssignment(left, right engine.StageAssignment) bool {
	left.Normalize()
	right.Normalize()
	return left == right
}

func (c *nodeClient) pipelineStageControl(
	ctx context.Context,
	nodeID, address, path string,
	assignment engine.StageAssignment,
) error {
	if err := assignment.Validate(); err != nil {
		return err
	}
	body, err := json.Marshal(assignment)
	if err != nil {
		return fmt.Errorf("encode stage assignment: %w", err)
	}
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, strings.TrimRight(address, "/")+path, bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("create stage control request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.DoDispatch(request)
	if err != nil {
		return fmt.Errorf("send stage control request to node: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		if readErr != nil {
			return fmt.Errorf("stage control returned %s and reading its error failed: %w", response.Status, readErr)
		}
		return &pipelineStageHTTPError{
			nodeID: nodeID, status: response.Status,
			body: strings.TrimSpace(string(body)),
		}
	}
	return nil
}

func (c *nodeClient) forwardPipelineStage(
	ctx context.Context,
	nodeID, address string,
	assignment engine.StageAssignment,
	prompt string,
	activations []byte,
	kvCache []byte,
	options engine.GenerationOptions,
	onToken func(string) error,
) ([]byte, []byte, time.Duration, error) {
	if err := assignment.Validate(); err != nil {
		return nil, nil, 0, err
	}
	if len(prompt)+len(activations)+len(kvCache) > maxPipelineStageBytes {
		return nil, nil, 0, fmt.Errorf("pipeline stage request exceeds %d bytes", maxPipelineStageBytes)
	}
	reader, writer := io.Pipe()
	defer reader.Close()
	writerDone := make(chan pipelineStageWriteResult, 1)
	go func() {
		started := time.Now()
		err := writePipelineStageRequest(
			writer, assignment, prompt, activations, kvCache, options,
		)
		writerDone <- pipelineStageWriteResult{duration: time.Since(started), err: err}
	}()
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, strings.TrimRight(address, "/")+engine.StageForwardPath, reader,
	)
	if err != nil {
		_ = reader.CloseWithError(err)
		return nil, nil, 0, fmt.Errorf("create stage forward request: %w", err)
	}
	request.Header.Set("Content-Type", "application/x-ndjson")
	request.Header.Set("Accept", "application/x-ndjson")
	response, err := c.DoDispatch(request)
	if err != nil {
		_ = reader.CloseWithError(err)
		return nil, nil, 0, fmt.Errorf("send stage forward request to node %s: %w", nodeID, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_ = reader.CloseWithError(errors.New("stage endpoint rejected request"))
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 4<<10))
		if readErr != nil {
			return nil, nil, 0, fmt.Errorf("stage forward returned %s and reading its error failed: %w", response.Status, readErr)
		}
		return nil, nil, 0, &pipelineStageHTTPError{
			nodeID: nodeID, status: response.Status, body: strings.TrimSpace(string(body)),
		}
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxPipelineStageBytes))
	var output bytes.Buffer
	var updatedCache []byte
	for {
		var frame engine.StageForwardFrame
		if err := decoder.Decode(&frame); err != nil {
			if err == io.EOF {
				return nil, nil, 0, errors.New("stage response ended without a complete frame")
			}
			return nil, nil, 0, fmt.Errorf("decode stage response from node %s: %w", nodeID, err)
		}
		switch frame.Type {
		case engine.StageFrameActivation:
			if assignment.IsFinal {
				return nil, nil, 0, errors.New("final pipeline stage returned activation data instead of tokens")
			}
			if output.Len()+len(frame.Data) > maxPipelineStageBytes {
				return nil, nil, 0, fmt.Errorf("pipeline activation output exceeds %d bytes", maxPipelineStageBytes)
			}
			if _, err := output.Write(frame.Data); err != nil {
				return nil, nil, 0, err
			}
		case engine.StageFrameToken:
			if !assignment.IsFinal {
				return nil, nil, 0, errors.New("non-final pipeline stage returned token data")
			}
			if !utf8.Valid(frame.Data) {
				return nil, nil, 0, errors.New("final pipeline stage returned non-UTF-8 token data")
			}
			if onToken != nil {
				if err := onToken(string(frame.Data)); err != nil {
					return nil, nil, 0, err
				}
			}
		case engine.StageFrameComplete:
			updatedCache = append([]byte(nil), frame.KVCache...)
			writeResult := <-writerDone
			if writeResult.err != nil {
				return nil, nil, 0, fmt.Errorf("write stage forward request: %w", writeResult.err)
			}
			return output.Bytes(), updatedCache, writeResult.duration, nil
		case engine.StageFrameError:
			return nil, nil, 0, fmt.Errorf("stage on node %s failed: %s", nodeID, frame.Error)
		default:
			return nil, nil, 0, fmt.Errorf("stage on node %s returned unexpected frame type %q", nodeID, frame.Type)
		}
	}
}

func writePipelineStageRequest(
	writer *io.PipeWriter,
	assignment engine.StageAssignment,
	prompt string,
	activations []byte,
	kvCache []byte,
	options engine.GenerationOptions,
) error {
	encoder := json.NewEncoder(writer)
	if err := encoder.Encode(engine.StageForwardFrame{
		Type: engine.StageFrameStart, Assignment: &assignment, Prompt: prompt,
		Temperature: options.Temperature, MaxTokens: options.MaxTokens, KVCache: kvCache,
	}); err != nil {
		_ = writer.CloseWithError(err)
		return err
	}
	for offset := 0; offset < len(activations); offset += 32 << 10 {
		end := min(offset+(32<<10), len(activations))
		if err := encoder.Encode(engine.StageForwardFrame{
			Type: engine.StageFrameActivation, Data: activations[offset:end],
		}); err != nil {
			_ = writer.CloseWithError(err)
			return err
		}
	}
	if err := encoder.Encode(engine.StageForwardFrame{Type: engine.StageFrameEnd}); err != nil {
		_ = writer.CloseWithError(err)
		return err
	}
	return writer.Close()
}
