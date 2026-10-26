package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/httpx"
)

const (
	StageLoadPath    = "/stage/load"
	StageForwardPath = "/stage/forward"
	StageUnloadPath  = "/stage/unload"

	StageFrameStart      = "start"
	StageFrameActivation = "activation"
	StageFrameEnd        = "end"
	StageFrameComplete   = "complete"
	StageFrameError      = "error"
	StageFrameToken      = "token"

	stageForwardChunkSize = 32 << 10
	stageErrorBodyLimit   = 4 << 10
)

// StageAssignment describes one local contiguous model stage.
type StageAssignment struct {
	ModelID       string `json:"model_id"`
	StartLayer    int    `json:"start_layer"`
	EndLayer      int    `json:"end_layer"`
	NextStageAddr string `json:"next_stage_addr,omitempty"`
	Engine        string `json:"engine,omitempty"`
	IsFinal       bool   `json:"is_final,omitempty"`
}

func (assignment *StageAssignment) Normalize() {
	assignment.ModelID = strings.TrimSpace(assignment.ModelID)
	assignment.NextStageAddr = strings.TrimRight(strings.TrimSpace(assignment.NextStageAddr), "/")
	assignment.Engine = strings.ToLower(strings.TrimSpace(assignment.Engine))
}

func (assignment StageAssignment) Validate() error {
	if assignment.ModelID == "" {
		return errors.New("model_id must not be empty")
	}
	if assignment.StartLayer < 0 || assignment.EndLayer <= assignment.StartLayer {
		return errors.New("layer range must satisfy 0 <= start_layer < end_layer")
	}
	if assignment.NextStageAddr != "" {
		parsed, err := url.Parse(assignment.NextStageAddr)
		if err != nil || parsed.Hostname() == "" || !strings.EqualFold(parsed.Scheme, "https") || parsed.User != nil {
			return errors.New("next_stage_addr must be an absolute HTTPS URL without user information")
		}
	}
	return nil
}

// StageForwardFrame is one newline-delimited JSON record on the stage stream.
// Requests start with a start frame, carry activation frames, and finish with
// an end frame. Responses carry activation frames followed by a complete frame.
type StageForwardFrame struct {
	Type        string           `json:"type"`
	Assignment  *StageAssignment `json:"assignment,omitempty"`
	Prompt      string           `json:"prompt,omitempty"`
	Temperature float64          `json:"temperature,omitempty"`
	MaxTokens   int              `json:"max_tokens,omitempty"`
	Data        []byte           `json:"data,omitempty"`
	KVCache     []byte           `json:"kv_cache,omitempty"`
	Error       string           `json:"error,omitempty"`
}

type StageForwardRequest struct {
	Prompt      string
	Activations io.Reader
	KVCache     []byte
	Options     GenerationOptions
}

// PipelineStageEngine is an optional engine capability. Keeping it separate
// from Engine leaves the existing full-model inference contract unchanged.
type PipelineStageEngine interface {
	LoadStage(context.Context, StageAssignment) error
	UnloadStage(context.Context, StageAssignment) error
	ForwardStage(context.Context, StageAssignment, io.Reader, []byte, func([]byte) error) ([]byte, error)
}

// PipelineStageRequestEngine supports prompt prefill and typed streaming
// outputs. It is optional so existing stage engines remain source-compatible.
type PipelineStageRequestEngine interface {
	ForwardStageRequest(context.Context, StageAssignment, StageForwardRequest, func(StageForwardFrame) error) ([]byte, error)
}

// DecodeStageForwardStart reads and validates the opening frame of a forward
// stream.
func DecodeStageForwardStart(decoder *json.Decoder) (StageForwardFrame, error) {
	var frame StageForwardFrame
	if err := decoder.Decode(&frame); err != nil {
		return StageForwardFrame{}, fmt.Errorf("decode stage start frame: %w", err)
	}
	if frame.Type != StageFrameStart || frame.Assignment == nil {
		return StageForwardFrame{}, errors.New("stage forward stream must begin with a start frame and assignment")
	}
	if err := frame.Assignment.Validate(); err != nil {
		return StageForwardFrame{}, err
	}
	return frame, nil
}

// StageActivationStream decodes activation frames lazily so engines can
// consume a forward request without buffering the complete activation stream.
type StageActivationStream struct {
	decoder *json.Decoder
	current *bytes.Reader
	ended   bool
}

func NewStageActivationStream(decoder *json.Decoder) *StageActivationStream {
	return &StageActivationStream{decoder: decoder}
}

func (stream *StageActivationStream) Read(buffer []byte) (int, error) {
	for {
		if stream.current != nil {
			n, err := stream.current.Read(buffer)
			if err != io.EOF {
				return n, err
			}
			stream.current = nil
		}
		if stream.ended {
			return 0, io.EOF
		}
		var frame StageForwardFrame
		if err := stream.decoder.Decode(&frame); err != nil {
			if err == io.EOF {
				return 0, errors.New("stage forward stream ended without an end frame")
			}
			return 0, fmt.Errorf("decode stage activation frame: %w", err)
		}
		switch frame.Type {
		case StageFrameActivation:
			if len(frame.Data) == 0 {
				continue
			}
			stream.current = bytes.NewReader(frame.Data)
		case StageFrameEnd:
			stream.ended = true
		default:
			return 0, fmt.Errorf("unexpected stage forward frame type %q", frame.Type)
		}
	}
}

// HTTPPipelineStageEngine adapts a local serving runtime that implements the
// stage HTTP endpoints to the optional PipelineStageEngine capability.
type HTTPPipelineStageEngine struct {
	name             string
	baseURL          string
	client           *http.Client
	capabilityClient *http.Client
}

func NewHTTPPipelineStageEngine(name, baseURL string, client *http.Client) (*HTTPPipelineStageEngine, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Hostname() == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, fmt.Errorf("invalid pipeline stage server URL %q", baseURL)
	}
	if client == nil {
		client = httpx.NewClient(httpx.ClientOptions{Timeout: httpx.InferenceTimeout})
	}
	return &HTTPPipelineStageEngine{
		name: name, baseURL: strings.TrimRight(baseURL, "/"), client: client,
		capabilityClient: httpx.NewClient(httpx.ClientOptions{Timeout: httpx.ProbeTimeout}),
	}, nil
}

func (engine *HTTPPipelineStageEngine) Name() string { return engine.name }

func (engine *HTTPPipelineStageEngine) ProbePipelineStageCapabilities(
	ctx context.Context,
) (*PipelineStageCapabilities, error) {
	request, err := http.NewRequestWithContext(
		ctx, http.MethodGet, engine.baseURL+StageCapabilitiesPath, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("create stage capability probe: %w", err)
	}
	response, err := engine.capabilityClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("probe stage capabilities: %w", err)
	}
	defer closeutil.Discard(response.Body)
	if response.StatusCode == http.StatusNotFound || response.StatusCode == http.StatusMethodNotAllowed {
		return nil, nil
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, readErr := io.ReadAll(io.LimitReader(response.Body, stageErrorBodyLimit))
		if readErr != nil {
			return nil, fmt.Errorf("stage capability probe returned %s and reading its error failed: %w", response.Status, readErr)
		}
		return nil, fmt.Errorf("stage capability probe returned %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	var capabilities PipelineStageCapabilities
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxPipelineStageHeaderBytes))
	if err := decoder.Decode(&capabilities); err != nil {
		return nil, fmt.Errorf("decode stage capabilities: %w", err)
	}
	if err := capabilities.Validate(); err != nil {
		return nil, fmt.Errorf("invalid stage capabilities: %w", err)
	}
	if !strings.EqualFold(capabilities.Runtime, engine.name) {
		return nil, fmt.Errorf("stage runtime reported %q, configured engine is %q", capabilities.Runtime, engine.name)
	}
	return &capabilities, nil
}

func (engine *HTTPPipelineStageEngine) LoadStage(ctx context.Context, assignment StageAssignment) error {
	return engine.postJSON(ctx, StageLoadPath, engine.assignmentForServer(assignment))
}

func (engine *HTTPPipelineStageEngine) UnloadStage(ctx context.Context, assignment StageAssignment) error {
	return engine.postJSON(ctx, StageUnloadPath, engine.assignmentForServer(assignment))
}

func (engine *HTTPPipelineStageEngine) assignmentForServer(assignment StageAssignment) StageAssignment {
	if assignment.Engine == "" {
		assignment.Engine = engine.name
	}
	return assignment
}

func (engine *HTTPPipelineStageEngine) postJSON(ctx context.Context, path string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal stage request: %w", err)
	}
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, engine.baseURL+path, bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("create stage request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := engine.client.Do(request)
	if err != nil {
		return fmt.Errorf("send stage request: %w", err)
	}
	defer closeutil.Discard(response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, readErr := io.ReadAll(io.LimitReader(response.Body, stageErrorBodyLimit))
		if readErr != nil {
			return fmt.Errorf("stage request returned %s and reading its error failed: %w", response.Status, readErr)
		}
		return fmt.Errorf("stage request returned %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	return nil
}

func (engine *HTTPPipelineStageEngine) ForwardStage(
	ctx context.Context,
	assignment StageAssignment,
	activations io.Reader,
	kvCache []byte,
	emit func([]byte) error,
) ([]byte, error) {
	return engine.ForwardStageRequest(ctx, assignment, StageForwardRequest{
		Activations: activations, KVCache: kvCache,
	}, func(frame StageForwardFrame) error {
		if frame.Type != StageFrameActivation && frame.Type != StageFrameToken {
			return fmt.Errorf("unexpected stage output frame type %q", frame.Type)
		}
		return emit(frame.Data)
	})
}

func (engine *HTTPPipelineStageEngine) ForwardStageRequest(
	ctx context.Context,
	assignment StageAssignment,
	stageRequest StageForwardRequest,
	emit func(StageForwardFrame) error,
) ([]byte, error) {
	assignment = engine.assignmentForServer(assignment)
	reader, writer := io.Pipe()
	defer closeutil.Discard(reader)
	writerDone := make(chan error, 1)
	go func() {
		writerDone <- writeStageForwardRequest(
			writer, assignment, stageRequest.Prompt, stageRequest.Activations,
			stageRequest.KVCache, stageRequest.Options,
		)
	}()

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, engine.baseURL+StageForwardPath, reader)
	if err != nil {
		_ = reader.CloseWithError(err)
		return nil, fmt.Errorf("create stage forward request: %w", err)
	}
	request.Header.Set("Content-Type", "application/x-ndjson")
	request.Header.Set("Accept", "application/x-ndjson")
	response, err := engine.client.Do(request)
	if err != nil {
		_ = reader.CloseWithError(err)
		return nil, fmt.Errorf("send stage forward request: %w", err)
	}
	defer closeutil.Discard(response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, readErr := io.ReadAll(io.LimitReader(response.Body, stageErrorBodyLimit))
		if readErr != nil {
			return nil, fmt.Errorf("stage forward returned %s and reading its error failed: %w", response.Status, readErr)
		}
		return nil, fmt.Errorf("stage forward returned %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	mediaType, _, parseErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if parseErr != nil || mediaType != "application/x-ndjson" {
		return nil, fmt.Errorf("stage forward returned unsupported content type %q", response.Header.Get("Content-Type"))
	}
	decoder := json.NewDecoder(response.Body)
	for {
		var frame StageForwardFrame
		if err := decoder.Decode(&frame); err != nil {
			if err == io.EOF {
				return nil, errors.New("stage forward response ended without a complete frame")
			}
			return nil, fmt.Errorf("decode stage forward response: %w", err)
		}
		switch frame.Type {
		case StageFrameActivation, StageFrameToken:
			if len(frame.Data) > 0 {
				if err := emit(frame); err != nil {
					return nil, fmt.Errorf("write stage output: %w", err)
				}
			}
		case StageFrameComplete:
			if err := <-writerDone; err != nil {
				return nil, fmt.Errorf("write stage forward request: %w", err)
			}
			return frame.KVCache, nil
		case StageFrameError:
			return nil, fmt.Errorf("stage forward failed: %s", frame.Error)
		default:
			return nil, fmt.Errorf("unexpected stage response frame type %q", frame.Type)
		}
	}
}

func writeStageForwardRequest(
	writer *io.PipeWriter,
	assignment StageAssignment,
	prompt string,
	activations io.Reader,
	kvCache []byte,
	options GenerationOptions,
) error {
	if activations == nil {
		activations = bytes.NewReader(nil)
	}
	encoder := json.NewEncoder(writer)
	if err := encoder.Encode(StageForwardFrame{
		Type: StageFrameStart, Assignment: &assignment, Prompt: prompt,
		Temperature: options.Temperature, MaxTokens: options.MaxTokens, KVCache: kvCache,
	}); err != nil {
		_ = writer.CloseWithError(err)
		return err
	}
	buffer := make([]byte, stageForwardChunkSize)
	for {
		n, err := activations.Read(buffer)
		if n > 0 {
			data := append([]byte(nil), buffer[:n]...)
			if encodeErr := encoder.Encode(StageForwardFrame{Type: StageFrameActivation, Data: data}); encodeErr != nil {
				_ = writer.CloseWithError(encodeErr)
				return encodeErr
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = writer.CloseWithError(err)
			return err
		}
	}
	if err := encoder.Encode(StageForwardFrame{Type: StageFrameEnd}); err != nil {
		_ = writer.CloseWithError(err)
		return err
	}
	return writer.Close()
}
