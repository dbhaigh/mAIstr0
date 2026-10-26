package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"strings"

	"github.com/maistr0/maistr0/internal/closeutil"
)

const (
	PipelineStageProtocolVersion = 2

	StageOpenPath              = "/stage/open"
	StageCapabilitiesPath      = "/stage/capabilities"
	StagePrefillPath           = "/stage/prefill"
	StageDecodePath            = "/stage/decode"
	StageClosePath             = "/stage/close"
	PipelineStageV2ContentType = "application/vnd.maistr0.pipeline-stage-v2"

	maxPipelineStageHeaderBytes = 1 << 20
	maxPipelineTensorBytes      = 64 << 20
	maxPipelineTensorRank       = 8
)

type TensorDType string

const (
	TensorFloat16  TensorDType = "float16"
	TensorBFloat16 TensorDType = "bfloat16"
	TensorFloat32  TensorDType = "float32"
	TensorInt64    TensorDType = "int64"
	TensorUint8    TensorDType = "uint8"
)

type PipelineStageModelCapability struct {
	ModelID   string `json:"model_id"`
	MaxLayers int    `json:"max_layers"`
}

type PipelineStageCapabilities struct {
	ProtocolVersion int                            `json:"protocol_version"`
	Runtime         string                         `json:"runtime"`
	RuntimeVersion  string                         `json:"runtime_version,omitempty"`
	Models          []PipelineStageModelCapability `json:"models"`
	TensorDTypes    []TensorDType                  `json:"tensor_dtypes"`
	DeviceBackends  []string                       `json:"device_backends,omitempty"`
}

func (capabilities PipelineStageCapabilities) Validate() error {
	if capabilities.ProtocolVersion != PipelineStageProtocolVersion {
		return fmt.Errorf("unsupported pipeline stage protocol version %d", capabilities.ProtocolVersion)
	}
	if capabilities.Runtime == "" {
		return errors.New("stage runtime must not be empty")
	}
	if len(capabilities.Models) == 0 {
		return errors.New("stage runtime must advertise at least one model")
	}
	seenModels := make(map[string]struct{}, len(capabilities.Models))
	for _, model := range capabilities.Models {
		if model.ModelID == "" || model.MaxLayers <= 0 {
			return errors.New("stage model capability requires model_id and positive max_layers")
		}
		if _, exists := seenModels[model.ModelID]; exists {
			return fmt.Errorf("stage runtime advertises duplicate model %q", model.ModelID)
		}
		seenModels[model.ModelID] = struct{}{}
	}
	if len(capabilities.TensorDTypes) == 0 {
		return errors.New("stage runtime must advertise at least one tensor dtype")
	}
	seenDTypes := make(map[TensorDType]struct{}, len(capabilities.TensorDTypes))
	for _, dtype := range capabilities.TensorDTypes {
		if _, exists := tensorElementSize(dtype); !exists {
			return fmt.Errorf("stage runtime advertises unsupported tensor dtype %q", dtype)
		}
		if _, exists := seenDTypes[dtype]; exists {
			return fmt.Errorf("stage runtime advertises duplicate tensor dtype %q", dtype)
		}
		seenDTypes[dtype] = struct{}{}
	}
	return nil
}

func (capabilities PipelineStageCapabilities) SupportsModel(modelID string, layers int) bool {
	for _, model := range capabilities.Models {
		if strings.EqualFold(model.ModelID, modelID) && model.MaxLayers >= layers {
			return true
		}
	}
	return false
}

type PipelineStageCapabilityProvider interface {
	ProbePipelineStageCapabilities(context.Context) (*PipelineStageCapabilities, error)
}

// ActivationTensor is a contiguous row-major tensor transferred between
// adjacent pipeline stages. Data is encoded as a binary payload, not JSON.
type ActivationTensor struct {
	DType TensorDType `json:"dtype"`
	Shape []int64     `json:"shape"`
	Data  []byte      `json:"-"`
}

func (tensor ActivationTensor) Validate() error {
	elementSize, ok := tensorElementSize(tensor.DType)
	if !ok {
		return fmt.Errorf("unsupported activation dtype %q", tensor.DType)
	}
	if len(tensor.Shape) == 0 || len(tensor.Shape) > maxPipelineTensorRank {
		return fmt.Errorf("activation rank must be between 1 and %d", maxPipelineTensorRank)
	}
	elements := int64(1)
	for _, dimension := range tensor.Shape {
		if dimension <= 0 {
			return errors.New("activation dimensions must be positive")
		}
		if elements > math.MaxInt64/dimension {
			return errors.New("activation shape overflows")
		}
		elements *= dimension
	}
	if elements > int64(maxPipelineTensorBytes)/int64(elementSize) {
		return fmt.Errorf("activation tensor exceeds %d bytes", maxPipelineTensorBytes)
	}
	expected := int(elements) * elementSize
	if len(tensor.Data) != expected {
		return fmt.Errorf("activation payload has %d bytes; shape and dtype require %d", len(tensor.Data), expected)
	}
	return nil
}

func tensorElementSize(dtype TensorDType) (int, bool) {
	switch dtype {
	case TensorFloat16, TensorBFloat16:
		return 2, true
	case TensorFloat32:
		return 4, true
	case TensorInt64:
		return 8, true
	case TensorUint8:
		return 1, true
	default:
		return 0, false
	}
}

// PipelineStageRequest identifies one tokenization, prefill, or decode step.
// Stage-local KV state is keyed by RequestID and never transferred between
// nodes; only the activation tensor crosses a stage boundary.
type PipelineStageRequest struct {
	ProtocolVersion int               `json:"protocol_version"`
	RequestID       string            `json:"request_id"`
	Assignment      StageAssignment   `json:"assignment"`
	Prompt          string            `json:"prompt,omitempty"`
	Activation      *ActivationTensor `json:"activation,omitempty"`
	InputTokenID    *int64            `json:"input_token_id,omitempty"`
	Position        int64             `json:"position,omitempty"`
	Temperature     float64           `json:"temperature,omitempty"`
	MaxTokens       int               `json:"max_tokens,omitempty"`
}

func (request PipelineStageRequest) Validate(isPrefill bool) error {
	if request.ProtocolVersion != PipelineStageProtocolVersion {
		return fmt.Errorf("unsupported pipeline stage protocol version %d", request.ProtocolVersion)
	}
	if request.RequestID == "" {
		return errors.New("request_id must not be empty")
	}
	if err := request.Assignment.Validate(); err != nil {
		return err
	}
	if request.Position < 0 {
		return errors.New("position must not be negative")
	}
	if request.MaxTokens < 0 {
		return errors.New("max_tokens must not be negative")
	}
	if request.Activation != nil {
		if err := request.Activation.Validate(); err != nil {
			return err
		}
	}
	if isPrefill {
		if request.Assignment.StartLayer == 0 {
			if request.Prompt == "" || request.Activation != nil {
				return errors.New("first stage prefill requires a prompt and no activation")
			}
		} else if request.Prompt != "" || request.Activation == nil {
			return errors.New("non-first stage prefill requires an activation and no prompt")
		}
		return nil
	}
	if request.Assignment.StartLayer == 0 {
		if request.InputTokenID == nil || request.Activation != nil {
			return errors.New("first stage decode requires an input token and no activation")
		}
	} else if request.InputTokenID != nil || request.Activation == nil {
		return errors.New("non-first stage decode requires an activation and no input token")
	}
	return nil
}

type PipelineStageControlRequest struct {
	ProtocolVersion int             `json:"protocol_version"`
	RequestID       string          `json:"request_id"`
	Assignment      StageAssignment `json:"assignment"`
}

func (request PipelineStageControlRequest) Validate() error {
	if request.ProtocolVersion != PipelineStageProtocolVersion {
		return fmt.Errorf("unsupported pipeline stage protocol version %d", request.ProtocolVersion)
	}
	if request.RequestID == "" {
		return errors.New("request_id must not be empty")
	}
	return request.Assignment.Validate()
}

type PipelineStageOutput struct {
	Activation   *ActivationTensor `json:"activation,omitempty"`
	Token        *PipelineToken    `json:"token,omitempty"`
	PromptTokens int               `json:"prompt_tokens,omitempty"`
}

func (output PipelineStageOutput) Validate(isFinal bool) error {
	if isFinal {
		if output.Token == nil || output.Activation != nil {
			return errors.New("final stage must return a token and no activation")
		}
		if output.PromptTokens < 0 {
			return errors.New("prompt_tokens must not be negative")
		}
		return nil
	}
	if output.Activation == nil || output.Token != nil || output.PromptTokens != 0 {
		return errors.New("non-final stage must return only an activation")
	}
	return output.Activation.Validate()
}

type PipelineToken struct {
	ID           int64  `json:"id"`
	Text         string `json:"text"`
	Done         bool   `json:"done,omitempty"`
	FinishReason string `json:"finish_reason,omitempty"`
}

// PipelineStageEngineV2 keeps request-scoped KV state on the node that owns
// the stage while transferring typed activations between stages.
type PipelineStageEngineV2 interface {
	OpenPipelineRequest(context.Context, StageAssignment, string) error
	PrefillPipelineStage(context.Context, StageAssignment, PipelineStageRequest) (PipelineStageOutput, error)
	DecodePipelineStage(context.Context, StageAssignment, PipelineStageRequest) (PipelineStageOutput, error)
	ClosePipelineRequest(context.Context, StageAssignment, string) error
}

func (engine *HTTPPipelineStageEngine) OpenPipelineRequest(
	ctx context.Context,
	assignment StageAssignment,
	requestID string,
) error {
	return engine.pipelineStageControlV2(ctx, StageOpenPath, PipelineStageControlRequest{
		ProtocolVersion: PipelineStageProtocolVersion, RequestID: requestID,
		Assignment: engine.assignmentForServer(assignment),
	})
}

func (engine *HTTPPipelineStageEngine) ClosePipelineRequest(
	ctx context.Context,
	assignment StageAssignment,
	requestID string,
) error {
	return engine.pipelineStageControlV2(ctx, StageClosePath, PipelineStageControlRequest{
		ProtocolVersion: PipelineStageProtocolVersion, RequestID: requestID,
		Assignment: engine.assignmentForServer(assignment),
	})
}

func (engine *HTTPPipelineStageEngine) pipelineStageControlV2(
	ctx context.Context,
	path string,
	payload PipelineStageControlRequest,
) error {
	if err := payload.Validate(); err != nil {
		return err
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode stage request control: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, engine.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create stage request control: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := engine.client.Do(request)
	if err != nil {
		return fmt.Errorf("send stage request control: %w", err)
	}
	defer closeutil.Discard(response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, readErr := io.ReadAll(io.LimitReader(response.Body, stageErrorBodyLimit))
		if readErr != nil {
			return fmt.Errorf("stage request control returned %s and reading its error failed: %w", response.Status, readErr)
		}
		return fmt.Errorf("stage request control returned %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	return nil
}

func (engine *HTTPPipelineStageEngine) PrefillPipelineStage(
	ctx context.Context,
	assignment StageAssignment,
	request PipelineStageRequest,
) (PipelineStageOutput, error) {
	return engine.pipelineStageStepV2(ctx, StagePrefillPath, assignment, request, true)
}

func (engine *HTTPPipelineStageEngine) DecodePipelineStage(
	ctx context.Context,
	assignment StageAssignment,
	request PipelineStageRequest,
) (PipelineStageOutput, error) {
	return engine.pipelineStageStepV2(ctx, StageDecodePath, assignment, request, false)
}

func (engine *HTTPPipelineStageEngine) pipelineStageStepV2(
	ctx context.Context,
	path string,
	assignment StageAssignment,
	stageRequest PipelineStageRequest,
	prefill bool,
) (PipelineStageOutput, error) {
	stageRequest.Assignment = engine.assignmentForServer(assignment)
	if err := stageRequest.Validate(prefill); err != nil {
		return PipelineStageOutput{}, err
	}
	reader, writer := io.Pipe()
	defer closeutil.Discard(reader)
	writerDone := make(chan error, 1)
	go func() {
		err := WriteStageMessage(writer, StageMessageHeader{Request: stageRequest}, stageRequest.Activation)
		if err != nil {
			_ = writer.CloseWithError(err)
		} else {
			err = writer.Close()
		}
		writerDone <- err
	}()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, engine.baseURL+path, reader)
	if err != nil {
		_ = reader.CloseWithError(err)
		return PipelineStageOutput{}, fmt.Errorf("create stage step request: %w", err)
	}
	request.Header.Set("Content-Type", PipelineStageV2ContentType)
	request.Header.Set("Accept", PipelineStageV2ContentType)
	response, err := engine.client.Do(request)
	if err != nil {
		_ = reader.CloseWithError(err)
		return PipelineStageOutput{}, fmt.Errorf("send stage step request: %w", err)
	}
	defer closeutil.Discard(response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_ = reader.CloseWithError(errors.New("stage endpoint rejected request"))
		body, readErr := io.ReadAll(io.LimitReader(response.Body, stageErrorBodyLimit))
		if readErr != nil {
			return PipelineStageOutput{}, fmt.Errorf("stage step returned %s and reading its error failed: %w", response.Status, readErr)
		}
		return PipelineStageOutput{}, fmt.Errorf("stage step returned %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != PipelineStageV2ContentType {
		return PipelineStageOutput{}, fmt.Errorf("stage step returned unsupported content type %q", response.Header.Get("Content-Type"))
	}
	message, err := ReadStageMessage(io.LimitReader(response.Body, maxPipelineTensorBytes+maxPipelineStageHeaderBytes+4))
	if err != nil {
		return PipelineStageOutput{}, fmt.Errorf("decode stage step response: %w", err)
	}
	if err := <-writerDone; err != nil {
		return PipelineStageOutput{}, fmt.Errorf("write stage step request: %w", err)
	}
	responseAssignment := message.Request.Assignment
	responseAssignment.Normalize()
	expectedAssignment := stageRequest.Assignment
	expectedAssignment.Normalize()
	if message.Request.RequestID != stageRequest.RequestID || responseAssignment != expectedAssignment ||
		message.Output == nil {
		return PipelineStageOutput{}, errors.New("stage step response does not match its request")
	}
	if err := message.Output.Validate(assignment.IsFinal); err != nil {
		return PipelineStageOutput{}, fmt.Errorf("invalid stage step output: %w", err)
	}
	return *message.Output, nil
}

// StageMessageHeader is the JSON metadata portion of the length-prefixed
// binary stage protocol. The tensor payload immediately follows this header.
type StageMessageHeader struct {
	Request PipelineStageRequest `json:"request"`
	Output  *PipelineStageOutput `json:"output,omitempty"`
}

func WriteStageMessage(writer io.Writer, header StageMessageHeader, tensor *ActivationTensor) error {
	headerTensor := header.Request.Activation
	if header.Output != nil && header.Output.Activation != nil {
		if headerTensor != nil {
			return errors.New("stage message cannot contain request and response tensors")
		}
		headerTensor = header.Output.Activation
	}
	if (headerTensor == nil) != (tensor == nil) {
		return errors.New("stage message tensor metadata and payload must be provided together")
	}
	if tensor != nil {
		if err := tensor.Validate(); err != nil {
			return err
		}
		if tensor.DType != headerTensor.DType || !sameTensorShape(tensor.Shape, headerTensor.Shape) {
			return errors.New("stage message tensor payload does not match its metadata")
		}
		if header.Request.Activation != nil {
			copy := *header.Request.Activation
			copy.Data = nil
			header.Request.Activation = &copy
		}
		if header.Output != nil && header.Output.Activation != nil {
			copy := *header.Output.Activation
			copy.Data = nil
			output := *header.Output
			output.Activation = &copy
			header.Output = &output
		}
	}
	payload, err := json.Marshal(header)
	if err != nil {
		return fmt.Errorf("encode stage message header: %w", err)
	}
	if len(payload) > maxPipelineStageHeaderBytes {
		return fmt.Errorf("stage message header exceeds %d bytes", maxPipelineStageHeaderBytes)
	}
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(payload)))
	if _, err := writeStageBytes(writer, length[:]); err != nil {
		return fmt.Errorf("write stage message length: %w", err)
	}
	if _, err := writeStageBytes(writer, payload); err != nil {
		return fmt.Errorf("write stage message header: %w", err)
	}
	if tensor != nil {
		if _, err := writeStageBytes(writer, tensor.Data); err != nil {
			return fmt.Errorf("write activation payload: %w", err)
		}
	}
	return nil
}

func writeStageBytes(writer io.Writer, data []byte) (int, error) {
	written := 0
	for written < len(data) {
		n, err := writer.Write(data[written:])
		written += n
		if err != nil {
			return written, err
		}
		if n == 0 {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func sameTensorShape(left, right []int64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func ReadStageMessage(reader io.Reader) (StageMessageHeader, error) {
	var length [4]byte
	if _, err := io.ReadFull(reader, length[:]); err != nil {
		return StageMessageHeader{}, fmt.Errorf("read stage message length: %w", err)
	}
	headerLength := binary.BigEndian.Uint32(length[:])
	if headerLength == 0 || headerLength > maxPipelineStageHeaderBytes {
		return StageMessageHeader{}, fmt.Errorf("invalid stage message header length %d", headerLength)
	}
	encoded := make([]byte, int(headerLength))
	if _, err := io.ReadFull(reader, encoded); err != nil {
		return StageMessageHeader{}, fmt.Errorf("read stage message header: %w", err)
	}
	var header StageMessageHeader
	if err := json.Unmarshal(encoded, &header); err != nil {
		return StageMessageHeader{}, fmt.Errorf("decode stage message header: %w", err)
	}
	tensor := header.Request.Activation
	if header.Output != nil && header.Output.Activation != nil {
		if tensor != nil {
			return StageMessageHeader{}, errors.New("stage message cannot contain request and response tensors")
		}
		tensor = header.Output.Activation
	}
	if tensor != nil {
		byteLength, err := tensorPayloadLength(*tensor)
		if err != nil {
			return StageMessageHeader{}, err
		}
		if byteLength > maxPipelineTensorBytes {
			return StageMessageHeader{}, fmt.Errorf("activation tensor exceeds %d bytes", maxPipelineTensorBytes)
		}
		tensor.Data = make([]byte, byteLength)
		if _, err := io.ReadFull(reader, tensor.Data); err != nil {
			return StageMessageHeader{}, fmt.Errorf("read activation payload: %w", err)
		}
		if err := tensor.Validate(); err != nil {
			return StageMessageHeader{}, err
		}
	}
	var extra [1]byte
	n, err := io.ReadFull(reader, extra[:])
	if n != 0 {
		return StageMessageHeader{}, errors.New("stage message contains trailing data")
	}
	if err != io.EOF {
		return StageMessageHeader{}, fmt.Errorf("check trailing stage message data: %w", err)
	}
	return header, nil
}

func tensorPayloadLength(tensor ActivationTensor) (int, error) {
	elementSize, ok := tensorElementSize(tensor.DType)
	if !ok {
		return 0, fmt.Errorf("unsupported activation dtype %q", tensor.DType)
	}
	if len(tensor.Shape) == 0 || len(tensor.Shape) > maxPipelineTensorRank {
		return 0, fmt.Errorf("activation rank must be between 1 and %d", maxPipelineTensorRank)
	}
	elements := int64(1)
	for _, dimension := range tensor.Shape {
		if dimension <= 0 {
			return 0, errors.New("activation dimensions must be positive")
		}
		if elements > math.MaxInt64/dimension {
			return 0, errors.New("activation shape overflows")
		}
		elements *= dimension
	}
	bytes := elements * int64(elementSize)
	if bytes > maxPipelineTensorBytes {
		return 0, fmt.Errorf("activation tensor exceeds %d bytes", maxPipelineTensorBytes)
	}
	return int(bytes), nil
}
