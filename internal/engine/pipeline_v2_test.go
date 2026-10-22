package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestStageMessageRoundTripsTypedActivation(t *testing.T) {
	assignment := StageAssignment{
		ModelID: "llama-8b", StartLayer: 8, EndLayer: 16, Engine: "llamacpp",
	}
	tensor := &ActivationTensor{
		DType: TensorFloat16, Shape: []int64{2, 2}, Data: []byte{1, 2, 3, 4, 5, 6, 7, 8},
	}
	header := StageMessageHeader{
		Request: PipelineStageRequest{
			ProtocolVersion: PipelineStageProtocolVersion, RequestID: "request-1",
			Assignment: assignment, Activation: tensor, Position: 4,
		},
	}
	var encoded bytes.Buffer
	if err := WriteStageMessage(&encoded, header, tensor); err != nil {
		t.Fatal(err)
	}
	decoded, err := ReadStageMessage(&encoded)
	if err != nil {
		t.Fatal(err)
	}
	got := decoded.Request.Activation
	if got == nil || got.DType != TensorFloat16 || !sameTensorShape(got.Shape, tensor.Shape) ||
		!bytes.Equal(got.Data, tensor.Data) || decoded.Request.RequestID != "request-1" {
		t.Fatalf("decoded stage message = %+v", decoded)
	}
}

func TestStageMessageRejectsMalformedTensorAndTrailingBytes(t *testing.T) {
	tests := []struct {
		name   string
		header StageMessageHeader
		tensor *ActivationTensor
		want   string
	}{
		{
			name: "payload mismatch",
			header: StageMessageHeader{Request: PipelineStageRequest{
				Activation: &ActivationTensor{DType: TensorFloat32, Shape: []int64{2}},
			}},
			tensor: &ActivationTensor{DType: TensorFloat32, Shape: []int64{2}, Data: []byte{1, 2}},
			want:   "activation payload",
		},
		{
			name: "metadata without payload",
			header: StageMessageHeader{Request: PipelineStageRequest{
				Activation: &ActivationTensor{DType: TensorUint8, Shape: []int64{1}},
			}},
			want: "provided together",
		},
		{
			name: "unsupported dtype",
			header: StageMessageHeader{Request: PipelineStageRequest{
				Activation: &ActivationTensor{DType: "float64", Shape: []int64{1}},
			}},
			tensor: &ActivationTensor{DType: "float64", Shape: []int64{1}, Data: []byte{1}},
			want:   "unsupported activation dtype",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var encoded bytes.Buffer
			if err := WriteStageMessage(&encoded, test.header, test.tensor); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("WriteStageMessage error = %v, want containing %q", err, test.want)
			}
		})
	}

	var encoded bytes.Buffer
	header := []byte(`{"request":{"protocol_version":2,"request_id":"r","assignment":{"model_id":"m","start_layer":0,"end_layer":1},"activation":{"dtype":"uint8","shape":[1]}}}`)
	var headerLength [4]byte
	binary.BigEndian.PutUint32(headerLength[:], uint32(len(header)))
	encoded.Write(headerLength[:])
	encoded.Write(header)
	encoded.Write([]byte{0x01, 0x02})
	if _, err := ReadStageMessage(&encoded); err == nil || !strings.Contains(err.Error(), "trailing data") {
		t.Fatalf("ReadStageMessage trailing-data error = %v", err)
	}
}

func TestPipelineStageRequestsValidatePrefillAndDecodeInputs(t *testing.T) {
	assignment := StageAssignment{ModelID: "model", StartLayer: 0, EndLayer: 4}
	prompt := PipelineStageRequest{
		ProtocolVersion: PipelineStageProtocolVersion, RequestID: "request",
		Assignment: assignment, Prompt: "prompt",
	}
	if err := prompt.Validate(true); err != nil {
		t.Fatalf("valid prefill request rejected: %v", err)
	}
	tokenID := int64(42)
	decode := PipelineStageRequest{
		ProtocolVersion: PipelineStageProtocolVersion, RequestID: "request",
		Assignment: assignment, InputTokenID: &tokenID, Position: 7,
	}
	if err := decode.Validate(false); err != nil {
		t.Fatalf("valid decode request rejected: %v", err)
	}

	invalid := prompt
	invalid.Activation = &ActivationTensor{DType: TensorUint8, Shape: []int64{1}, Data: []byte{1}}
	if err := invalid.Validate(true); err == nil {
		t.Fatal("first stage prefill accepted both prompt and activation")
	}
	invalid = decode
	invalid.Position = -1
	if err := invalid.Validate(false); err == nil {
		t.Fatal("decode accepted a negative position")
	}
}

func TestHTTPPipelineStageEngineProbesRuntimeCapabilities(t *testing.T) {
	capabilities := PipelineStageCapabilities{
		ProtocolVersion: PipelineStageProtocolVersion,
		Runtime:         "llamacpp",
		Models:          []PipelineStageModelCapability{{ModelID: "llama-8b", MaxLayers: 32}},
		TensorDTypes:    []TensorDType{TensorFloat16, TensorFloat32},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != StageCapabilitiesPath {
			t.Errorf("capability probe request = %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if err := json.NewEncoder(w).Encode(capabilities); err != nil {
			t.Errorf("encode capabilities: %v", err)
		}
	}))
	defer server.Close()

	stageEngine, err := NewHTTPPipelineStageEngine("llamacpp", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	got, err := stageEngine.ProbePipelineStageCapabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || !got.SupportsModel("llama-8b", 32) || got.SupportsModel("llama-8b", 33) {
		t.Fatalf("probed capabilities = %+v", got)
	}

	legacyServer := httptest.NewServer(http.NotFoundHandler())
	defer legacyServer.Close()
	legacyEngine, err := NewHTTPPipelineStageEngine("llamacpp", legacyServer.URL, legacyServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	if got, err := legacyEngine.ProbePipelineStageCapabilities(context.Background()); err != nil || got != nil {
		t.Fatalf("legacy server capability probe = %+v, %v; want nil, nil", got, err)
	}

	mismatchServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		capabilities.Runtime = "vllm"
		_ = json.NewEncoder(w).Encode(capabilities)
	}))
	defer mismatchServer.Close()
	mismatchEngine, err := NewHTTPPipelineStageEngine("llamacpp", mismatchServer.URL, mismatchServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mismatchEngine.ProbePipelineStageCapabilities(context.Background()); err == nil {
		t.Fatal("capability probe accepted a different runtime identity")
	}
}

func TestSimulatedStagesRunTokenByTokenAndKeepRequestStateLocal(t *testing.T) {
	stages := []*Simulated{NewSimulated(), NewSimulated()}
	assignments := []StageAssignment{
		{ModelID: "simulated-general", StartLayer: 0, EndLayer: 4},
		{ModelID: "simulated-general", StartLayer: 4, EndLayer: 8, IsFinal: true},
	}
	for i, stage := range stages {
		if err := stage.LoadStage(context.Background(), assignments[i]); err != nil {
			t.Fatal(err)
		}
		if err := stage.OpenPipelineRequest(context.Background(), assignments[i], "request"); err != nil {
			t.Fatal(err)
		}
	}

	first, err := stages[0].PrefillPipelineStage(context.Background(), assignments[0], PipelineStageRequest{
		ProtocolVersion: PipelineStageProtocolVersion, RequestID: "request",
		Assignment: assignments[0], Prompt: "hello pipeline",
	})
	if err != nil {
		t.Fatal(err)
	}
	last, err := stages[1].PrefillPipelineStage(context.Background(), assignments[1], PipelineStageRequest{
		ProtocolVersion: PipelineStageProtocolVersion, RequestID: "request",
		Assignment: assignments[1], Activation: first.Activation,
	})
	if err != nil {
		t.Fatal(err)
	}
	if last.Token == nil || last.PromptTokens != len("hello pipeline") {
		t.Fatalf("prefill result = %+v", last)
	}
	var output strings.Builder
	output.WriteString(last.Token.Text)
	token := last.Token
	position := int64(last.PromptTokens)
	for !token.Done {
		tokenID := token.ID
		activation, err := stages[0].DecodePipelineStage(context.Background(), assignments[0], PipelineStageRequest{
			ProtocolVersion: PipelineStageProtocolVersion, RequestID: "request",
			Assignment: assignments[0], InputTokenID: &tokenID, Position: position,
		})
		if err != nil {
			t.Fatal(err)
		}
		next, err := stages[1].DecodePipelineStage(context.Background(), assignments[1], PipelineStageRequest{
			ProtocolVersion: PipelineStageProtocolVersion, RequestID: "request",
			Assignment: assignments[1], Activation: activation.Activation, Position: position,
		})
		if err != nil {
			t.Fatal(err)
		}
		token = next.Token
		output.WriteString(token.Text)
		position++
	}
	if !strings.Contains(output.String(), "simulated response from simulated-general") {
		t.Fatalf("generated output = %q", output.String())
	}
	for i, stage := range stages {
		if err := stage.ClosePipelineRequest(context.Background(), assignments[i], "request"); err != nil {
			t.Fatal(err)
		}
	}
}
