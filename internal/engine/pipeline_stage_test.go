package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestSimulatedPipelineStageLifecycleAndForward(t *testing.T) {
	simulated := NewSimulated()
	assignment := StageAssignment{ModelID: "model", StartLayer: 0, EndLayer: 4}
	if err := simulated.LoadStage(context.Background(), assignment); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	updatedCache, err := simulated.ForwardStage(
		context.Background(), assignment, bytes.NewBufferString("activation"), []byte("old-cache"),
		func(chunk []byte) error {
			_, writeErr := output.Write(chunk)
			return writeErr
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if output.String() != "activation" {
		t.Fatalf("forwarded activation = %q, want activation", output.String())
	}
	if string(updatedCache) != "old-cacheactivation" {
		t.Fatalf("updated KV cache = %q", updatedCache)
	}
	if err := simulated.UnloadStage(context.Background(), assignment); err != nil {
		t.Fatal(err)
	}
	if _, err := simulated.ForwardStage(
		context.Background(), assignment, bytes.NewReader(nil), nil, func([]byte) error { return nil },
	); err == nil {
		t.Fatal("forward succeeded after stage unload")
	}
}

func TestSimulatedPipelineStagePromptDrainsInputFrames(t *testing.T) {
	simulated := NewSimulated()
	assignment := StageAssignment{ModelID: "model", StartLayer: 0, EndLayer: 4}
	if err := simulated.LoadStage(context.Background(), assignment); err != nil {
		t.Fatal(err)
	}

	decoder := json.NewDecoder(strings.NewReader("{\"type\":\"end\"}\n"))
	var output bytes.Buffer
	_, err := simulated.ForwardStageRequest(
		context.Background(), assignment,
		StageForwardRequest{Prompt: "prompt", Activations: NewStageActivationStream(decoder)},
		func(frame StageForwardFrame) error {
			_, writeErr := output.Write(frame.Data)
			return writeErr
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if output.String() != "prompt" {
		t.Fatalf("prompt activation = %q, want prompt", output.String())
	}
	if err := decoder.Decode(new(StageForwardFrame)); err != io.EOF {
		t.Fatalf("activation stream was not fully consumed, next decode error = %v", err)
	}
}

func TestHTTPPipelineStageEngineStreamsAndDelegates(t *testing.T) {
	var loads, forwards, unloads atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("POST /stage/load", func(w http.ResponseWriter, r *http.Request) {
		var assignment StageAssignment
		if err := json.NewDecoder(r.Body).Decode(&assignment); err != nil {
			t.Errorf("decode load assignment: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if assignment.Engine != "vllm" {
			t.Errorf("load engine = %q, want vllm", assignment.Engine)
		}
		loads.Add(1)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /stage/unload", func(w http.ResponseWriter, r *http.Request) {
		var assignment StageAssignment
		if err := json.NewDecoder(r.Body).Decode(&assignment); err != nil {
			t.Errorf("decode unload assignment: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if assignment.Engine != "vllm" {
			t.Errorf("unload engine = %q, want vllm", assignment.Engine)
		}
		unloads.Add(1)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /stage/forward", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/x-ndjson" {
			t.Errorf("forward content type = %q", r.Header.Get("Content-Type"))
		}
		decoder := json.NewDecoder(r.Body)
		start, err := DecodeStageForwardStart(decoder)
		if err != nil {
			t.Errorf("decode forward start: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if start.Assignment.Engine != "vllm" {
			t.Errorf("forward engine = %q, want vllm", start.Assignment.Engine)
		}
		w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
		encoder := json.NewEncoder(w)
		activation := NewStageActivationStream(decoder)
		if err := encoder.Encode(StageForwardFrame{Type: StageFrameActivation, Data: []byte("forwarded:")}); err != nil {
			t.Errorf("write activation prefix: %v", err)
			return
		}
		var data bytes.Buffer
		if _, err := io.Copy(&data, activation); err != nil {
			t.Errorf("read activation stream: %v", err)
			return
		}
		if err := encoder.Encode(StageForwardFrame{Type: StageFrameActivation, Data: data.Bytes()}); err != nil {
			t.Errorf("write activation: %v", err)
			return
		}
		if err := encoder.Encode(StageForwardFrame{
			Type: StageFrameComplete, KVCache: append(start.KVCache, data.Bytes()...),
		}); err != nil {
			t.Errorf("write complete frame: %v", err)
			return
		}
		forwards.Add(1)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	stageEngine, err := NewHTTPPipelineStageEngine("vllm", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	assignment := StageAssignment{ModelID: "model", StartLayer: 1, EndLayer: 3}
	if err := stageEngine.LoadStage(context.Background(), assignment); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	cache, err := stageEngine.ForwardStage(
		context.Background(), assignment, bytes.NewBufferString("activation"), []byte("cache"),
		func(chunk []byte) error {
			_, writeErr := output.Write(chunk)
			return writeErr
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if output.String() != "forwarded:activation" || string(cache) != "cacheactivation" {
		t.Fatalf("forward result = %q, cache = %q", output.String(), cache)
	}
	if err := stageEngine.UnloadStage(context.Background(), assignment); err != nil {
		t.Fatal(err)
	}
	if loads.Load() != 1 || forwards.Load() != 1 || unloads.Load() != 1 {
		t.Fatalf("local stage endpoint calls = load:%d forward:%d unload:%d", loads.Load(), forwards.Load(), unloads.Load())
	}
}

func TestStageAssignmentRejectsInvalidRangeAndNextAddress(t *testing.T) {
	tests := []struct {
		name       string
		assignment StageAssignment
	}{
		{
			name: "negative start",
			assignment: StageAssignment{
				ModelID: "model", StartLayer: -1, EndLayer: 2,
			},
		},
		{
			name: "empty range",
			assignment: StageAssignment{
				ModelID: "model", StartLayer: 2, EndLayer: 2,
			},
		},
		{
			name: "insecure next stage",
			assignment: StageAssignment{
				ModelID: "model", StartLayer: 0, EndLayer: 2,
				NextStageAddr: "http://worker:7451",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.assignment.Validate(); err == nil {
				t.Fatal("invalid stage assignment was accepted")
			}
		})
	}
}

func TestStageActivationStreamRequiresEndFrame(t *testing.T) {
	decoder := json.NewDecoder(bytes.NewBufferString("{\"type\":\"activation\",\"data\":\"YQ==\"}\n"))
	stream := NewStageActivationStream(decoder)
	data, err := io.ReadAll(stream)
	if err == nil || !strings.Contains(err.Error(), "without an end frame") {
		t.Fatalf("reading unterminated activation stream returned %q, %v", data, err)
	}
}
