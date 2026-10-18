package nodeagent

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
)

func TestHandleGeneratePreservesJSONResponse(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID:     "generation-handler-test",
		MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("close test agent: %v", err)
		}
	})
	agent.engines = []engine.Engine{engine.NewSimulated()}

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/generate", strings.NewReader(
		`{"model":"simulated-general","prompt":"hello"}`,
	))
	agent.handleGenerate(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("POST /generate returned %d: %s", response.Code, response.Body.String())
	}

	var body map[string]json.RawMessage
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode generation response: %v", err)
	}
	for _, field := range []string{"model", "output", "duration_ms"} {
		if _, ok := body[field]; !ok {
			t.Errorf("generation response omitted %q: %s", field, response.Body.String())
		}
	}

	if len(body) != 3 {
		t.Errorf("generation response fields = %v, want model, output, duration_ms", body)
	}
	var output string
	if err := json.Unmarshal(body["output"], &output); err != nil || output == "" {
		t.Errorf("generation output = %q (decode error %v), want non-empty output", output, err)
	}
}

type countingGenerateEngine struct {
	engine.Engine
	calls atomic.Int32
}

func (e *countingGenerateEngine) Generate(string, string) (string, error) {
	return fmt.Sprintf("generated-%d", e.calls.Add(1)), nil
}

type loseFirstGenerateResponse struct {
	base    http.RoundTripper
	dropped atomic.Bool
}

func (t *loseFirstGenerateResponse) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(request)
	if err != nil {
		return response, err
	}
	if t.dropped.CompareAndSwap(false, true) {
		_, copyErr := io.Copy(io.Discard, response.Body)
		closeErr := response.Body.Close()
		if copyErr != nil {
			return nil, copyErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		return nil, errors.New("simulated dropped response after worker completion")
	}
	return response, nil
}

func TestGenerateIdempotencyKeyReplaysCompletedWorkerResult(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID:     "generation-idempotency-test",
		MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("close test agent: %v", err)
		}
	})
	eng := &countingGenerateEngine{Engine: engine.NewSimulated()}
	agent.engines = []engine.Engine{eng}
	server := httptest.NewServer(http.HandlerFunc(agent.handleGenerate))
	defer server.Close()

	client := &http.Client{Transport: &loseFirstGenerateResponse{base: http.DefaultTransport}}
	generate := func(prompt string) *http.Request {
		t.Helper()
		body := `{"model":"simulated-general","prompt":"` + prompt + `"}`
		request, err := http.NewRequest(http.MethodPost, server.URL+"/generate", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", "same-logical-request")
		return request
	}

	if _, err := client.Do(generate("hello")); err == nil {
		t.Fatal("first response was not dropped by the test transport")
	}
	response, err := client.Do(generate("hello"))
	if err != nil {
		t.Fatalf("retry after dropped response failed: %v", err)
	}
	defer response.Body.Close()
	var result generateResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || result.Output != "generated-1" || eng.calls.Load() != 1 {
		t.Fatalf("retry response = status %d, body %+v, engine calls %d", response.StatusCode, result, eng.calls.Load())
	}

	conflict, err := client.Do(generate("different prompt"))
	if err != nil {
		t.Fatal(err)
	}
	defer conflict.Body.Close()
	if conflict.StatusCode != http.StatusConflict || eng.calls.Load() != 1 {
		t.Fatalf("different request with reused key = status %d, engine calls %d", conflict.StatusCode, eng.calls.Load())
	}

	streaming := generate("streaming")
	streaming.URL.RawQuery = "stream=true"
	streamResponse, err := client.Do(streaming)
	if err != nil {
		t.Fatal(err)
	}
	defer streamResponse.Body.Close()
	if streamResponse.StatusCode != http.StatusBadRequest || eng.calls.Load() != 1 {
		t.Fatalf("streaming request with key = status %d, engine calls %d", streamResponse.StatusCode, eng.calls.Load())
	}
}
