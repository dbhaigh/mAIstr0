package nodeagent

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
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
