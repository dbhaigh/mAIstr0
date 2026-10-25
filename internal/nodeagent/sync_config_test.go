package nodeagent

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSyncDecodersEnforceConfiguredRequestLimit(t *testing.T) {
	taskRequest := httptest.NewRequest(http.MethodPost, "/tasks/replica", strings.NewReader(`[{"id":"oversized"}]`))
	if _, err := decodeTaskSync(httptest.NewRecorder(), taskRequest, 4); err == nil {
		t.Fatal("task sync accepted a body above the configured limit")
	}

	memoryRequest := httptest.NewRequest(http.MethodPost, "/memory/sync", strings.NewReader(`{"experiences":[]}`))
	if _, _, err := decodeMemorySyncRequest(httptest.NewRecorder(), memoryRequest, 4); err == nil {
		t.Fatal("memory sync accepted a body above the configured limit")
	}
}
