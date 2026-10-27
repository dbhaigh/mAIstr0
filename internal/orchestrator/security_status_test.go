package orchestrator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/discovery"
)

func TestSecurityStatusUsesConfiguredDiscoveryModeWithoutExposingKey(t *testing.T) {
	srv := newTestServer(t)
	secret := "test-only-cluster-secret-32-bytes"
	srv.election.setDiscoveryHMACKey([]byte(secret))
	response := httptest.NewRecorder()
	srv.handleSecurityStatus(response, httptest.NewRequest(http.MethodGet, "/api/security/status", nil))

	var status discovery.SecurityStatus
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || status.DiscoveryAuthentication != "hmac-sha256" {
		t.Fatalf("security status = %d, %+v", response.Code, status)
	}
	if response.Body.String() == "" || len(response.Body.String()) > 128 || strings.Contains(response.Body.String(), secret) {
		t.Fatalf("unexpected security status body: %s", response.Body.String())
	}
}
