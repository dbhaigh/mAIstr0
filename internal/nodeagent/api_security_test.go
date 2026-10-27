package nodeagent

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/discovery"
)

func TestSecurityStatusReportsOnlyDiscoveryAuthenticationMode(t *testing.T) {
	secret := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", 32)))
	agent := mustNewTestAgent(t, config.NodeConfig{DiscoveryHMACSecret: secret})
	response := httptest.NewRecorder()
	agent.handleAPISecurityStatus(response, httptest.NewRequest(http.MethodGet, "/api/security/status", nil))

	var status discovery.SecurityStatus
	if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || status.DiscoveryAuthentication != "hmac-sha256" {
		t.Fatalf("security status = %d, %+v", response.Code, status)
	}
	if strings.Contains(response.Body.String(), secret) {
		t.Fatal("security status exposed the configured discovery secret")
	}
}
