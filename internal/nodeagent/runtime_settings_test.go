package nodeagent

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/config"
)

func TestClusterPairingPolicyPersistsAcrossRuntimeSettingsPatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.json")
	allowPeerUpdates := true
	agent := &Agent{orchestratorLink: &orchestratorLink{}}
	agent.SetRuntimeSettings(path, config.RuntimeSettings{AllowPeerUpdates: &allowPeerUpdates})

	if err := agent.SetPairingVerificationRequired(true); err != nil {
		t.Fatal(err)
	}
	if !agent.PairingVerificationRequired() || !agent.orchestratorLink.pairingVerificationRequired() {
		t.Fatal("pairing policy was not applied to the node agent")
	}

	request := httptest.NewRequest(http.MethodPatch, "/api/runtime-settings",
		strings.NewReader(`{"role":"node","allow_peer_updates":true}`))
	response := httptest.NewRecorder()
	agent.handleRuntimeSettings(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("runtime settings patch returned %d: %s", response.Code, response.Body.String())
	}
	saved, err := config.LoadRuntimeSettings(path)
	if err != nil {
		t.Fatal(err)
	}
	if saved.RequirePairingVerification == nil || !*saved.RequirePairingVerification ||
		saved.AllowPeerUpdates == nil || !*saved.AllowPeerUpdates {
		t.Fatalf("runtime settings patch lost persisted policy or unrelated setting: %+v", saved)
	}
}
