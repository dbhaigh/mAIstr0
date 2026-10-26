package nodeagent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

func securityFingerprint(t *testing.T, identity *peeridentity.Identity) string {
	t.Helper()
	fingerprint, err := peeridentity.CertificateFingerprint(identity.CertificatePEM())
	if err != nil {
		t.Fatal(err)
	}
	return fingerprint
}

type securityURLEngine struct {
	engine.Engine
	endpoint     string
	inventoryErr error
}

func (*securityURLEngine) Name() string  { return "ollama" }
func (e *securityURLEngine) URL() string { return e.endpoint }
func (e *securityURLEngine) ListModels() ([]engine.Model, error) {
	if e.inventoryErr != nil {
		return nil, e.inventoryErr
	}
	return e.Engine.ListModels()
}

func TestEngineAPIsRedactCredentialsAndPreserveRoundTrip(t *testing.T) {
	a := newTestAgent(t, "secret-boundary")
	probe := &securityURLEngine{Engine: engine.NewSimulated(), endpoint: "https://private-user:private-password@engine.invalid/v1?api_key=private-query#private-fragment"}
	a.engineComponent.replaceConfiguredEngine(probe)
	for _, handler := range []http.HandlerFunc{a.handleProperties, a.handleEngineInfo} {
		rec := httptest.NewRecorder()
		handler(rec, httptest.NewRequest(http.MethodGet, "/properties", nil))
		for _, secret := range []string{"private-user", "private-password", "private-query", "private-fragment"} {
			if strings.Contains(rec.Body.String(), secret) {
				t.Fatalf("public API exposed %s: %s", secret, rec.Body.String())
			}
		}
		if !strings.Contains(rec.Body.String(), "https://engine.invalid/v1") {
			t.Fatalf("public endpoint lost useful routing information: %s", rec.Body.String())
		}
	}
	props := a.propertiesSnapshot()
	body, err := json.Marshal(props)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	a.handleProperties(rec, httptest.NewRequest(http.MethodPatch, "/properties", strings.NewReader(string(body))))
	if rec.Code != http.StatusOK || a.engineSnapshot()[0] != probe || engineURL(probe) != probe.endpoint {
		t.Fatal("saving unchanged redacted properties replaced the authenticated engine")
	}
	probe.inventoryErr = errors.New("upstream echoed private-password and private-query")
	_, statuses := a.engineComponent.inspectInventory()
	for _, status := range statuses {
		if strings.Contains(status.LastError, "private-") || status.LastError == "" {
			t.Fatalf("inventory error is not explicit and redacted: %+v", status)
		}
	}
}

func TestPairingRejectsSubstitutionOversizeAndRedirect(t *testing.T) {
	for _, mode := range []string{"authorized", "substituted certificate", "substituted roster", "invalid roster", "trailing JSON", "oversize", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			coordinator, err := peeridentity.Open(filepath.Join(t.TempDir(), "coordinator.json"), "coordinator")
			if err != nil {
				t.Fatal(err)
			}

			offered := coordinator.CertificatePEM()
			if mode == "substituted certificate" || mode == "substituted roster" {
				other, err := peeridentity.Open(filepath.Join(t.TempDir(), "other.json"), "coordinator")
				if err != nil {
					t.Fatal(err)
				}
				offered = other.CertificatePEM()
			}
			redirected := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Error("pairing followed an insecure redirect")
				w.WriteHeader(http.StatusNoContent)
			}))
			defer redirected.Close()
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "redirect" {
					http.Redirect(w, r, redirected.URL, http.StatusTemporaryRedirect)
					return
				}
				response := map[string]any{"id": coordinator.ID(), "role": "orchestrator", "certificate_pem": offered}
				if mode == "authorized" {
					response["peers"] = map[string]string{coordinator.ID(): offered}
				}
				if mode == "substituted roster" {
					response["certificate_pem"] = coordinator.CertificatePEM()
					response["peers"] = map[string]string{coordinator.ID(): offered}
				}
				if mode == "invalid roster" {
					response["peers"] = map[string]string{"bad-peer": "not a certificate"}
				}
				if mode == "oversize" {
					response["padding"] = strings.Repeat("x", int(httpx.ControlJSONBodyBytes)+1)
				}
				if err := json.NewEncoder(w).Encode(response); err != nil && mode != "oversize" {
					t.Error(err)
				}
				if mode == "trailing JSON" {
					_, _ = w.Write([]byte(`{"trailing":true}`))
				}
			}))
			server.TLS = coordinator.ServerTLSConfig()
			server.StartTLS()
			defer server.Close()
			a := newTestAgent(t, "pairing-boundary")
			if err := a.InitializeIdentity(); err != nil {
				t.Fatal(err)
			}
			err = a.orchestratorLink.pairWithAddressContext(context.Background(), server.URL, coordinator.ID(), "123456", securityFingerprint(t, coordinator))
			if mode == "authorized" {
				if err != nil || !a.identity.IsTrustedID(coordinator.ID()) {
					t.Fatalf("authorized pairing failed: %v", err)
				}
			} else if err == nil || len(a.identity.TrustedIDs()) != 0 {
				t.Fatalf("invalid pairing changed trust: mode=%s error=%v", mode, err)
			}
		})
	}
}

func TestPinnedLeaderReconnectIgnoresPriorBootstrapAnchor(t *testing.T) {
	leader, err := peeridentity.Open(filepath.Join(t.TempDir(), "leader.json"), "leader")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, nodePairingResponse{
			ID: leader.ID(), Role: "orchestrator", CertificatePEM: leader.CertificatePEM(),
		})
	}))
	server.TLS = leader.ServerTLSConfig()
	server.StartTLS()
	defer server.Close()
	a := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "pinned-reconnect", OrchestratorFingerprint: strings.Repeat("0", 64),
	})
	if err := a.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	if err := a.identity.TrustPeer(leader.ID(), leader.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	if err := a.orchestratorLink.pairWithAddressContext(context.Background(), server.URL, leader.ID(), "123456"); err != nil {
		t.Fatalf("prior bootstrap anchor blocked a pinned leader: %v", err)
	}
	if err := a.orchestratorLink.pairWithAddressContext(context.Background(), server.URL, leader.ID(), "123456", strings.Repeat("0", 64)); err == nil {
		t.Fatal("explicit re-pairing fingerprint was ignored in favor of a stored pin")
	}
}
