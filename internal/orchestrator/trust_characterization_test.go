package orchestrator

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/peeridentity"
)

// trustRemotePeerAddr is outside the local subnet, so joins require a PIN.
const trustRemotePeerAddr = "203.0.113.10:1234"

type trustFixture struct {
	srv      *Server
	identity *peeridentity.Identity
	path     string
	pin      string
}

func newTrustFixture(t *testing.T) trustFixture {
	t.Helper()
	srv := newTestServer(t)
	path := filepath.Join(t.TempDir(), "orchestrator.json")
	identity, err := peeridentity.Open(path, srv.SelfID())
	if err != nil {
		t.Fatal(err)
	}
	srv.identity = identity
	pin, err := identity.NewPairingPIN()
	if err != nil {
		t.Fatal(err)
	}
	return trustFixture{srv: srv, identity: identity, path: path, pin: pin}
}

func (f trustFixture) durableIdentity(t *testing.T) []byte {
	t.Helper()
	raw, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func trustJoin(t *testing.T, handler http.Handler, request pairingJoinRequest) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	httpRequest := httptest.NewRequest(http.MethodPost, "/api/pairing/join", bytes.NewReader(body))
	httpRequest.RemoteAddr = trustRemotePeerAddr
	httpRequest.TLS = &tls.ConnectionState{}
	if block, _ := pem.Decode([]byte(request.CertificatePEM)); block != nil {
		if certificate, err := x509.ParseCertificate(block.Bytes); err == nil {
			httpRequest.TLS.PeerCertificates = []*x509.Certificate{certificate}
		}
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httpRequest)
	return response
}

func trustNode(t *testing.T, id string) *peeridentity.Identity {
	t.Helper()
	node, err := peeridentity.Open(filepath.Join(t.TempDir(), id+".json"), id)
	if err != nil {
		t.Fatal(err)
	}

	return node
}

func trustTLS(t *testing.T, identity *peeridentity.Identity) *tls.ConnectionState {
	t.Helper()
	block, _ := pem.Decode([]byte(identity.CertificatePEM()))
	if block == nil {
		t.Fatal("missing test certificate")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.ConnectionState{PeerCertificates: []*x509.Certificate{certificate}}
}

func nodeJoin(node *peeridentity.Identity, pin string) pairingJoinRequest {
	return pairingJoinRequest{ID: node.ID(), Role: "node", PIN: pin, CertificatePEM: node.CertificatePEM()}
}

func TestRemotePairingJoinRejectionsLeaveTrustUnchanged(t *testing.T) {
	tests := []struct {
		name string
		// prepare returns the join request and whether the original PIN must
		// remain consumable after the rejection.
		prepare    func(t *testing.T, f trustFixture, node *peeridentity.Identity) (pairingJoinRequest, bool)
		wantStatus int
	}{
		{
			name: "consumed PIN",
			prepare: func(t *testing.T, f trustFixture, node *peeridentity.Identity) (pairingJoinRequest, bool) {
				if !f.identity.ConsumePairingPIN(f.pin) {
					t.Fatal("could not consume the PIN")
				}
				return nodeJoin(node, f.pin), false
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "correct PIN after remote lockout",
			prepare: func(t *testing.T, f trustFixture, node *peeridentity.Identity) (pairingJoinRequest, bool) {
				wrong := "000000"
				if wrong == f.pin {
					wrong = "999999"
				}
				for attempt := range 20 {
					if response := trustJoin(t, f.srv.Mux(), nodeJoin(node, wrong)); response.Code != http.StatusUnauthorized {
						t.Fatalf("wrong PIN attempt %d returned %d, want 401", attempt+1, response.Code)
					}
				}
				return nodeJoin(node, f.pin), false
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "PIN rotated by loopback regeneration",
			prepare: func(t *testing.T, f trustFixture, node *peeridentity.Identity) (pairingJoinRequest, bool) {
				for {
					request := httptest.NewRequest(http.MethodPost, "/api/pairing/pin", nil)
					request.Host = "localhost"
					request.RemoteAddr = "127.0.0.1:1234"
					response := httptest.NewRecorder()
					f.srv.Mux().ServeHTTP(response, request)
					if response.Code != http.StatusOK {
						t.Fatalf("PIN regeneration returned %d", response.Code)
					}
					if active, _, _ := f.identity.ActivePairingPIN(); active != f.pin {
						break
					}
				}
				return nodeJoin(node, f.pin), false
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "certificate for another identity",
			prepare: func(t *testing.T, f trustFixture, node *peeridentity.Identity) (pairingJoinRequest, bool) {
				request := nodeJoin(node, f.pin)
				request.CertificatePEM = trustNode(t, "certificate-owner").CertificatePEM()
				return request, true
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "malformed certificate",
			prepare: func(_ *testing.T, f trustFixture, node *peeridentity.Identity) (pairingJoinRequest, bool) {
				request := nodeJoin(node, f.pin)
				request.CertificatePEM = "not a certificate"
				return request, true
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "peer claims the orchestrator identity",
			prepare: func(_ *testing.T, f trustFixture, _ *peeridentity.Identity) (pairingJoinRequest, bool) {
				return nodeJoin(f.identity, f.pin), true
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "empty peer identity",
			prepare: func(_ *testing.T, f trustFixture, node *peeridentity.Identity) (pairingJoinRequest, bool) {
				request := nodeJoin(node, f.pin)
				request.ID = ""
				return request, true
			},
			wantStatus: http.StatusBadRequest,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newTrustFixture(t)
			node := trustNode(t, "joining-node")
			request, pinStillValid := test.prepare(t, f, node)
			durableBefore := f.durableIdentity(t)

			response := trustJoin(t, f.srv.Mux(), request)
			if response.Code != test.wantStatus {
				t.Fatalf("join returned %d, want %d: %s", response.Code, test.wantStatus, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "BEGIN CERTIFICATE") {
				t.Fatal("rejected join returned certificate material")
			}
			if ids := f.identity.TrustedIDs(); len(ids) != 0 {
				t.Fatalf("rejected join changed the trust roster: %v", ids)
			}
			if !bytes.Equal(durableBefore, f.durableIdentity(t)) {
				t.Fatal("rejected join rewrote the durable identity file")
			}
			if got := f.identity.VerifyPairingPIN(f.pin); got != pinStillValid {
				t.Fatalf("original PIN valid after rejection = %t, want %t", got, pinStillValid)
			}
			if f.srv.metrics.snapshot().pairingEvents != 0 {
				t.Fatal("rejected join was recorded as a pairing event")
			}
		})
	}
}

func TestRemotePINJoinRotatesPINForNextJoin(t *testing.T) {
	f := newTrustFixture(t)
	first, second := trustNode(t, "first-node"), trustNode(t, "second-node")

	if response := trustJoin(t, f.srv.Mux(), nodeJoin(first, f.pin)); response.Code != http.StatusOK {
		t.Fatalf("first join returned %d: %s", response.Code, response.Body.String())
	}
	reloaded, err := peeridentity.Open(f.path, f.srv.SelfID())
	if err != nil {
		t.Fatal(err)
	}
	if !reloaded.IsTrustedID(first.ID()) {
		t.Fatal("successful join was not persisted")
	}
	next, _, active := f.identity.ActivePairingPIN()
	if !active {
		t.Fatal("successful PIN join did not create the next PIN")
	}

	if next != f.pin {
		if response := trustJoin(t, f.srv.Mux(), nodeJoin(second, f.pin)); response.Code != http.StatusUnauthorized {
			t.Fatalf("join with the consumed PIN returned %d, want 401", response.Code)
		}
		if f.identity.IsTrustedID(second.ID()) {
			t.Fatal("consumed PIN admitted a second node")
		}
	}
	if response := trustJoin(t, f.srv.Mux(), nodeJoin(second, next)); response.Code != http.StatusOK {
		t.Fatalf("join with the rotated PIN returned %d: %s", response.Code, response.Body.String())
	}
	if !f.identity.IsTrustedID(second.ID()) {
		t.Fatal("rotated PIN did not admit the second node")
	}
}

// TestPairingJoinPersistenceFailureCharacterization pins join behavior when
// the trust write fails: the response is 400, the node is not trusted in
// memory, the PIN is consumed, and no replacement PIN is issued. The status
// code and PIN handling are unchanged wire behavior.
func TestPairingJoinPersistenceFailureCharacterization(t *testing.T) {
	f := newTrustFixture(t)
	node := trustNode(t, "unsaved-node")
	if err := os.Remove(f.path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(f.path, 0o700); err != nil {
		t.Fatal(err)
	}

	response := trustJoin(t, f.srv.Mux(), nodeJoin(node, f.pin))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("join with failed persistence returned %d, want 400: %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "BEGIN CERTIFICATE") {
		t.Fatal("failed join returned the orchestrator certificate")
	}
	if f.identity.IsTrustedID(node.ID()) {
		t.Fatal("failed persistence left the node trusted in memory")
	}
	if _, _, active := f.identity.ActivePairingPIN(); active {
		t.Fatal("characterization changed: failed persistence now leaves an active PIN")
	}
	if f.identity.VerifyPairingPIN(f.pin) {
		t.Fatal("PIN remained usable after a join consumed it")
	}
	if f.srv.metrics.snapshot().pairingEvents != 0 {
		t.Fatal("failed join was recorded as a pairing event")
	}
}

func TestYieldedOrchestratorRejectsPairingAndMutations(t *testing.T) {
	f := newTrustFixture(t)
	f.srv.Retire()
	if f.srv.Elected() {
		t.Fatal("retired server still reports itself elected")
	}
	handler := requireElectedOrchestrator(f.srv.isElectedOrchestrator, f.srv.fenceStateChangingRequests(f.srv.Mux()), nil)
	node := trustNode(t, "yielded-join-node")

	tests := []struct {
		name    string
		request func() *httptest.ResponseRecorder
	}{
		{name: "pairing join", request: func() *httptest.ResponseRecorder { return trustJoin(t, handler, nodeJoin(node, f.pin)) }},
		{name: "session create", request: func() *httptest.ResponseRecorder {
			request := httptest.NewRequest(http.MethodPost, "/api/agent/sessions", strings.NewReader(`{"title":"yielded","max_steps":1}`))
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			return response
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if response := test.request(); response.Code != http.StatusServiceUnavailable {
				t.Fatalf("yielded server returned %d, want 503: %s", response.Code, response.Body.String())
			}
			if f.identity.IsTrustedID(node.ID()) || !f.identity.VerifyPairingPIN(f.pin) {
				t.Fatal("yielded server changed trust or consumed the PIN")
			}
			if sessions := f.srv.listAgentSessions(); len(sessions) != 0 {
				t.Fatalf("yielded server created %d sessions", len(sessions))
			}
		})
	}

	f.srv.Reinstate()
	if response := trustJoin(t, handler, nodeJoin(node, f.pin)); response.Code != http.StatusOK {
		t.Fatalf("reinstated server join returned %d: %s", response.Code, response.Body.String())
	}
}

func TestViewFenceAdditionalStaleViewCases(t *testing.T) {
	srv := newTrustFixture(t).srv
	handler := srv.fenceStateChangingRequests(srv.Mux())
	tests := []struct {
		name       string
		method     string
		path       string
		id         string
		epoch      string
		wantStatus int
	}{
		{name: "epoch without orchestrator ID", method: http.MethodPost, path: "/api/agent/sessions", epoch: srv.orchestratorEpoch, wantStatus: http.StatusConflict},
		{name: "current ID with stale epoch on delete", method: http.MethodDelete, path: "/api/agent/sessions/missing", id: srv.SelfID(), epoch: "stale-epoch", wantStatus: http.StatusConflict},
		{name: "stale view on put", method: http.MethodPut, path: "/api/agent/sessions", id: "old-orchestrator", epoch: srv.orchestratorEpoch, wantStatus: http.StatusConflict},
		{name: "stale view on patch", method: http.MethodPatch, path: "/api/agent/sessions", id: srv.SelfID(), epoch: "stale-epoch", wantStatus: http.StatusConflict},
		{name: "stale view may read", method: http.MethodGet, path: "/api/agent/sessions", id: "old-orchestrator", epoch: "stale-epoch", wantStatus: http.StatusOK},
		// Pairing is exempt; the handler rejects plaintext with 426, not the fence.
		{name: "stale view does not fence pairing", method: http.MethodPost, path: "/api/pairing/join", id: "old-orchestrator", epoch: "stale-epoch", wantStatus: http.StatusUpgradeRequired},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.path, strings.NewReader(`{"title":"must not be created","max_steps":1}`))
			if test.id != "" {
				request.Header.Set(HeaderOrchestratorID, test.id)
			}
			if test.epoch != "" {
				request.Header.Set(HeaderOrchestratorEpoch, test.epoch)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus {
				t.Fatalf("returned %d, want %d: %s", response.Code, test.wantStatus, response.Body.String())
			}
			if response.Header().Get(HeaderOrchestratorID) != srv.SelfID() ||
				response.Header().Get(HeaderOrchestratorEpoch) != srv.orchestratorEpoch {
				t.Fatalf("response omitted current view headers: %#v", response.Header())
			}
			if sessions := srv.listAgentSessions(); len(sessions) != 0 {
				t.Fatalf("stale request created %d sessions", len(sessions))
			}
		})
	}
}
