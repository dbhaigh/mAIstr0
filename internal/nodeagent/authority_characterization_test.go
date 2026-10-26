package nodeagent

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

type authorityFixture struct {
	agent    *Agent
	previous *peeridentity.Identity
	current  *peeridentity.Identity
	worker   *peeridentity.Identity
	unpaired *peeridentity.Identity
	// impostor presents the current orchestrator's ID with an unpinned key.
	impostor *peeridentity.Identity
}

func newAuthorityFixture(t *testing.T, nodeID string) authorityFixture {
	t.Helper()
	cfg := config.DefaultNodeConfig()
	cfg.NodeID = nodeID
	cfg.MemoryPath = filepath.Join(t.TempDir(), "node.db")
	agent := mustNewTestAgent(t, cfg)
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	fixture := authorityFixture{
		agent:    agent,
		previous: mustIdentity(t, "previous-orchestrator"),
		current:  mustIdentity(t, "current-orchestrator"),
		worker:   mustIdentity(t, "paired-worker"),
		unpaired: mustIdentity(t, "unpaired-peer"),
	}
	impostor, err := peeridentity.Open(filepath.Join(t.TempDir(), "impostor.json"), fixture.current.ID())
	if err != nil {
		t.Fatal(err)
	}
	fixture.impostor = impostor
	for _, peer := range []struct {
		identity *peeridentity.Identity
		role     string
	}{
		{fixture.previous, "orchestrator"},
		{fixture.current, "orchestrator"},
		{fixture.worker, "node"},
	} {
		if err := agent.identityComponent.TrustPeerDetails(peer.identity.ID(), peer.identity.CertificatePEM(), peer.role, "https://127.0.0.1:7450"); err != nil {
			t.Fatal(err)
		}
	}
	agent.setOrchestratorID(fixture.current.ID())
	return fixture
}

func (f authorityFixture) callers(t *testing.T) []struct {
	name        string
	certificate *x509.Certificate
} {
	t.Helper()
	return []struct {
		name        string
		certificate *x509.Certificate
	}{
		{name: "plaintext"},
		{name: "unpaired", certificate: certificateFromPEM(t, f.unpaired.CertificatePEM())},
		{name: "impostor of current orchestrator", certificate: certificateFromPEM(t, f.impostor.CertificatePEM())},
		{name: "paired worker", certificate: certificateFromPEM(t, f.worker.CertificatePEM())},
		{name: "previous orchestrator", certificate: certificateFromPEM(t, f.previous.CertificatePEM())},
		{name: "current orchestrator", certificate: certificateFromPEM(t, f.current.CertificatePEM())},
	}
}

func TestCoordinatorWriteFenceMethodAndCallerMatrix(t *testing.T) {
	fixture := newAuthorityFixture(t, "write-fence-node")
	var writes atomic.Int32
	handler := fixture.agent.identityComponent.requireTrustedPeer(fixture.agent.currentCoordinatorWriteFence(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			writes.Add(1)
			w.WriteHeader(http.StatusNoContent)
		})))
	// Expected status per caller for reads and for state-changing methods.
	want := map[string]struct{ read, write int }{
		"plaintext":                        {http.StatusForbidden, http.StatusForbidden},
		"unpaired":                         {http.StatusForbidden, http.StatusForbidden},
		"impostor of current orchestrator": {http.StatusForbidden, http.StatusForbidden},
		"paired worker":                    {http.StatusNoContent, http.StatusNoContent},
		"previous orchestrator":            {http.StatusNoContent, http.StatusForbidden},
		"current orchestrator":             {http.StatusNoContent, http.StatusNoContent},
	}
	methods := []string{http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete}
	for _, caller := range fixture.callers(t) {
		for _, method := range methods {
			t.Run(caller.name+" "+method, func(t *testing.T) {
				expected := want[caller.name].read
				if method != http.MethodGet && method != http.MethodHead && method != http.MethodOptions {
					expected = want[caller.name].write
				}
				request := httptest.NewRequest(method, "/api/models", strings.NewReader(`{}`))
				if caller.certificate != nil {
					request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{caller.certificate}}
				}
				before := writes.Load()
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				if response.Code != expected {
					t.Fatalf("status = %d, want %d: %s", response.Code, expected, response.Body.String())
				}
				if reached := writes.Load() != before; reached != (expected == http.StatusNoContent) {
					t.Fatalf("handler reached = %t with status %d", reached, response.Code)
				}
			})
		}
	}
}

func TestCoordinatorHandoffMovesWriteAuthorityOverMutualTLS(t *testing.T) {
	fixture := newAuthorityFixture(t, "handoff-node")
	agent := fixture.agent
	var writes atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/models", func(w http.ResponseWriter, _ *http.Request) {
		writes.Add(1)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /api/models", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	server := httptest.NewUnstartedServer(agent.identityComponent.requireTrustedPeer(agent.currentCoordinatorWriteFence(mux)))
	server.TLS = agent.identityComponent.serverTLSConfig()
	server.StartTLS()
	t.Cleanup(server.Close)

	clientFor := func(identity *peeridentity.Identity) *http.Client {
		if err := identity.TrustPeer(agent.configSnapshot().NodeID, agent.identityComponent.CertificatePEM()); err != nil {
			t.Fatal(err)
		}
		transport := identity.ClientTransport()
		t.Cleanup(transport.CloseIdleConnections)
		return &http.Client{Timeout: 3 * time.Second, Transport: transport}
	}
	clients := map[string]*http.Client{
		fixture.previous.ID(): clientFor(fixture.previous),
		fixture.current.ID():  clientFor(fixture.current),
	}
	call := func(t *testing.T, id, method string) int {
		t.Helper()
		request, err := http.NewRequest(method, server.URL+"/api/models", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := clients[id].Do(request)
		if err != nil {
			t.Fatalf("%s %s by %s: %v", method, request.URL.Path, id, err)
		}
		_ = response.Body.Close()
		return response.StatusCode
	}

	steps := []struct {
		name       string
		apply      func(t *testing.T)
		caller     string
		method     string
		wantStatus int
		wantWrite  bool
	}{
		{name: "current coordinator writes", caller: fixture.current.ID(), method: http.MethodPost, wantStatus: http.StatusNoContent, wantWrite: true},
		{name: "previous coordinator is fenced", caller: fixture.previous.ID(), method: http.MethodPost, wantStatus: http.StatusForbidden},
		{name: "previous coordinator may still read", caller: fixture.previous.ID(), method: http.MethodGet, wantStatus: http.StatusOK},
		{
			name:   "authority returns to previous coordinator",
			apply:  func(*testing.T) { agent.setOrchestratorID(fixture.previous.ID()) },
			caller: fixture.previous.ID(), method: http.MethodPost, wantStatus: http.StatusNoContent, wantWrite: true,
		},
		{name: "replaced coordinator is fenced on its pooled connection", caller: fixture.current.ID(), method: http.MethodPost, wantStatus: http.StatusForbidden},
		{
			name: "unknown coordinator fails closed to the local node",
			apply: func(t *testing.T) {
				agent.setOrchestratorID("")
				if got := agent.currentElectedOrchestratorID(); got != agent.configSnapshot().NodeID {
					t.Fatalf("fallback coordinator = %q, want local node", got)
				}
			},
			caller: fixture.previous.ID(), method: http.MethodPost, wantStatus: http.StatusForbidden,
		},
		{
			name: "revoked current coordinator is rejected",
			apply: func(t *testing.T) {
				agent.setOrchestratorID(fixture.current.ID())
				if err := agent.identityComponent.RemovePeer(fixture.current.ID()); err != nil {
					t.Fatal(err)
				}
			},
			caller: fixture.current.ID(), method: http.MethodPost, wantStatus: http.StatusForbidden,
		},
	}
	for _, step := range steps {
		if step.apply != nil {
			step.apply(t)
		}
		before := writes.Load()
		if got := call(t, step.caller, step.method); got != step.wantStatus {
			t.Fatalf("%s: status = %d, want %d", step.name, got, step.wantStatus)
		}
		if wrote := writes.Load() != before; wrote != step.wantWrite {
			t.Fatalf("%s: write reached handler = %t, want %t", step.name, wrote, step.wantWrite)
		}
	}
}

func TestManagedLifecycleEndpointsRejectUnauthorizedCallers(t *testing.T) {
	fixture := newAuthorityFixture(t, "lifecycle-node")
	mux := http.NewServeMux()
	registerManagedEngineRoutes(mux, fixture.agent)
	// The node also serves this mux on loopback plaintext.
	loopback := peeridentity.LoopbackOnly(mux)
	routes := []struct{ name, method, path, body string }{
		{"start", http.MethodPost, "/api/engines/managed/missing/start", `{}`},
		{"stop", http.MethodPost, "/api/engines/managed/missing/stop", ``},
		{"restart", http.MethodPost, "/api/engines/managed/missing/restart", `{}`},
		{"proxy", http.MethodPost, "/api/engines/managed/missing/proxy/v1/chat/completions", `{}`},
	}
	statusesBefore := len(fixture.agent.managedEngines.Statuses())
	for _, caller := range append(fixture.callers(t), struct {
		name        string
		certificate *x509.Certificate
	}{name: "loopback plaintext"}) {
		for _, route := range routes {
			t.Run(route.name+" by "+caller.name, func(t *testing.T) {
				request := httptest.NewRequest(route.method, route.path, strings.NewReader(route.body))
				handler := http.Handler(mux)
				if caller.name == "loopback plaintext" {
					request.Host = "localhost"
					request.RemoteAddr = "127.0.0.1:1234"
					handler = loopback
				}
				if caller.certificate != nil {
					request.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{caller.certificate}}
				}
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				want := http.StatusForbidden
				if caller.name == "current orchestrator" {
					// The gate admits the caller; the handler reports the unknown engine.
					want = http.StatusNotFound
				}
				if response.Code != want {
					t.Fatalf("status = %d, want %d: %s", response.Code, want, response.Body.String())
				}
				if got := len(fixture.agent.managedEngines.Statuses()); got != statusesBefore {
					t.Fatalf("managed engine statuses changed from %d to %d", statusesBefore, got)
				}
			})
		}
	}
}
