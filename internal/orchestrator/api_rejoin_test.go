package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

func TestRejoinEndpointIssuesNodeBoundSingleUseTicketAndAudits(t *testing.T) {
	var audit bytes.Buffer
	previousLogger := slog.Default()
	previousLogWriter := log.Writer()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&audit, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
		log.SetOutput(previousLogWriter)
	})

	server, identity, httpsServer, _ := newRejoinTestServer(t, false)
	nodePath := filepath.Join(t.TempDir(), "node.json")
	node, err := peeridentity.Open(nodePath, "node-a")
	if err != nil {
		t.Fatal(err)
	}
	otherNode, err := peeridentity.Open(filepath.Join(t.TempDir(), "node-b.json"), "node-b")
	if err != nil {
		t.Fatal(err)
	}
	trustRejoinTestNode(t, identity, httpsServer.URL, node)
	trustRejoinTestNode(t, identity, httpsServer.URL, otherNode)

	issueClient := rejoinTestClient(t, node, server.SelfID())
	response, err := issueClient.Post(httpsServer.URL+"/api/pairing/rejoin-ticket", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	var issued rejoinTicketResponse
	if err := json.NewDecoder(response.Body).Decode(&issued); err != nil {
		_ = response.Body.Close()
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("ticket request returned %s", response.Status)
	}
	if issued.NodeID != node.ID() || issued.OrchestratorID != server.SelfID() ||
		issued.Ticket == "" || !issued.ExpiresAt.After(time.Now()) ||
		issued.ExpiresAt.After(time.Now().Add(rejoinTicketTTL)) {
		t.Fatalf("issued ticket response = %+v", issued)
	}

	wrongNodeClient := rejoinTestClient(t, otherNode, server.SelfID())
	wrongResponse := postRejoinTestRequest(t, wrongNodeClient, httpsServer.URL, rejoinRequest{
		ID: otherNode.ID(), CertificatePEM: otherNode.CertificatePEM(), Ticket: issued.Ticket,
	})
	_ = wrongResponse.Body.Close()
	if wrongResponse.StatusCode != http.StatusForbidden {
		t.Fatalf("wrong-node ticket request returned %s, want 403", wrongResponse.Status)
	}
	if got := identity.TrustedPeers()[otherNode.ID()]; got != otherNode.CertificatePEM() {
		t.Fatal("wrong-node request changed the other node's trusted certificate")
	}

	restartedNode, err := peeridentity.Open(nodePath, node.ID())
	if err != nil {
		t.Fatal(err)
	}
	if restartedNode.CertificatePEM() == node.CertificatePEM() {
		t.Fatal("restarted node reused its previous certificate")
	}
	rejoinClient := rejoinTestClient(t, restartedNode, server.SelfID())
	request := rejoinRequest{
		ID: restartedNode.ID(), PeerAddr: "https://127.0.0.1:7451",
		CertificatePEM: restartedNode.CertificatePEM(), Ticket: issued.Ticket,
	}
	rejoinResponse := postRejoinTestRequest(t, rejoinClient, httpsServer.URL, request)
	var paired pairingJoinResponse
	if err := json.NewDecoder(rejoinResponse.Body).Decode(&paired); err != nil {
		_ = rejoinResponse.Body.Close()
		t.Fatal(err)
	}
	_ = rejoinResponse.Body.Close()
	if rejoinResponse.StatusCode != http.StatusOK || paired.ID != server.SelfID() ||
		paired.Role != "orchestrator" {
		t.Fatalf("rejoin response = %s %+v", rejoinResponse.Status, paired)
	}
	if got := identity.TrustedPeers()[node.ID()]; got != restartedNode.CertificatePEM() {
		t.Fatal("orchestrator did not replace the node certificate after rejoin")
	}

	reusedResponse := postRejoinTestRequest(t, rejoinClient, httpsServer.URL, request)
	_ = reusedResponse.Body.Close()
	if reusedResponse.StatusCode != http.StatusForbidden {
		t.Fatalf("reused ticket returned %s, want 403", reusedResponse.Status)
	}
	logs := audit.String()
	for _, event := range []string{
		"event=rejoin.ticket.issued", "event=rejoin.ticket.consumed",
		"event=rejoin.ticket.rejected", "ticket_wrong_node",
	} {
		if !strings.Contains(logs, event) {
			t.Errorf("audit log is missing %q: %s", event, logs)
		}
	}
	if strings.Contains(logs, issued.Ticket) {
		t.Fatal("audit log contains the raw rejoin ticket")
	}
}

func TestRejoinEndpointRejectsExpiredTicketAndPreservesStrictPolicy(t *testing.T) {
	t.Run("expired", func(t *testing.T) {
		server, identity, httpsServer, _ := newRejoinTestServer(t, false)
		nodePath := filepath.Join(t.TempDir(), "node.json")
		node, err := peeridentity.Open(nodePath, "node-expired")
		if err != nil {
			t.Fatal(err)
		}
		trustRejoinTestNode(t, identity, httpsServer.URL, node)
		now := time.Now()
		ticket, _, err := identity.IssueRejoinTicket(node.ID(), now.Add(-2*time.Minute), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		restartedNode, err := peeridentity.Open(nodePath, node.ID())
		if err != nil {
			t.Fatal(err)
		}
		client := rejoinTestClient(t, restartedNode, server.SelfID())
		response := postRejoinTestRequest(t, client, httpsServer.URL, rejoinRequest{
			ID: restartedNode.ID(), CertificatePEM: restartedNode.CertificatePEM(), Ticket: ticket,
		})
		_ = response.Body.Close()
		if response.StatusCode != http.StatusForbidden {
			t.Fatalf("expired ticket returned %s, want 403", response.Status)
		}
		if got := identity.TrustedPeers()[node.ID()]; got != node.CertificatePEM() {
			t.Fatal("expired ticket changed the trusted node certificate")
		}
	})

	t.Run("off-subnet", func(t *testing.T) {
		server, identity, httpsServer, _ := newRejoinTestServerAt(
			t, false, &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1)},
		)
		nodePath := filepath.Join(t.TempDir(), "node.json")
		node, err := peeridentity.Open(nodePath, "node-off-subnet")
		if err != nil {
			t.Fatal(err)
		}
		trustRejoinTestNode(t, identity, httpsServer.URL, node)
		oldClient := rejoinTestClient(t, node, server.SelfID())
		issueResponse, err := oldClient.Post(httpsServer.URL+"/api/pairing/rejoin-ticket", "", nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = issueResponse.Body.Close()
		if issueResponse.StatusCode != http.StatusForbidden {
			t.Fatalf("off-subnet ticket request returned %s, want 403", issueResponse.Status)
		}

		ticket, _, err := identity.IssueRejoinTicket(node.ID(), time.Now(), rejoinTicketTTL)
		if err != nil {
			t.Fatal(err)
		}
		restartedNode, err := peeridentity.Open(nodePath, node.ID())
		if err != nil {
			t.Fatal(err)
		}
		client := rejoinTestClient(t, restartedNode, server.SelfID())
		response := postRejoinTestRequest(t, client, httpsServer.URL, rejoinRequest{
			ID: restartedNode.ID(), CertificatePEM: restartedNode.CertificatePEM(), Ticket: ticket,
		})
		_ = response.Body.Close()
		if response.StatusCode != http.StatusForbidden {
			t.Fatalf("off-subnet rejoin returned %s, want 403", response.Status)
		}
		if got := identity.TrustedPeers()[node.ID()]; got != node.CertificatePEM() {
			t.Fatal("off-subnet rejoin changed the trusted node certificate")
		}
	})

	t.Run("strict mode", func(t *testing.T) {
		server, identity, httpsServer, _ := newRejoinTestServer(t, true)
		nodePath := filepath.Join(t.TempDir(), "node.json")
		node, err := peeridentity.Open(nodePath, "node-strict")
		if err != nil {
			t.Fatal(err)
		}
		trustRejoinTestNode(t, identity, httpsServer.URL, node)
		oldClient := rejoinTestClient(t, node, server.SelfID())
		issueResponse, err := oldClient.Post(httpsServer.URL+"/api/pairing/rejoin-ticket", "", nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = issueResponse.Body.Close()
		if issueResponse.StatusCode != http.StatusForbidden {
			t.Fatalf("strict-mode ticket request returned %s, want 403", issueResponse.Status)
		}

		ticket, _, err := identity.IssueRejoinTicket(node.ID(), time.Now(), rejoinTicketTTL)
		if err != nil {
			t.Fatal(err)
		}
		restartedNode, err := peeridentity.Open(nodePath, node.ID())
		if err != nil {
			t.Fatal(err)
		}
		request := rejoinRequest{
			ID: restartedNode.ID(), CertificatePEM: restartedNode.CertificatePEM(), Ticket: ticket,
		}
		client := rejoinTestClient(t, restartedNode, server.SelfID())
		rejected := postRejoinTestRequest(t, client, httpsServer.URL, request)
		_ = rejected.Body.Close()
		if rejected.StatusCode != http.StatusForbidden {
			t.Fatalf("strict-mode rejoin returned %s, want 403", rejected.Status)
		}
		if got := identity.TrustedPeers()[node.ID()]; got != node.CertificatePEM() {
			t.Fatal("strict-mode rejoin changed the trusted node certificate")
		}

		server.SetPairingVerificationRequired(false)
		accepted := postRejoinTestRequest(t, client, httpsServer.URL, request)
		_ = accepted.Body.Close()
		if accepted.StatusCode != http.StatusOK {
			t.Fatalf("ticket rejected under strict mode was not preserved: %s", accepted.Status)
		}
	})

	t.Run("trust persistence failure", func(t *testing.T) {
		server, identity, httpsServer, identityPath := newRejoinTestServer(t, false)
		nodePath := filepath.Join(t.TempDir(), "node.json")
		node, err := peeridentity.Open(nodePath, "node-persist-failure")
		if err != nil {
			t.Fatal(err)
		}
		trustRejoinTestNode(t, identity, httpsServer.URL, node)
		ticket, _, err := identity.IssueRejoinTicket(node.ID(), time.Now(), rejoinTicketTTL)
		if err != nil {
			t.Fatal(err)
		}
		restartedNode, err := peeridentity.Open(nodePath, node.ID())
		if err != nil {
			t.Fatal(err)
		}
		backupPath := identityPath + ".backup"
		if err := os.Rename(identityPath, backupPath); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(identityPath, 0o700); err != nil {
			t.Fatal(err)
		}
		client := rejoinTestClient(t, restartedNode, server.SelfID())
		response := postRejoinTestRequest(t, client, httpsServer.URL, rejoinRequest{
			ID: restartedNode.ID(), CertificatePEM: restartedNode.CertificatePEM(), Ticket: ticket,
		})
		_ = response.Body.Close()
		if response.StatusCode != http.StatusInternalServerError {
			t.Fatalf("trust persistence failure returned %s, want 500", response.Status)
		}
		if got := identity.TrustedPeers()[node.ID()]; got != node.CertificatePEM() {
			t.Fatal("failed trust persistence changed the in-memory trusted certificate")
		}
		if err := os.Remove(identityPath); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(backupPath, identityPath); err != nil {
			t.Fatal(err)
		}
		retried := postRejoinTestRequest(t, client, httpsServer.URL, rejoinRequest{
			ID: restartedNode.ID(), CertificatePEM: restartedNode.CertificatePEM(), Ticket: ticket,
		})
		_ = retried.Body.Close()
		if retried.StatusCode != http.StatusOK {
			t.Fatalf("rejoin after restoring persistence returned %s, want 200", retried.Status)
		}
	})
}

func newRejoinTestServer(t *testing.T, strict bool) (*Server, *peeridentity.Identity, *httptest.Server, string) {
	return newRejoinTestServerAt(t, strict, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
}

func newRejoinTestServerAt(
	t *testing.T,
	strict bool,
	localAddr net.Addr,
) (*Server, *peeridentity.Identity, *httptest.Server, string) {
	t.Helper()
	cfg := config.DefaultOrchestratorConfig()
	cfg.Mode = config.OrchestratorModeMinimal
	cfg.MemoryPath = filepath.Join(t.TempDir(), "orchestrator.db")
	server, err := NewWithConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close rejoin test server: %v", err)
		}
	})
	identityPath := filepath.Join(t.TempDir(), "orchestrator.json")
	identity, err := peeridentity.Open(identityPath, "orchestrator-rejoin-test")
	if err != nil {
		t.Fatal(err)
	}
	if err := server.UseIdentity(identity); err != nil {
		t.Fatal(err)
	}
	server.SetPairingVerificationRequired(strict)
	mux := server.Mux()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), http.LocalAddrContextKey, localAddr)
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
	httpsServer := httptest.NewUnstartedServer(handler)
	httpsServer.TLS = identity.ServerTLSConfig()
	httpsServer.StartTLS()
	t.Cleanup(httpsServer.Close)
	return server, identity, httpsServer, identityPath
}

func trustRejoinTestNode(t *testing.T, orchestrator *peeridentity.Identity, address string, node *peeridentity.Identity) {
	t.Helper()
	if err := orchestrator.TrustPeerDetails(node.ID(), node.CertificatePEM(), "node", ""); err != nil {
		t.Fatal(err)
	}
	if err := node.TrustPeerDetails(orchestrator.ID(), orchestrator.CertificatePEM(), "orchestrator", address); err != nil {
		t.Fatal(err)
	}
}

func rejoinTestClient(t *testing.T, identity *peeridentity.Identity, orchestratorID string) *http.Client {
	t.Helper()
	transport := identity.BootstrapTransportForID(orchestratorID)
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{
		Transport: transport, Timeout: 3 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("redirects are not expected in rejoin tests")
		},
	}
}

func postRejoinTestRequest(t *testing.T, client *http.Client, address string, body rejoinRequest) *http.Response {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, address+"/api/pairing/rejoin", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}
