package peeridentity

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func tcOpen(t *testing.T, id string) *Identity {
	t.Helper()
	identity, err := Open(filepath.Join(t.TempDir(), id+".json"), id)
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func tcTrackedConnections(identity *Identity) int {
	identity.mu.RLock()
	defer identity.mu.RUnlock()
	return len(identity.activeConns)
}

func tcPairingAttempts(identity *Identity) int {
	identity.mu.RLock()
	defer identity.mu.RUnlock()
	return identity.pairingAttempts
}

func tcSetPairingExpiry(identity *Identity, expires time.Time) {
	identity.mu.Lock()
	identity.pairingExpires = expires
	identity.mu.Unlock()
}

// tcStartTLSServer serves handler with config and counts requests that reach it.
func tcStartTLSServer(t *testing.T, config *tls.Config, handler http.Handler) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var reached atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached.Add(1)
		handler.ServeHTTP(w, r)
	}))
	server.TLS = config
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, &reached
}

func tcNoContent(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }

func TestPairingPINLifecycleCharacterization(t *testing.T) {
	const wrongPIN = "000000"
	tests := []struct {
		name string
		run  func(t *testing.T, identity *Identity, pin string)
	}{
		{
			name: "PIN just before expiry is accepted once",
			run: func(t *testing.T, identity *Identity, pin string) {
				tcSetPairingExpiry(identity, time.Now().Add(time.Second))
				if !identity.VerifyPairingPIN(pin) || !identity.ConsumePairingPIN(pin) {
					t.Fatal("PIN inside its validity window was rejected")
				}
			},
		},
		{
			name: "PIN just after expiry is rejected without counting an attempt",
			run: func(t *testing.T, identity *Identity, pin string) {
				tcSetPairingExpiry(identity, time.Now().Add(-time.Millisecond))
				if identity.VerifyPairingPIN(pin) || identity.ConsumePairingPIN(pin) {
					t.Fatal("expired PIN was accepted")
				}
				if _, _, active := identity.ActivePairingPIN(); active {
					t.Fatal("expired PIN is reported active")
				}
				if got := tcPairingAttempts(identity); got != 0 {
					t.Fatalf("expired PIN counted %d failed attempts, want 0", got)
				}
			},
		},
		{
			name: "reuse after successful consumption is rejected everywhere",
			run: func(t *testing.T, identity *Identity, pin string) {
				if !identity.ConsumePairingPIN(pin) {
					t.Fatal("valid PIN was rejected")
				}
				if identity.VerifyPairingPIN(pin) {
					t.Fatal("consumed PIN still verifies")
				}
				if _, _, active := identity.ActivePairingPIN(); active {
					t.Fatal("consumed PIN is still reported active")
				}
				if identity.ConsumePairingPIN(pin) {
					t.Fatal("consumed PIN was accepted a second time")
				}
			},
		},
		{
			name: "nineteen failures still allow the correct PIN",
			run: func(t *testing.T, identity *Identity, pin string) {
				for range 19 {
					if identity.ConsumePairingPIN(wrongPIN) {
						t.Fatal("wrong PIN was accepted")
					}
				}
				if !identity.ConsumePairingPIN(pin) {
					t.Fatal("correct PIN was rejected before the lockout threshold")
				}
			},
		},
		{
			name: "lockout rejects the correct PIN in every accessor",
			run: func(t *testing.T, identity *Identity, pin string) {
				for range 20 {
					_ = identity.ConsumePairingPIN(wrongPIN)
				}
				if identity.VerifyPairingPIN(pin) || identity.ConsumePairingPIN(pin) {
					t.Fatal("correct PIN was accepted after lockout")
				}
				if _, _, active := identity.ActivePairingPIN(); active {
					t.Fatal("locked-out PIN is reported active")
				}
			},
		},
		{
			name: "wrong-length PINs count toward lockout",
			run: func(t *testing.T, identity *Identity, pin string) {
				for range 20 {
					_ = identity.ConsumePairingPIN(pin + "0")
				}
				if identity.ConsumePairingPIN(pin) {
					t.Fatal("correct PIN was accepted after wrong-length lockout")
				}
			},
		},
		{
			name: "verification never counts toward lockout",
			run: func(t *testing.T, identity *Identity, pin string) {
				for range 25 {
					_ = identity.VerifyPairingPIN(wrongPIN)
				}
				if got := tcPairingAttempts(identity); got != 0 {
					t.Fatalf("VerifyPairingPIN counted %d attempts, want 0", got)
				}
				if !identity.ConsumePairingPIN(pin) {
					t.Fatal("correct PIN was rejected after read-only verification")
				}
			},
		},
		{
			name: "rotation invalidates the previous PIN",
			run: func(t *testing.T, identity *Identity, pin string) {
				next := pin
				for next == pin {
					var err error
					if next, err = identity.NewPairingPIN(); err != nil {
						t.Fatal(err)
					}
				}
				if identity.VerifyPairingPIN(pin) || identity.ConsumePairingPIN(pin) {
					t.Fatal("rotated-out PIN was accepted")
				}
				if !identity.ConsumePairingPIN(next) {
					t.Fatal("replacement PIN was rejected")
				}
			},
		},
		{
			name: "rotation clears lockout and restores a fresh window",
			run: func(t *testing.T, identity *Identity, _ string) {
				for range 20 {
					_ = identity.ConsumePairingPIN(wrongPIN)
				}
				next, err := identity.NewPairingPIN()
				if err != nil {
					t.Fatal(err)
				}
				active, expires, ok := identity.ActivePairingPIN()
				if !ok || active != next {
					t.Fatal("replacement PIN is not active after lockout")
				}
				if remaining := time.Until(expires); remaining < 29*time.Minute || remaining > 30*time.Minute {
					t.Fatalf("replacement PIN validity = %s, want about 30m", remaining)
				}
				if !identity.ConsumePairingPIN(next) {
					t.Fatal("replacement PIN was rejected after lockout")
				}
			},
		},
		{
			name: "empty PIN never matches a consumed or locked state",
			run: func(t *testing.T, identity *Identity, pin string) {
				if !identity.ConsumePairingPIN(pin) {
					t.Fatal("valid PIN was rejected")
				}
				if identity.VerifyPairingPIN("") || identity.ConsumePairingPIN("") {
					t.Fatal("empty PIN matched the cleared pairing state")
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			identity := tcOpen(t, "pin-orchestrator")
			pin, err := identity.NewPairingPIN()
			if err != nil {
				t.Fatal(err)
			}
			test.run(t, identity, pin)
			if len(identity.TrustedIDs()) != 0 {
				t.Fatal("PIN operations changed the trust roster")
			}
		})
	}
}

func TestPinnedClientsRefuseServerWithDifferentCertificate(t *testing.T) {
	client := tcOpen(t, "pinning-client")
	genuine := tcOpen(t, "pinned-server")
	impostor := tcOpen(t, "pinned-server") // same stable ID, different key pair
	if err := client.TrustPeer(genuine.ID(), genuine.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	if err := impostor.TrustPeer(client.ID(), client.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	server, reached := tcStartTLSServer(t, impostor.ServerTLSConfig(), http.HandlerFunc(tcNoContent))
	address := server.Listener.Addr().String()

	tests := []struct {
		name string
		dial func() error
	}{
		{
			name: "client transport",
			dial: func() error {
				httpClient := &http.Client{Timeout: 3 * time.Second, Transport: client.ClientTransport()}
				response, err := httpClient.Get(server.URL)
				if err == nil {
					_ = response.Body.Close()
				}
				return err
			},
		},
		{
			name: "pinned dial",
			dial: func() error {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				connection, err := client.DialPinned(ctx, "tcp", address, genuine.ID())
				if err == nil {
					_ = connection.Close()
				}
				return err
			},
		},
		{
			name: "pinned client TLS config",
			dial: func() error {
				connection, err := tls.Dial("tcp", address, client.PinnedClientTLSConfig(genuine.ID()))
				if err == nil {
					_ = connection.Close()
				}
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.dial(); err == nil {
				t.Fatal("client accepted a server certificate that does not match its pin")
			}
			if got := reached.Load(); got != 0 {
				t.Fatalf("impostor server handled %d requests", got)
			}
			if got := tcTrackedConnections(client); got != 0 {
				t.Fatalf("failed handshake left %d tracked connections", got)
			}
		})
	}
	t.Cleanup(client.ClientTransport().CloseIdleConnections)
}

func TestPinnedDialRejectsPinnedServerUnderAnotherExpectedID(t *testing.T) {
	client := tcOpen(t, "dial-client")
	serverIdentity := tcOpen(t, "dial-server")
	other := tcOpen(t, "dial-other")
	for _, peer := range []*Identity{serverIdentity, other} {
		if err := client.TrustPeer(peer.ID(), peer.CertificatePEM()); err != nil {
			t.Fatal(err)
		}
	}
	if err := serverIdentity.TrustPeer(client.ID(), client.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	server, _ := tcStartTLSServer(t, serverIdentity.PinnedPeerServerTLSConfig(), http.HandlerFunc(tcNoContent))
	address := server.Listener.Addr().String()

	tests := []struct {
		name       string
		expectedID string
		wantErr    bool
	}{
		{name: "empty expected ID", expectedID: "", wantErr: true},
		{name: "different pinned peer ID", expectedID: other.ID(), wantErr: true},
		{name: "matching pinned peer ID", expectedID: serverIdentity.ID(), wantErr: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			connection, err := client.DialPinned(ctx, "tcp", address, test.expectedID)
			if (err != nil) != test.wantErr {
				t.Fatalf("DialPinned error = %v, wantErr %t", err, test.wantErr)
			}
			if err != nil {
				if got := tcTrackedConnections(client); got != 0 {
					t.Fatalf("rejected dial left %d tracked connections", got)
				}
				return
			}
			if got := tcTrackedConnections(client); got != 1 {
				t.Fatalf("accepted dial tracked %d connections, want 1", got)
			}
			_ = connection.Close()
			if got := tcTrackedConnections(client); got != 0 {
				t.Fatalf("closed dial left %d tracked connections", got)
			}
		})
	}
}

func TestTrustedPeerGatesRejectUnpinnedClientsOverTLS(t *testing.T) {
	serverIdentity := tcOpen(t, "gate-server")
	paired := tcOpen(t, "gate-paired")
	impostor := tcOpen(t, "gate-paired") // same CommonName as the pinned peer
	unpaired := tcOpen(t, "gate-unpaired")
	if err := serverIdentity.TrustPeer(paired.ID(), paired.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	var handlerReached atomic.Int32
	requestServer, _ := tcStartTLSServer(t, serverIdentity.ServerTLSConfig(),
		serverIdentity.RequireTrustedPeer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handlerReached.Add(1)
			tcNoContent(w, r)
		})))
	var gatedReached atomic.Int32
	gatedServer, _ := tcStartTLSServer(t, serverIdentity.PinnedPeerServerTLSConfig(),
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gatedReached.Add(1)
			tcNoContent(w, r)
		}))

	tests := []struct {
		name        string
		certificate *tls.Certificate
		wantStatus  int
	}{
		{name: "no client certificate", wantStatus: http.StatusForbidden},
		{name: "unpaired certificate", certificate: &unpaired.certificate, wantStatus: http.StatusForbidden},
		{name: "same identity with unpinned key", certificate: &impostor.certificate, wantStatus: http.StatusForbidden},
		{name: "pinned certificate", certificate: &paired.certificate, wantStatus: http.StatusNoContent},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}
			if test.certificate != nil {
				config.Certificates = []tls.Certificate{*test.certificate}
			}
			transport := &http.Transport{TLSClientConfig: config, DisableKeepAlives: true}
			defer transport.CloseIdleConnections()
			httpClient := &http.Client{Timeout: 3 * time.Second, Transport: transport}

			beforeHandler := handlerReached.Load()
			response, err := httpClient.Post(requestServer.URL+"/api/tasks", "application/json", nil)
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			if response.StatusCode != test.wantStatus {
				t.Fatalf("RequireTrustedPeer status = %d, want %d", response.StatusCode, test.wantStatus)
			}
			if reached := handlerReached.Load() != beforeHandler; reached != (test.wantStatus == http.StatusNoContent) {
				t.Fatalf("protected handler reached = %t for status %d", reached, response.StatusCode)
			}

			beforeGate := gatedReached.Load()
			response, err = httpClient.Get(gatedServer.URL)
			if test.wantStatus == http.StatusForbidden {
				if err == nil {
					_ = response.Body.Close()
					t.Fatal("pinned-peer TLS server completed a request from an unpinned client")
				}
				if gatedReached.Load() != beforeGate {
					t.Fatal("pinned-peer TLS server handler ran for an unpinned client")
				}
				return
			}
			if err != nil {
				t.Fatalf("pinned client was rejected by pinned-peer TLS server: %v", err)
			}
			_ = response.Body.Close()
		})
	}
}

func TestRevocationClosesConnectionsAndFailsLaterRequests(t *testing.T) {
	serverIdentity := tcOpen(t, "revoke-server")
	client := tcOpen(t, "revoke-client")
	if err := serverIdentity.TrustPeer(client.ID(), client.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	if err := client.TrustPeer(serverIdentity.ID(), serverIdentity.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	var handled atomic.Int32
	server, _ := tcStartTLSServer(t, serverIdentity.ServerTLSConfig(),
		serverIdentity.RequireTrustedPeer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handled.Add(1)
			tcNoContent(w, r)
		})))
	transport := client.ClientTransport()
	t.Cleanup(transport.CloseIdleConnections)
	httpClient := &http.Client{Timeout: 3 * time.Second, Transport: transport}
	get := func() (int, error) {
		response, err := httpClient.Get(server.URL)
		if err != nil {
			return 0, err
		}
		_ = response.Body.Close()
		return response.StatusCode, nil
	}

	if status, err := get(); err != nil || status != http.StatusNoContent {
		t.Fatalf("initial paired request = %d, %v", status, err)
	}

	// Server-side revocation is enforced per request; the existing inbound
	// keep-alive connection is not tracked, but it can no longer reach handlers.
	if err := serverIdentity.RemovePeer(client.ID()); err != nil {
		t.Fatal(err)
	}
	if status, err := get(); err != nil || status != http.StatusForbidden {
		t.Fatalf("request after server revocation = %d, %v; want 403", status, err)
	}
	if got := handled.Load(); got != 1 {
		t.Fatalf("revoked client reached the handler %d times, want 1", got)
	}

	// Client-side unpinning closes every tracked outbound connection.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	dialed, err := client.DialPinned(ctx, "tcp", server.Listener.Addr().String(), serverIdentity.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dialed.Close() }()
	if got := tcTrackedConnections(client); got < 2 {
		t.Fatalf("tracked connections before unpin = %d, want pooled and dialed connections", got)
	}
	if err := client.RemovePeer(serverIdentity.ID()); err != nil {
		t.Fatal(err)
	}
	if got := tcTrackedConnections(client); got != 0 {
		t.Fatalf("unpin left %d tracked connections open", got)
	}
	if _, err := dialed.Write([]byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("write on revoked pinned connection error = %v, want net.ErrClosed", err)
	}
	if _, err := get(); err == nil {
		t.Fatal("request succeeded after the client unpinned the server")
	}
	if got := tcTrackedConnections(client); got != 0 {
		t.Fatalf("failed request left %d tracked connections", got)
	}
	if _, err := client.DialPinned(ctx, "tcp", server.Listener.Addr().String(), serverIdentity.ID()); err == nil {
		t.Fatal("pinned dial succeeded after unpinning")
	}
}

func TestRevocationClosesTrackedInboundConnection(t *testing.T) {
	serverIdentity := tcOpen(t, "inbound-server")
	peer := tcOpen(t, "inbound-peer")
	if err := serverIdentity.TrustPeer(peer.ID(), peer.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	local, remote := net.Pipe()
	defer func() { _ = remote.Close() }()
	certificate := peer.certificate.Leaf

	unpinned := tcOpen(t, "inbound-unpinned")
	if _, err := serverIdentity.TrackPinnedPeerConnection(unpinned.certificate.Leaf, local); err == nil {
		t.Fatal("unpinned certificate connection was tracked")
	}
	if got := tcTrackedConnections(serverIdentity); got != 0 {
		t.Fatalf("rejected tracking left %d connections", got)
	}

	tracked, err := serverIdentity.TrackPinnedPeerConnection(certificate, local)
	if err != nil {
		t.Fatal(err)
	}
	if err := serverIdentity.RemovePeer(peer.ID()); err != nil {
		t.Fatal(err)
	}
	if got := tcTrackedConnections(serverIdentity); got != 0 {
		t.Fatalf("revocation left %d tracked inbound connections", got)
	}
	if _, err := remote.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("remote side read after revocation = %v, want EOF", err)
	}
	if err := tracked.Close(); err != nil {
		t.Fatalf("second close returned %v, want idempotent nil", err)
	}
}

// TestTrustPersistenceFailureCharacterization pins persistence-failure
// behavior. A failed pin write is rolled back so a peer is never trusted in
// memory without also being trusted after a restart. A failed revocation
// write keeps the peer revoked in memory (fail closed), but the durable roster
// still trusts it, so trust returns after a restart.
func TestTrustPersistenceFailureCharacterization(t *testing.T) {
	tests := []struct {
		name          string
		pinBefore     bool
		mutate        func(identity *Identity, peer *Identity) error
		wantLiveTrust bool
		wantDurable   bool
	}{
		{
			name:          "pairing write failure rolls back live trust",
			mutate:        func(identity, peer *Identity) error { return identity.TrustPeer(peer.ID(), peer.CertificatePEM()) },
			wantLiveTrust: false,
			wantDurable:   false,
		},
		{
			name: "detailed pairing write failure rolls back live trust",
			mutate: func(identity, peer *Identity) error {
				return identity.TrustPeerDetails(peer.ID(), peer.CertificatePEM(), "orchestrator", "https://192.0.2.10:7450")
			},
			wantLiveTrust: false,
			wantDurable:   false,
		},
		{
			name:          "pin refresh write failure keeps the previous pin",
			pinBefore:     true,
			mutate:        func(identity, peer *Identity) error { return identity.TrustPeer(peer.ID(), peer.CertificatePEM()) },
			wantLiveTrust: true,
			wantDurable:   true,
		},
		{
			name:          "revocation write failure revokes live trust but trust returns after restart",
			pinBefore:     true,
			mutate:        func(identity, peer *Identity) error { return identity.RemovePeer(peer.ID()) },
			wantLiveTrust: false,
			wantDurable:   true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "identity.json")
			identity, err := Open(path, "persist-node")
			if err != nil {
				t.Fatal(err)
			}
			peer := tcOpen(t, "persist-peer")
			if test.pinBefore {
				if err := identity.TrustPeer(peer.ID(), peer.CertificatePEM()); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			unwritable := filepath.Join(dir, "directory-in-place-of-file")
			if err := os.Mkdir(unwritable, 0o700); err != nil {
				t.Fatal(err)
			}
			identity.mu.Lock()
			identity.path = unwritable
			identity.mu.Unlock()

			if err := test.mutate(identity, peer); err == nil {
				t.Fatal("trust mutation reported success although persistence failed")
			}
			if got := identity.IsTrustedID(peer.ID()); got != test.wantLiveTrust {
				t.Fatalf("live trust = %t, want %t", got, test.wantLiveTrust)
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(after) != string(before) {
				t.Fatal("failed persistence modified the durable identity file")
			}
			var saved identityFile
			if err := json.Unmarshal(after, &saved); err != nil {
				t.Fatal(err)
			}
			if _, durable := saved.Trusted[peer.ID()]; durable != test.wantDurable {
				t.Fatalf("durable trust = %t, want %t", durable, test.wantDurable)
			}
			reloaded, err := Open(path, identity.ID())
			if err != nil {
				t.Fatal(err)
			}
			if got := reloaded.IsTrustedID(peer.ID()); got != test.wantDurable {
				t.Fatalf("trust after restart = %t, want %t", got, test.wantDurable)
			}
		})
	}
}
