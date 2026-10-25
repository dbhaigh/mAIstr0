package nodeagent

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/nodeagent/supervisor"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

func TestRPCRelayRejectsClientWithoutPinnedCertificate(t *testing.T) {
	backendListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backendListener.Close()
	backendAccepted := make(chan struct{}, 1)
	go func() {
		connection, err := backendListener.Accept()
		if err == nil {
			backendAccepted <- struct{}{}
			_ = connection.Close()
		}
	}()
	identity := openRelayIdentity(t, "rpc-worker")
	relay, err := newRPCRelayManager(identity, identity.ID(), "127.0.0.1:0", "https://127.0.0.1:7451")
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	endpoint, err := relay.StartWorker(context.Background(), "rpc-worker", backendListener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	connection, err := tls.DialWithDialer(
		&net.Dialer{Timeout: time.Second},
		"tcp", endpoint,
		&tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS13},
	)
	if err == nil {
		defer connection.Close()
		_ = connection.SetDeadline(time.Now().Add(time.Second))
		_, _ = connection.Write([]byte("must-not-forward"))
		response := make([]byte, len("must-not-forward"))
		if _, readErr := io.ReadFull(connection, response); readErr == nil {
			t.Fatalf("relay returned backend data to an unpinned client: %q", response)
		}
	}
	select {
	case <-backendAccepted:
		t.Fatal("relay forwarded a connection without a pinned client certificate")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestRPCRelaySurvivesWorkerBackendRestart(t *testing.T) {
	backendAddress, closeBackend := startRelayEchoBackend(t, "127.0.0.1:0")
	defer closeBackend()
	workerIdentity := openRelayIdentity(t, "rpc-worker")
	headIdentity := openRelayIdentity(t, "rpc-head")
	if err := workerIdentity.TrustPeer(headIdentity.ID(), headIdentity.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	if err := headIdentity.TrustPeer(workerIdentity.ID(), workerIdentity.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	workerRelay, err := newRPCRelayManager(workerIdentity, workerIdentity.ID(), "127.0.0.1:0", "https://127.0.0.1:7451")
	if err != nil {
		t.Fatal(err)
	}
	defer workerRelay.Close()
	relayAddress, err := workerRelay.StartWorker(context.Background(), "rpc-worker-engine", backendAddress)
	if err != nil {
		t.Fatal(err)
	}
	headRelay, err := newRPCRelayManager(headIdentity, headIdentity.ID(), "127.0.0.1:0", "https://127.0.0.1:7451")
	if err != nil {
		t.Fatal(err)
	}
	defer headRelay.Close()
	localEndpoints, err := headRelay.StartHead(context.Background(), "llama-head", []supervisor.RPCRelayTarget{
		{PeerID: workerIdentity.ID(), Address: relayAddress},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(localEndpoints) != 1 {
		t.Fatalf("local relay endpoints = %v, want exactly one", localEndpoints)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	exchangeRelayPayload(t, ctx, localEndpoints[0], []byte("before-restart"))
	closeBackend()

	_, closeBackend, err = startRelayEchoBackendAt(t, backendAddress)
	if err != nil {
		t.Fatalf("restart RPC backend on %s: %v", backendAddress, err)
	}
	defer closeBackend()
	exchangeRelayPayload(t, ctx, localEndpoints[0], []byte("after-restart"))
}

func BenchmarkRPCRelayOverhead(b *testing.B) {
	backend, closeBackend := startRelayEchoBackend(b, "127.0.0.1:0")
	defer closeBackend()
	workerIdentity := openRelayIdentity(b, "rpc-worker-bench")
	headIdentity := openRelayIdentity(b, "rpc-head-bench")
	if err := workerIdentity.TrustPeer(headIdentity.ID(), headIdentity.CertificatePEM()); err != nil {
		b.Fatal(err)
	}
	if err := headIdentity.TrustPeer(workerIdentity.ID(), workerIdentity.CertificatePEM()); err != nil {
		b.Fatal(err)
	}
	workerRelay, err := newRPCRelayManager(workerIdentity, workerIdentity.ID(), "127.0.0.1:0", "https://127.0.0.1:7451")
	if err != nil {
		b.Fatal(err)
	}
	defer workerRelay.Close()
	relayAddress, err := workerRelay.StartWorker(context.Background(), "rpc-worker", backend)
	if err != nil {
		b.Fatal(err)
	}
	headRelay, err := newRPCRelayManager(headIdentity, headIdentity.ID(), "127.0.0.1:0", "https://127.0.0.1:7451")
	if err != nil {
		b.Fatal(err)
	}
	defer headRelay.Close()
	localEndpoints, err := headRelay.StartHead(context.Background(), "llama-head", []supervisor.RPCRelayTarget{
		{PeerID: workerIdentity.ID(), Address: relayAddress},
	})
	if err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, 64<<10)
	for index := range payload {
		payload[index] = byte(index)
	}
	b.Run("direct", func(b *testing.B) {
		benchmarkRelayRoundTrip(b, backend, payload)
	})
	b.Run("mtls-relay", func(b *testing.B) {
		benchmarkRelayRoundTrip(b, localEndpoints[0], payload)
	})
}

func benchmarkRelayRoundTrip(b *testing.B, address string, payload []byte) {
	b.Helper()
	connection, err := net.Dial("tcp", address)
	if err != nil {
		b.Fatal(err)
	}
	defer connection.Close()
	buffer := make([]byte, len(payload))
	b.SetBytes(int64(len(payload) * 2))
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := writeRelayPayload(connection, payload); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(connection, buffer); err != nil {
			b.Fatal(err)
		}
	}
}

func exchangeRelayPayload(t testing.TB, ctx context.Context, endpoint string, payload []byte) {
	t.Helper()
	connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
	if err := writeRelayPayload(connection, payload); err != nil {
		t.Fatal(err)
	}
	response := make([]byte, len(payload))
	if _, err := io.ReadFull(connection, response); err != nil {
		t.Fatal(err)
	}
	if string(response) != string(payload) {
		t.Fatalf("relay response = %q, want %q", response, payload)
	}
}

func writeRelayPayload(connection net.Conn, payload []byte) error {
	for len(payload) > 0 {
		written, err := connection.Write(payload)
		if err != nil {
			return err
		}
		payload = payload[written:]
	}
	return nil
}

func startRelayEchoBackend(tb testing.TB, address string) (string, func()) {
	tb.Helper()
	endpoint, closeServer, err := startRelayEchoBackendAt(tb, address)
	if err != nil {
		tb.Fatal(err)
	}
	return endpoint, closeServer
}

func startRelayEchoBackendAt(tb testing.TB, address string) (string, func(), error) {
	tb.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return "", nil, err
	}
	done := make(chan struct{})
	var connections sync.WaitGroup
	go func() {
		defer close(done)
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Add(1)
			go func() {
				defer connections.Done()
				defer connection.Close()
				_, _ = io.Copy(connection, connection)
			}()
		}
	}()
	return listener.Addr().String(), func() {
		_ = listener.Close()
		<-done
		connections.Wait()
	}, nil
}

func openRelayIdentity(tb testing.TB, id string) *peeridentity.Identity {
	tb.Helper()
	identity, err := peeridentity.Open(filepath.Join(tb.TempDir(), "identity.json"), id)
	if err != nil {
		tb.Fatal(err)
	}
	return identity
}

func TestRPCRelayRejectsUntrustedPinnedPeer(t *testing.T) {
	backend, closeBackend := startRelayEchoBackend(t, "127.0.0.1:0")
	defer closeBackend()
	workerIdentity := openRelayIdentity(t, "rpc-worker")
	headIdentity := openRelayIdentity(t, "rpc-head")
	if err := workerIdentity.TrustPeer(headIdentity.ID(), headIdentity.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	workerRelay, err := newRPCRelayManager(workerIdentity, workerIdentity.ID(), "127.0.0.1:0", "https://127.0.0.1:7451")
	if err != nil {
		t.Fatal(err)
	}
	defer workerRelay.Close()
	relayAddress, err := workerRelay.StartWorker(context.Background(), "rpc-worker", backend)
	if err != nil {
		t.Fatal(err)
	}

	_, err = headIdentity.DialPinned(context.Background(), "tcp", relayAddress, workerIdentity.ID())
	if err == nil {
		t.Fatal("client connected to a relay whose server certificate was not pinned by the client")
	}
}

func TestRPCRelayClosesActiveSessionWhenClientPinIsRevoked(t *testing.T) {
	backend, closeBackend := startRelayEchoBackend(t, "127.0.0.1:0")
	defer closeBackend()
	workerIdentity := openRelayIdentity(t, "rpc-worker")
	headIdentity := openRelayIdentity(t, "rpc-head")
	if err := workerIdentity.TrustPeer(headIdentity.ID(), headIdentity.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	if err := headIdentity.TrustPeer(workerIdentity.ID(), workerIdentity.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	workerRelay, err := newRPCRelayManager(workerIdentity, workerIdentity.ID(), "127.0.0.1:0", "https://127.0.0.1:7451")
	if err != nil {
		t.Fatal(err)
	}
	defer workerRelay.Close()
	endpoint, err := workerRelay.StartWorker(context.Background(), "rpc-worker-engine", backend)
	if err != nil {
		t.Fatal(err)
	}

	connection, err := headIdentity.DialPinned(context.Background(), "tcp", endpoint, workerIdentity.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
	payload := []byte("pinned-session")
	if err := writeRelayPayload(connection, payload); err != nil {
		t.Fatal(err)
	}
	response := make([]byte, len(payload))
	if _, err := io.ReadFull(connection, response); err != nil {
		t.Fatal(err)
	}
	if err := workerIdentity.RemovePeer(headIdentity.ID()); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Read(make([]byte, 1)); err == nil {
		t.Fatal("relay session remained open after the client pin was revoked")
	}
}
