package nodeagent

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/config"
)

func TestPairWithAddressContextCancelsInFlightRequest(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		select {
		case <-r.Context().Done():
			// Returning normally would send an implicit empty 200 that can race
			// the client's cancellation and be decoded as a reply.
			panic(http.ErrAbortHandler)
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "pairing-context-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("close agent: %v", err)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- agent.orchestratorLink.pairWithAddressContext(ctx, server.URL, "", "123456")
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("pairing request did not reach server")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("pairing error = %v, want context cancellation", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pairing request did not stop after cancellation")
	}
	if agent.getOrchestratorAddr() != "" {
		t.Fatal("cancelled pairing changed the orchestrator address")
	}
}
