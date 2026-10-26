package peeridentity

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
)

func TestDualShutdownJoinsCanceledActiveHandler(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, canceled, finish := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var finishOnce sync.Once
	t.Cleanup(func() { finishOnce.Do(func() { close(finish) }) })
	handler := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(canceled)
		<-finish
	})
	served := make(chan error, 1)
	go func() {
		served <- ServeDualListenerContext(ctx, listener, handler, handler, &tls.Config{})
	}()
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		response, err := http.Get("http://" + listener.Addr().String())
		if err == nil {
			_ = response.Body.Close()
		}
	}()
	<-started
	cancel()
	<-canceled
	select {
	case err := <-served:
		t.Errorf("server returned before handler cleanup: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	finishOnce.Do(func() { close(finish) })
	select {
	case err := <-served:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not join canceled handler")
	}
	<-clientDone
}

func TestSplitCloseUnblocksAndJoinsUnclassifiedConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	split := newSplitListener(listener)
	inbound, outbound := net.Pipe()
	defer closeutil.Discard(outbound)
	if !split.startRoute(inbound) {
		t.Fatal("connection was not accepted")
	}
	if err := split.Close(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { split.routes.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("unclassified reader survived Close")
	}
	split.routeMu.Lock()
	defer split.routeMu.Unlock()
	if len(split.pending) != 0 {
		t.Fatalf("shutdown retained %d connections", len(split.pending))
	}
}

func BenchmarkSplitClassification(b *testing.B) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	split := newSplitListener(listener)
	defer closeutil.Discard(split)
	b.ReportAllocs()
	for b.Loop() {
		inbound, outbound := net.Pipe()
		go func() {
			_, _ = outbound.Write([]byte("G"))
			_ = outbound.Close()
		}()
		split.startRoute(inbound)
		connection := <-split.plain
		_ = connection.Close()
	}
	split.routes.Wait()
}
