package peeridentity

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestServeDualContextCancelsRequestsAndJoinsListeners(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handlerDone := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(handlerDone)
	})
	done := make(chan error, 1)
	go func() {
		done <- ServeDualListenerContext(ctx, listener, handler, handler, nil)
	}()
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("http://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	cancel()
	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP request context was not canceled")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dual HTTP listener did not stop")
	}
}
