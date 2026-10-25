package orchestrator

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestManagedStartAllowsSlowModelLoadWithoutExtendingStatusRequests(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			_, _ = w.Write([]byte(`{"id":"head","kind":"llama-server","state":"ready","restart_count":0}`))
		} else {
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	defer target.Close()
	client := newNodeClient(nil, nil, nil)
	client.managed.Timeout = 10 * time.Millisecond
	status, err := client.Start(context.Background(), target.URL, "head", nil)
	if err != nil || status.State != "ready" {
		t.Fatalf("slow managed start=%+v err=%v", status, err)
	}
	if client.managed.Timeout != 10*time.Millisecond {
		t.Fatal("start mutated shared HTTP client")
	}
	if _, err := client.List(context.Background(), target.URL); err == nil {
		t.Fatal("status request lost its short timeout")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Start(ctx, target.URL, "head", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("start cancellation=%v", err)
	}
}
