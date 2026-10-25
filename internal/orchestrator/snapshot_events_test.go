package orchestrator

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/hub"
)

func TestSnapshotPublishSkipsIdleBuild(t *testing.T) {
	// No other dependencies are supplied: an idle publish must not read them.
	srv := &Server{events: hub.New()}
	srv.publishSnapshot()
	if srv.publisher != nil {
		t.Fatal("idle publish started a publisher")
	}
}

func TestRunFailureStopsBackgroundWorkers(t *testing.T) {
	srv := newTestServer(t)
	srv.SetDiscoveryEnabled(false)
	if err := srv.SetAdvertiseAddress("https://127.0.0.1:7450"); err != nil {
		t.Fatal(err)
	}
	if err := srv.Run("invalid listen address"); err == nil {
		t.Fatal("invalid listener unexpectedly started")
	}
	if err := srv.background.Context().Err(); err != context.Canceled {
		t.Fatalf("background context after listener failure = %v", err)
	}
}

func TestSnapshotEventsMatchExistingPayloadContract(t *testing.T) {
	srv := newTestServer(t)
	wantBytes, err := json.Marshal(srv.snapshot())
	if err != nil {
		t.Fatal(err)
	}
	var want map[string]json.RawMessage
	if err := json.Unmarshal(wantBytes, &want); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(srv.Mux())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/api/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("SSE response = %d, headers = %v", response.StatusCode, response.Header)
	}
	reader := bufio.NewReader(response.Body)
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	payload, ok := strings.CutPrefix(line, "data: ")
	if !ok {
		t.Fatalf("event framing = %q", line)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal([]byte(payload), &got); err != nil {
		t.Fatal(err)
	}
	for _, snapshot := range []map[string]json.RawMessage{want, got} {
		var dispatch map[string]json.RawMessage
		if err := json.Unmarshal(snapshot["dispatch"], &dispatch); err != nil {
			t.Fatal(err)
		}
		var goroutines int
		if err := json.Unmarshal(dispatch["goroutines"], &goroutines); err != nil || goroutines <= 0 {
			t.Fatalf("invalid goroutine metric: %s (%v)", dispatch["goroutines"], err)
		}
		// Opening the SSE connection necessarily starts additional goroutines.
		dispatch["goroutines"] = json.RawMessage("0")
		snapshot["dispatch"], err = json.Marshal(dispatch)
		if err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot payload changed:\ngot: %s\nwant: %s", payload, wantBytes)
	}
	if terminator, err := reader.ReadString('\n'); err != nil || terminator != "\n" {
		t.Fatalf("event terminator = %q, error = %v", terminator, err)
	}
}
