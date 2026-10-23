package orchestrator

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/httpx"
	"github.com/maistr0/maistr0/internal/inferroute"
)

func TestNodeClientPropagatesRequestIDsFromContextAndAttemptHeaders(t *testing.T) {
	type observedHeaders struct {
		standard string
		legacy   string
	}
	seen := make(chan observedHeaders, 3)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- observedHeaders{
			standard: r.Header.Get(httpx.RequestIDHeader),
			legacy:   r.Header.Get(inferroute.HeaderRequestID),
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	client := newNodeClient(nil, nil, nil)
	for _, test := range []struct {
		name       string
		incomingID string
		attemptID  string
		wantID     string
	}{
		{name: "context request ID", incomingID: "client-trace", wantID: "client-trace"},
		{name: "attempt ID is preserved", incomingID: "client-trace", attemptID: "task-1:subtask-2", wantID: "task-1:subtask-2"},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := httpx.RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, target.URL, nil)
				if err != nil {
					t.Errorf("create outgoing request: %v", err)
					return
				}
				if test.attemptID != "" {
					request.Header.Set(httpx.RequestIDHeader, test.attemptID)
					request.Header.Set(inferroute.HeaderRequestID, test.attemptID)
				}
				response, err := client.DoDispatch(request)
				if err != nil {
					t.Errorf("send outgoing request: %v", err)
					return
				}
				_ = response.Body.Close()
				w.WriteHeader(http.StatusNoContent)
			}))
			incoming := httptest.NewRequest(http.MethodGet, "/", nil)
			incoming.Header.Set(httpx.RequestIDHeader, test.incomingID)
			handler.ServeHTTP(httptest.NewRecorder(), incoming)

			var got observedHeaders
			select {
			case got = <-seen:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for forwarded request")
			}
			if got.standard != test.wantID || got.legacy != test.wantID {
				t.Fatalf("forwarded request IDs = %+v, want %q in both headers", got, test.wantID)
			}
		})
	}

	request, err := http.NewRequest(http.MethodGet, target.URL, nil)
	if err != nil {
		t.Fatalf("create background request: %v", err)
	}
	response, err := client.DoDispatch(request)
	if err != nil {
		t.Fatalf("send background request: %v", err)
	}
	_ = response.Body.Close()
	select {
	case got := <-seen:
		if got.standard == "" || got.standard != got.legacy {
			t.Fatalf("background request IDs = %+v, want a generated shared ID", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for background request ID")
	}
}
