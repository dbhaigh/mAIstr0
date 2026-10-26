package orchestrator

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/shard"
)

func TestRouterShardGroupAcquisitionAndHeadVerification(t *testing.T) {
	saved := shard.ModelProfile{Name: "cluster-model", Architecture: "llama", LayerCount: 32, RequestedContext: 4096}
	for _, test := range []struct {
		name          string
		removeGate    bool
		profileStatus int
		profileName   string
		wantStatus    int
		wantBody      string
		wantProxied   int32
	}{
		// The group passes the router's readiness check, but its request gate
		// disappears before acquisition, as during a concurrent stop.
		{name: "acquisition fails after readiness check", removeGate: true, profileStatus: http.StatusInternalServerError,
			wantStatus: http.StatusServiceUnavailable, wantBody: "unavailable"},
		{name: "head serves a different model", profileStatus: http.StatusOK, profileName: "other-model",
			wantStatus: http.StatusServiceUnavailable, wantBody: "does not match saved group"},
		{name: "head profile unreadable", profileStatus: http.StatusInternalServerError,
			wantStatus: http.StatusServiceUnavailable, wantBody: "does not match saved group"},
		{name: "head serves the saved model", profileStatus: http.StatusOK, profileName: saved.Name,
			wantStatus: http.StatusOK, wantBody: "data: routed", wantProxied: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			var proxied, profiles atomic.Int32
			head := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/engines/managed/maistr0-auto-head/profile":
					profiles.Add(1)
					if test.profileStatus != http.StatusOK {
						http.Error(w, "profile unavailable", test.profileStatus)
						return
					}
					profile := saved
					profile.Name = test.profileName
					_ = json.NewEncoder(w).Encode(profile)
				case "/api/engines/managed/maistr0-auto-head/proxy/v1/chat/completions":
					proxied.Add(1)
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: routed\n\n")
				default:
					http.NotFound(w, r)
				}
			}))
			defer head.Close()

			srv := newTestServer(t)
			group := addTaskTestShardGroup(srv, head.URL)
			srv.shards.mu.Lock()
			group.Model = saved
			group.Members[0].EngineID = "maistr0-auto-head"
			srv.shards.groups[group.ID] = group
			if test.removeGate {
				delete(srv.shards.requestGates, group.ID)
			}
			srv.shards.mu.Unlock()

			response := httptest.NewRecorder()
			srv.Mux().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/router/v1/chat/completions",
				strings.NewReader(`{"model":"cluster-model","messages":[{"role":"user","content":"hello"}]}`)))

			if response.Code != test.wantStatus || !strings.Contains(response.Body.String(), test.wantBody) {
				t.Fatalf("router response = %d %q, want %d containing %q", response.Code, response.Body.String(), test.wantStatus, test.wantBody)
			}
			if got := proxied.Load(); got != test.wantProxied {
				t.Fatalf("proxied requests = %d, want %d", got, test.wantProxied)
			}
			if test.removeGate {
				if profiles.Load() != 0 {
					t.Fatal("verified a head without holding its request slot")
				}
				return
			}
			if profiles.Load() != 1 {
				t.Fatalf("head verifications = %d, want 1 per routed request", profiles.Load())
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			release, err := srv.shards.AcquireRequest(ctx, group.ID)
			if err != nil {
				t.Fatalf("request slot leaked: %v", err)
			}
			release()
		})
	}
}
