package nodeagent

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/engine"
	"github.com/maistr0/maistr0/internal/inferroute"
)

func TestTransparentProxyPreservesProviderRequestAndStripsInternalHeaders(t *testing.T) {
	const providerBody = `{"model":"demo","messages":[{"role":"user","content":"hello"}],"stream":true}`
	var requestPath, authorization string
	var gotBody string
	var internalHeadersPresent bool
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/tags" {
			_, _ = io.WriteString(w, `{"models":[{"name":"demo"}]}`)
			return
		}
		requestPath = r.URL.Path
		authorization = r.Header.Get("Authorization")
		internalHeadersPresent = r.Header.Get(inferroute.HeaderEngine) != "" ||
			r.Header.Get(inferroute.HeaderRequestID) != "" ||
			r.Header.Get(inferroute.HeaderAttemptID) != ""
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read provider request: %v", err)
		}
		gotBody = string(body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"response","choices":[]}`)
	}))
	defer provider.Close()

	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "proxy-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("close test agent: %v", err)
		}
	})
	agent.engines = []engine.Engine{engine.NewOllama(provider.URL)}

	req := httptest.NewRequest(http.MethodPost, "/proxy/v1/chat/completions", strings.NewReader(providerBody))
	req.SetPathValue("path", "v1/chat/completions")
	req.Header.Set("Authorization", "Bearer provider-key")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(inferroute.HeaderEngine, "ollama")
	req.Header.Set(inferroute.HeaderRequestID, "request-id")
	req.Header.Set(inferroute.HeaderAttemptID, "attempt-id")
	rec := httptest.NewRecorder()
	agent.handleTransparentProxy(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("proxy response = %d: %s", rec.Code, rec.Body.String())
	}
	if requestPath != "/v1/chat/completions" {
		t.Fatalf("provider path = %q", requestPath)
	}
	if authorization != "Bearer provider-key" {
		t.Fatalf("authorization = %q", authorization)
	}
	if internalHeadersPresent {
		t.Fatal("internal routing headers leaked to provider")
	}
	if gotBody != providerBody {
		t.Fatalf("provider body changed: %s", gotBody)
	}
}

func TestPreserveForwardingHeadersMatchesDirectorBehavior(t *testing.T) {
	tests := []struct {
		name       string
		remoteAddr string
		in         http.Header
		want       http.Header
	}{
		{
			name:       "appends client IP",
			remoteAddr: "192.0.2.7:4100",
			in:         http.Header{},
			want:       http.Header{"X-Forwarded-For": {"192.0.2.7"}},
		},
		{
			name:       "keeps client forwarding headers",
			remoteAddr: "192.0.2.7:4100",
			in: http.Header{
				"Forwarded":         {"for=198.51.100.1"},
				"X-Forwarded-For":   {"198.51.100.1", "203.0.113.5"},
				"X-Forwarded-Host":  {"api.example"},
				"X-Forwarded-Proto": {"https"},
			},
			want: http.Header{
				"Forwarded":         {"for=198.51.100.1"},
				"X-Forwarded-For":   {"198.51.100.1, 203.0.113.5, 192.0.2.7"},
				"X-Forwarded-Host":  {"api.example"},
				"X-Forwarded-Proto": {"https"},
			},
		},
		{
			name:       "unparseable remote address passes headers through",
			remoteAddr: "pipe",
			in:         http.Header{"X-Forwarded-For": {"198.51.100.1"}},
			want:       http.Header{"X-Forwarded-For": {"198.51.100.1"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			in := httptest.NewRequest(http.MethodPost, "/proxy/v1/chat/completions", nil)
			in.RemoteAddr = test.remoteAddr
			in.Header = test.in.Clone()
			pr := &httputil.ProxyRequest{In: in, Out: in.Clone(in.Context())}
			pr.Out.Header = http.Header{}

			preserveForwardingHeaders(pr)

			if !reflect.DeepEqual(pr.Out.Header, test.want) {
				t.Fatalf("outbound headers = %v, want %v", pr.Out.Header, test.want)
			}
			if !reflect.DeepEqual(in.Header, test.in) {
				t.Fatalf("inbound headers changed to %v", in.Header)
			}
		})
	}
}
