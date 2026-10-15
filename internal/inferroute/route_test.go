package inferroute

import (
	"net/http"
	"testing"
)

func TestLookupTransparentInferenceRoutes(t *testing.T) {
	tests := []struct {
		method   string
		path     string
		protocol Protocol
		ok       bool
	}{
		{http.MethodPost, "/v1/chat/completions", ProtocolOpenAI, true},
		{http.MethodPost, "/v1/completions", ProtocolOpenAI, true},
		{http.MethodPost, "/v1/messages", ProtocolAnthropic, true},
		{http.MethodPost, "/api/chat", ProtocolOllama, true},
		{http.MethodPost, "/api/generate", ProtocolOllama, true},
		{http.MethodGet, "/v1/models", "", false},
		{http.MethodPost, "/admin", "", false},
		{http.MethodPost, "/v1/chat/completions/extra", "", false},
	}
	for _, test := range tests {
		t.Run(test.method+" "+test.path, func(t *testing.T) {
			route, ok := Lookup(test.method, test.path)
			if ok != test.ok || route.Protocol != test.protocol {
				t.Fatalf("Lookup(%q, %q) = (%+v, %t), want protocol %q, ok %t",
					test.method, test.path, route, ok, test.protocol, test.ok)
			}
		})
	}
}
