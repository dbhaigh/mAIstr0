package orchestrator

import (
	"testing"

	"github.com/maistr0/maistr0/internal/agent"
)

func TestServerExecutionLocationReportsLoopbackAndRemoteHosts(t *testing.T) {
	tests := []struct {
		url      string
		expected agent.ExecutionLocation
	}{
		{url: "http://127.0.0.1:8000", expected: agent.ExecutionLocal},
		{url: "https://[::1]:8000", expected: agent.ExecutionLocal},
		{url: "https://localhost:8000", expected: agent.ExecutionLocal},
		{url: "https://agent.example.test", expected: agent.ExecutionRemote},
		{url: "", expected: agent.ExecutionUnknown},
		{url: "invalid url", expected: agent.ExecutionUnknown},
	}
	for _, test := range tests {
		t.Run(test.url, func(t *testing.T) {
			if actual := serverExecutionLocation(test.url); actual != test.expected {
				t.Fatalf("serverExecutionLocation(%q) = %q, want %q", test.url, actual, test.expected)
			}
		})
	}
}
