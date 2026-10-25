package engine

import (
	"net/http"
	"testing"
	"time"
)

func TestEngineClientProfilesPreserveLegacyTimeouts(t *testing.T) {
	ollama := NewOllama("http://localhost:11434")
	vllm := NewVLLM("http://localhost:8000")
	stage, err := NewHTTPPipelineStageEngine("stage", "http://localhost:8001", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name   string
		client *http.Client
		want   time.Duration
	}{
		{"shared discovery", httpClient, 5 * time.Second},
		{"ollama probe", ollama.client, 3 * time.Second},
		{"ollama inference", ollama.genClient, 10 * time.Minute},
		{"ollama download", ollama.pullClient, time.Hour},
		{"vllm probe", vllm.probeClient, 5 * time.Second},
		{"vllm inference", vllm.client, 10 * time.Minute},
		{"stage probe", stage.capabilityClient, 5 * time.Second},
		{"stage inference", stage.client, 10 * time.Minute},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.client.Timeout != tt.want {
				t.Fatalf("timeout = %s, want legacy %s", tt.client.Timeout, tt.want)
			}
			if tt.client.Transport != http.DefaultTransport {
				t.Fatal("client does not share the default transport")
			}
		})
	}
}

func TestPipelineStagePreservesInjectedClient(t *testing.T) {
	for _, timeout := range []time.Duration{0, 23 * time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			transport := &http.Transport{DisableKeepAlives: true}
			defer transport.CloseIdleConnections()
			client := &http.Client{Timeout: timeout, Transport: transport}
			stage, err := NewHTTPPipelineStageEngine("stage", "http://localhost:8001", client)
			if err != nil {
				t.Fatal(err)
			}
			if stage.client != client || stage.client.Transport != transport || stage.client.Timeout != timeout {
				t.Fatal("injected client was replaced or modified")
			}
		})
	}
}
