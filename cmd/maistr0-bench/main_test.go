package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/benchrun"
)

func TestCLIReportAndExclusiveOutput(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"synthetic"}}]}`))
	}))
	defer server.Close()
	cfg := benchrun.Config{TimeoutMS: 1000,
		Targets: []benchrun.Target{{Name: "direct", URL: server.URL, Nodes: 1, Engine: "fake", Hardware: []string{"fake"}, Settings: "fixed"}},
		Workloads: []benchrun.Workload{{Name: "smoke", Model: "fake", ModelRevision: "fixture-v1", PromptWords: 4,
			OutputTokens: 4, Concurrency: 1, Requests: 2, Warmup: 1, LoadState: "warm"}}}
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	configPath, outputPath := filepath.Join(t.TempDir(), "config.json"), filepath.Join(t.TempDir(), "results.json")
	if err := os.WriteFile(configPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := run(configPath, outputPath); err != nil {
		t.Fatal(err)
	}
	report, err := os.ReadFile(outputPath)
	if err != nil || !json.Valid(report) {
		t.Fatalf("report: %v", err)
	}
	if err := run(configPath, outputPath); err == nil {
		t.Fatal("existing measurements were overwritten")
	}
	after, err := os.ReadFile(outputPath)
	if err != nil || string(after) != string(report) {
		t.Fatal("original measurements changed")
	}
}

func TestCLIRejectsUnknownFieldsAndMissingCredentials(t *testing.T) {
	const valid = `{"timeout_ms":1000,"targets":[{"name":"direct","url":"http://127.0.0.1:1","nodes":1,"engine":"fake","hardware":["fake"],"settings":"fixed","bearer_token_env":"MAISTR0_BENCH_TEST_EMPTY"}],"workloads":[{"name":"test","model":"fake","model_revision":"v1","prompt_words":1,"output_tokens":1,"concurrency":1,"requests":1,"load_state":"warm"}]}`
	t.Setenv("MAISTR0_BENCH_TEST_EMPTY", "")
	for _, tc := range []struct{ name, input string }{
		{"unknown", `{"unknown":true}`},
		{"missing-credentials", valid},
		{"oversized", valid + strings.Repeat(" ", 1<<20)},
		{"multiple-objects", valid + `{}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "config.json")
			outputPath := filepath.Join(t.TempDir(), "results.json")
			if err := os.WriteFile(configPath, []byte(tc.input), 0600); err != nil {
				t.Fatal(err)
			}
			if err := run(configPath, outputPath); err == nil {
				t.Fatal("invalid configuration accepted")
			}
			if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
				t.Fatal("invalid configuration created a success-shaped report")
			}
		})
	}
}
