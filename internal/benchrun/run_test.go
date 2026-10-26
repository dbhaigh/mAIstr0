package benchrun

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestQuantileSampleThresholds(t *testing.T) {
	for _, count := range []int{0, 1, 19, 20, 99, 100, 999, 1000} {
		values := make([]float64, count)
		for i := range values {
			values[i] = float64(i + 1)
		}
		got := Summarize(values)
		if got.Samples != count || (got.MeanMS != nil) != (count > 0) ||
			(got.P50MS != nil) != (count >= 20) || (got.P95MS != nil) != (count >= 100) ||
			(got.P99MS != nil) != (count >= 1000) {
			t.Fatalf("count %d: %+v", count, got)
		}
		if count == 1000 && (*got.P50MS != 500 || *got.P95MS != 950 || *got.P99MS != 990) {
			t.Fatalf("nearest ranks = %+v", got)
		}
	}
}

func TestHTTPMeasurementsValidateContentAndKeepSecretsOut(t *testing.T) {
	for _, test := range []struct {
		name, body string
		stream     bool
		status     int
		outcome    string
	}{
		{"json", `{"choices":[{"message":{"content":"private-output"}}]}`, false, 200, "completed"},
		{"stream", "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n" +
			"data: {\"choices\":[{\"delta\":{\"content\":\"private-output\"}}]}\n\n" +
			"data: [DONE]\n\n", true, 200, "completed"},
		{"role is not a token", "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\ndata: [DONE]\n\n", true, 200, "invalid_or_truncated_response"},
		{"missing terminal", "data: {\"choices\":[{\"delta\":{\"content\":\"private-output\"}}]}\n\n", true, 200, "invalid_or_truncated_response"},
		{"stream error after token", "data: {\"choices\":[{\"delta\":{\"content\":\"private-output\"}}]}\n\ndata: {\"error\":\"private-output\"}\n\ndata: [DONE]\n\n", true, 200, "invalid_or_truncated_response"},
		{"malformed stream frame", "data: {\"choices\":[{\"delta\":{\"content\":\"private-output\"}}]}\n\ndata: invalid\n\ndata: [DONE]\n\n", true, 200, "invalid_or_truncated_response"},
		{"bad json", `{"error":"private-output"}`, false, 200, "invalid_or_truncated_response"},
		{"upstream error", "private-output", false, 503, "http_error"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer private-token" {
					t.Error("bearer token not sent")
				}
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			cfg := Config{TimeoutMS: 1000, Targets: []Target{{Name: "test", URL: server.URL, Nodes: 1,
				Engine: "synthetic", Hardware: []string{"fake"}, Settings: "fixed", Token: "private-token"}},
				Workloads: []Workload{{Name: "case", Model: "synthetic", ModelRevision: "fixture",
					PromptWords: 4, OutputTokens: 4, Concurrency: 2, Requests: 4, Stream: test.stream, LoadState: "uncontrolled"}}}
			report, err := Run(context.Background(), server.Client(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			result := report.Results[0]
			if len(result.Samples) != 4 {
				t.Fatalf("sample count: %d", len(result.Samples))
			}
			for _, sample := range result.Samples {
				if sample.Outcome != test.outcome || sample.ElapsedMS < 0 ||
					(sample.FirstTokenMS != nil) != (test.stream && strings.Contains(test.body, `"content"`)) {
					t.Errorf("sample = %+v", sample)
				}
			}
			data, err := json.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			for _, secret := range []string{server.URL, "private-token", "private-output", `"content"`} {
				if strings.Contains(string(data), secret) {
					t.Errorf("report leaked %q", secret)
				}
			}
			if !test.stream && result.FirstToken.Samples != 0 {
				t.Fatal("non-streaming response fabricated token latency")
			}
		})
	}
}

func TestCancellationAndResponseBounds(t *testing.T) {
	for _, test := range []struct {
		name   string
		cancel bool
	}{
		{"cancelled", true}, {"oversized", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if test.cancel {
					<-r.Context().Done()
					return
				}
				_, _ = io.WriteString(w, strings.Repeat("x", maxResponseBytes+2))
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if test.cancel {
				cancel()
			}
			sample := request(ctx, server.Client(), Target{URL: server.URL}, []byte(`{}`), false, time.Second)
			want := "invalid_or_truncated_response"
			if test.cancel {
				want = "cancelled"
			}
			if sample.Outcome != want || sample.Bytes > maxResponseBytes+1 {
				t.Fatalf("sample = %+v", sample)
			}
		})
	}
}

func TestCancellationAfterUpstreamAcceptsJoinsRequest(t *testing.T) {
	accepted, released := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return
		}
		close(accepted)
		<-r.Context().Done()
		close(released)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan Sample, 1)
	go func() {
		done <- request(ctx, server.Client(), Target{URL: server.URL}, []byte(`{}`), false, time.Second)
	}()
	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("upstream was not reached")
	}
	cancel()
	select {
	case sample := <-done:
		if sample.Outcome != "cancelled" {
			t.Fatalf("outcome = %s", sample.Outcome)
		}
	case <-time.After(time.Second):
		t.Fatal("client cancellation did not finish")
	}
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("upstream cancellation was not propagated")
	}
}

func TestRejectUnboundedOrAmbiguousConfigurations(t *testing.T) {
	base := Config{TimeoutMS: 1000, Targets: []Target{{Name: "direct", URL: "http://localhost/v1/chat/completions",
		Nodes: 1, Engine: "fake", Hardware: []string{"fake"}, Settings: "fixed"}},
		Workloads: []Workload{{Name: "case", Model: "fake", ModelRevision: "fixture", PromptWords: 1,
			OutputTokens: 1, Concurrency: 1, Requests: 1, LoadState: "uncontrolled"}}}
	for _, mutate := range []func(*Config){
		func(c *Config) { c.TimeoutMS = 0 },
		func(c *Config) { c.Targets[0].URL = "http://user:secret@localhost/v1" },
		func(c *Config) { c.Targets[0].URL += "?key=secret" },
		func(c *Config) { c.Workloads[0].Concurrency = 129 },
		func(c *Config) { c.Workloads[0].PromptWords = 65537 },
		func(c *Config) { c.Workloads[0].LoadState = "operator-cold"; c.Workloads[0].Requests = 2 },
	} {
		copy := base
		copy.Targets = append([]Target(nil), base.Targets...)
		copy.Workloads = append([]Workload(nil), base.Workloads...)
		mutate(&copy)
		if err := copy.Validate(); err == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
}
