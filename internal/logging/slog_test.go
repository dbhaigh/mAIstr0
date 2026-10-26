package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"strings"
	"testing"
)

func TestStructuredLoggingOmitsOpaqueURLSecrets(t *testing.T) {
	result := redactURLSecrets("https://example.test/path?opaque=private-query#private-fragment")
	for _, secret := range []string{"private-query", "private-fragment"} {
		if strings.Contains(result, secret) {
			t.Fatalf("URL diagnostics exposed %q", secret)
		}
	}
	if !strings.Contains(result, "https://example.test/path") || !strings.Contains(result, "redacted") {
		t.Fatalf("redacted endpoint lost useful routing/omission context: %q", result)
	}
}

func TestStructuredLoggingRedactsDynamicErrorsAndRoutesFileOnlyRecords(t *testing.T) {
	var console, file bytes.Buffer
	logger := NewSlogLogger(&console, &file, slog.LevelInfo)
	previous := slog.Default()
	previousOutput := log.Writer()
	slog.SetDefault(logger)
	t.Cleanup(func() {
		slog.SetDefault(previous)
		log.SetOutput(previousOutput)
	})

	Errorf("router", "upstream failed: %v at %s",
		errors.New("prompt content echoed by upstream"),
		"https://user:credential@engine.local/v1?api_key=secret&mode=chat")
	FileInfof("memory", "sync completed for peer %s", "peer-1")

	for _, secret := range []string{
		"prompt content echoed by upstream", "credential", "api_key=secret",
	} {
		if strings.Contains(file.String(), secret) || strings.Contains(console.String(), secret) {
			t.Fatalf("logs contain sensitive value %q: file=%s console=%s", secret, file.String(), console.String())
		}
	}
	if strings.Contains(console.String(), "sync completed") {
		t.Fatalf("file-only record reached console: %s", console.String())
	}
	if !strings.Contains(file.String(), `"file_only":true`) {
		t.Fatalf("file log omitted file_only attribute: %s", file.String())
	}

	var record map[string]any
	if err := json.Unmarshal(bytes.Split(bytes.TrimSpace(file.Bytes()), []byte("\n"))[0], &record); err != nil {
		t.Fatalf("decode structured log record: %v", err)
	}
	if record["component"] != "router" || record["error_type"] != "*errors.errorString" {
		t.Fatalf("structured error fields = %#v", record)
	}
	if !strings.Contains(record["msg"].(string), "[redacted]") {
		t.Fatalf("URL secrets were not redacted: %#v", record["msg"])
	}
}
