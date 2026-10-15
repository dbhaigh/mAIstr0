package logging

import (
	"bytes"
	"io"
	"log"
	"strings"
	"testing"
)

func TestConsoleWriterRoutesFileOnlyLogsToFile(t *testing.T) {
	var console, file bytes.Buffer
	logger := log.New(io.MultiWriter(NewConsoleWriter(&console), &file), "", 0)

	logger.Print("FILE_ONLY: memory sync failed: connection refused")
	logger.Print("nodeagent: unrelated warning")

	if strings.Contains(console.String(), "FILE_ONLY:") || strings.Contains(console.String(), "memory sync") {
		t.Fatalf("console contains file-only output: %q", console.String())
	}
	if !strings.Contains(console.String(), "unrelated warning") {
		t.Fatalf("console omitted failure output: %q", console.String())
	}
	if !strings.Contains(file.String(), "FILE_ONLY: memory sync failed: connection refused") ||
		!strings.Contains(file.String(), "unrelated warning") {
		t.Fatalf("file log did not retain all output: %q", file.String())
	}
}
