package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/config"
	bolt "go.etcd.io/bbolt"
)

func TestDiagnosticsRedactsSecretsAndDoesNotExposeRecentLogMessages(t *testing.T) {
	engineServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer engineServer.Close()
	engineURL, err := url.Parse(engineServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	engineURL.User = url.UserPassword("diagnostic-user", "engine-credential")

	tempDir := t.TempDir()
	memoryPath := filepath.Join(tempDir, "missing", "private-memory.db")
	nodeConfig := config.DefaultNodeConfig()
	nodeConfig.NodeID = "diagnostic-node"
	nodeConfig.PairingPIN = "123456"
	nodeConfig.MemoryPath = memoryPath
	nodeConfig.Engines = []config.EngineConfig{{Name: "vllm", URL: engineURL.String()}}
	configPath := filepath.Join(tempDir, "node.json")
	configBytes, err := json.Marshal(nodeConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, configBytes, 0o600); err != nil {
		t.Fatal(err)
	}

	role, cfg, err := loadDiagnosticsConfig(configPrintOptions{role: "node", nodeConfigPath: configPath})
	if err != nil {
		t.Fatalf("load diagnostic configuration: %v", err)
	}
	if _, err := os.Stat(filepath.Dir(memoryPath)); !os.IsNotExist(err) {
		t.Fatalf("diagnostic config validation created its database directory: %v", err)
	}

	logPath := filepath.Join(tempDir, "maistr0.log")
	logRecord := `{"time":"2026-10-21T12:00:00Z","level":"ERROR","msg":"private prompt must not be displayed","component":"nodeagent","error_type":"*errors.errorString"}` + "\n"
	if err := os.WriteFile(logPath, []byte(logRecord), 0o600); err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	if err := writeDiagnostics(&output, role, cfg, logPath); err != nil {
		t.Fatalf("write diagnostics: %v", err)
	}
	report := output.String()
	for _, secret := range []string{"123456", "diagnostic-user", "engine-credential", memoryPath, "private prompt"} {
		if strings.Contains(report, secret) {
			t.Fatalf("diagnostics exposed sensitive value %q: %s", secret, report)
		}
	}
	for _, want := range []string{
		"version:", "role: node", "log_level: info", "engine vllm: available",
		"memory_database: not initialized", "component=nodeagent error_type=*errors.errorString",
	} {
		if !strings.Contains(report, want) {
			t.Fatalf("diagnostics omitted %q: %s", want, report)
		}
	}
	if _, err := os.Stat(filepath.Dir(memoryPath)); !os.IsNotExist(err) {
		t.Fatalf("diagnostic database check created its parent directory: %v", err)
	}
}

func TestCheckBoltDatabaseUsesReadOnlyAccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "health.db")
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("health"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if got := checkBoltDatabase(path); got != "healthy" {
		t.Fatalf("database health = %q, want healthy", got)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		t.Fatalf("read-only health check modified database: before=%v after=%v", before, after)
	}
}

func TestRecentDiagnosticErrorsAreBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "maistr0.log")
	var records strings.Builder
	for index := 0; index < maxDiagnosticErrors+5; index++ {
		records.WriteString(`{"time":"2026-10-21T12:00:00Z","level":"ERROR","msg":"ignored","component":"orchestrator","error_type":"*errors.errorString"}`)
		records.WriteByte('\n')
	}
	if err := os.WriteFile(path, []byte(records.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	entries, err := recentDiagnosticErrors(path, time.Now())
	if err != nil {
		t.Fatalf("read recent errors: %v", err)
	}
	if len(entries) != maxDiagnosticErrors {
		t.Fatalf("recent diagnostic errors = %d, want %d", len(entries), maxDiagnosticErrors)
	}
}
