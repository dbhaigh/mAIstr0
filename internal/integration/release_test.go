package integration

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
)

func TestReleaseCandidateInstallUpgrade(t *testing.T) {
	binary, upgrade, archive := os.Getenv("MAISTR0_RELEASE_BINARY"), os.Getenv("MAISTR0_UPGRADE_BINARY"), os.Getenv("MAISTR0_RELEASE_ARCHIVE")
	if binary == "" && upgrade == "" && archive == "" {
		t.Skip("opt-in packaged candidate check; run build/verify-release.ps1")
	}
	if binary == "" || upgrade == "" || archive == "" {
		t.Fatal("all release-candidate paths are required")
	}
	directory := t.TempDir()
	installed := filepath.Join(directory, filepath.Base(binary))
	extractReleaseBinary(t, archive, installed)
	hash := func(path string) [32]byte {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return sha256.Sum256(data)
	}
	if hash(binary) != hash(installed) {
		t.Fatal("packaged binary checksum differs")
	}
	home := filepath.Join(directory, "home")
	if err := os.Mkdir(home, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("APPDATA", home)
	t.Setenv("LOCALAPPDATA", home)
	t.Setenv("XDG_CONFIG_HOME", home)
	t.Setenv("XDG_DATA_HOME", home)
	t.Setenv("XDG_CACHE_HOME", home)
	address := freeTCPAddress(t)
	configPath := filepath.Join(directory, "node.json")
	engine := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"data":[]}`)
	}))
	defer engine.Close()
	data, err := json.Marshal(map[string]any{"listen_addr": address, "memory_path": filepath.Join(directory, "memory.db"),
		"node_id": "release-check", "advertise_addr": "https://" + address, "rpc_relay_listen_addr": freeTCPAddress(t),
		"engines":           []map[string]string{{"name": "openai", "url": engine.URL}},
		"discovery_enabled": false, "auto_open_browser": false, "tray_mode": "hidden"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	runCLI := func(binary string, args ...string) []byte {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, binary, args...)
		command.Dir = directory
		output, err := command.Output()
		if err != nil {
			t.Fatalf("candidate CLI %v failed: %v", args, err)
		}
		return output
	}
	if output := runCLI(installed, "--version"); !strings.HasPrefix(string(output), "maistr0 ") {
		t.Fatal("missing packaged version")
	}
	args := []string{"--role", "node", "--node-config", configPath, "--no-discovery"}
	if output := runCLI(installed, append(args, "--print-config")...); !json.Valid(output) {
		t.Fatal("normalized configuration is not JSON")
	}
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	requestJSON := func(method, path, body string, output any) {
		t.Helper()
		request, err := http.NewRequest(method, "http://"+address+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer closeutil.Discard(response.Body)
		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s %s = %d", method, path, response.StatusCode)
		}
		if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(output); err != nil {
			t.Fatal(err)
		}
	}
	first := startReleaseCandidate(t, installed, directory, args, address)
	var pairing struct {
		Fingerprint string `json:"fingerprint"`
	}
	requestJSON(http.MethodGet, "/api/pairing/status", "", &pairing)
	if pairing.Fingerprint == "" {
		t.Fatal("clean install has no identity")
	}
	var fact map[string]any
	requestJSON(http.MethodPost, "/memory/facts", `{"key":"release-check","value":"retained","scope":"release-check"}`, &fact)
	first()
	if err := os.Remove(installed); err != nil {
		t.Fatal(err)
	}
	source, err := os.Open(upgrade)
	if err != nil {
		t.Fatal(err)
	}
	destination, err := os.OpenFile(installed, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
	if err != nil {
		_ = source.Close()
		t.Fatal(err)
	}
	_, copyErr := io.Copy(destination, source)
	sourceErr, destinationErr := source.Close(), destination.Close()
	if copyErr != nil || sourceErr != nil || destinationErr != nil {
		t.Fatalf("replace candidate: %v / %v / %v", copyErr, sourceErr, destinationErr)
	}
	second := startReleaseCandidate(t, installed, directory, args, address)
	var after struct {
		Fingerprint string `json:"fingerprint"`
	}
	requestJSON(http.MethodGet, "/api/pairing/status", "", &after)
	if after.Fingerprint != pairing.Fingerprint {
		t.Fatal("upgrade changed identity fingerprint")
	}
	var facts []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	requestJSON(http.MethodGet, "/memory/facts?scope=release-check", "", &facts)
	if len(facts) != 1 || facts[0].Key != "release-check" || facts[0].Value != "retained" {
		t.Fatal("upgrade lost persistent fact")
	}
	second()
	if len(runCLI(installed, append(args, "--diagnostics")...)) == 0 {
		t.Fatal("candidate diagnostics empty")
	}
}

type releaseStartupOutput struct{ data []byte }

func (o *releaseStartupOutput) Write(p []byte) (int, error) {
	o.data = append(o.data, p[:min(len(p), (32<<10)-len(o.data))]...)
	return len(p), nil
}

func startReleaseCandidate(t *testing.T, binary, directory string, args []string, address string) func() {
	t.Helper()
	command := exec.Command(binary, append(args, "--no-browser", "--no-peer-updates")...)
	command.Dir = directory
	// os/exec serializes writes to this shared writer; inspect only after Wait.
	output := &releaseStartupOutput{}
	command.Stdout, command.Stderr = output, output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait(); close(done) }()
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		if runtime.GOOS == "windows" {
			_ = command.Process.Kill()
		} else {
			_ = command.Process.Signal(os.Interrupt)
		}
		select {
		case err := <-done:
			if runtime.GOOS != "windows" && err != nil {
				t.Errorf("candidate shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			_ = command.Process.Kill()
			if err := <-done; err != nil {
				t.Logf("forced candidate shutdown: %v", err)
			}
			t.Error("candidate needed forced shutdown")
		}
	}
	t.Cleanup(stop)
	client := &http.Client{Timeout: 200 * time.Millisecond}
	defer client.CloseIdleConnections()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			t.Fatalf("candidate exited before readiness: %v; startup: %s", err, output.data)
		case <-deadline.C:
			t.Fatal("candidate readiness timed out")
		case <-ticker.C:
			response, err := client.Get("http://" + address + "/api/status")
			if err == nil {
				_ = response.Body.Close()
				if response.StatusCode == http.StatusOK {
					return stop
				}
			}
		}
	}
}

func extractReleaseBinary(t *testing.T, archive, destination string) {
	t.Helper()
	var data []byte
	read := func(reader io.Reader) {
		t.Helper()
		var err error
		data, err = io.ReadAll(io.LimitReader(reader, 128<<20+1))
		if err != nil || len(data) > 128<<20 {
			t.Fatalf("invalid candidate binary: %v", err)
		}
	}
	if strings.HasSuffix(archive, ".zip") {
		reader, err := zip.OpenReader(archive)
		if err != nil {
			t.Fatal(err)
		}
		defer closeutil.Discard(reader)
		for _, file := range reader.File {
			if file.Name != filepath.Base(destination) {
				continue
			}
			body, err := file.Open()
			if err != nil {
				t.Fatal(err)
			}
			read(body)
			if err := body.Close(); err != nil {
				t.Fatal(err)
			}
		}
	} else {
		file, err := os.Open(archive)
		if err != nil {
			t.Fatal(err)
		}
		defer closeutil.Discard(file)
		compressed, err := gzip.NewReader(file)
		if err != nil {
			t.Fatal(err)
		}
		defer closeutil.Discard(compressed)
		reader := tar.NewReader(compressed)
		for {
			header, err := reader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if header.Name == "./"+filepath.Base(destination) || header.Name == filepath.Base(destination) {
				read(reader)
			}
		}
	}
	if len(data) == 0 {
		t.Fatalf("archive has no %s", filepath.Base(destination))
	}
	if err := os.WriteFile(destination, data, 0700); err != nil {
		t.Fatal(err)
	}
}
