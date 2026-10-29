package supervisor

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/config"
)

type workerTestTransport func(*http.Request) (*http.Response, error)

func (f workerTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func workerTestArchive(t *testing.T) ([]byte, string) {
	t.Helper()
	var data bytes.Buffer
	if runtime.GOOS == "windows" {
		zw := zip.NewWriter(&data)
		file, err := zw.Create("ggml-rpc-server.exe")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(file, "test worker"); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		return data.Bytes(), "llama-test-bin-win-vulkan-x64.zip"
	}
	gz := gzip.NewWriter(&data)
	tw := tar.NewWriter(gz)
	content := []byte("test worker")
	if err := tw.WriteHeader(&tar.Header{Name: "llama/ggml-rpc-server", Size: int64(len(content)), Mode: 0o755}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: "llama/library-alias.so", Typeflag: tar.TypeSymlink, Linkname: "ggml-rpc-server"}); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return data.Bytes(), "llama-test-bin-ubuntu-vulkan-x64.tar.gz"
}

func TestEnsureWorkerInstallation(t *testing.T) {
	if runtime.GOOS != "windows" && runtime.GOOS != "linux" {
		t.Skipf("managed worker installation is not supported on %s", runtime.GOOS)
	}
	for _, test := range []struct {
		name    string
		corrupt bool
		active  bool
		wantErr bool
	}{
		{name: "verified install survives restart"},
		{name: "checksum mismatch never activates", corrupt: true, wantErr: true},
		{name: "active engines never update", active: true, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := config.NodeConfig{NodeID: "install-test", MemoryPath: filepath.Join(t.TempDir(), "node.db")}
			manager, err := New(cfg, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer closeutil.Discard(manager)
			archive, name := workerTestArchive(t)
			digest := sha256.Sum256(archive)
			release := workerRelease{Tag: "b123", Prerelease: true, Assets: []releaseAsset{{
				Name: name, Size: int64(len(archive)), Digest: "sha256:" + hex.EncodeToString(digest[:]),
				URL: "https://github.com/ggml-org/llama.cpp/releases/download/test/" + name,
			}}}
			if test.corrupt {
				archive[0] ^= 0xff
			}
			if test.active {
				manager.engines["busy"] = &managedEngine{config: config.ManagedEngineConfig{ID: "busy"}, status: Status{State: stateStarting}}
			}
			requests := 0
			client := &http.Client{Transport: workerTestTransport(func(r *http.Request) (*http.Response, error) {
				requests++
				body := archive
				if r.URL.Hostname() == "api.github.com" {
					body, err = json.Marshal([]workerRelease{release})
					if err != nil {
						return nil, err
					}
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body)), Header: make(http.Header)}, nil
			})}
			result, err := manager.ensureWorker(context.Background(), "official", client)
			if (err != nil) != test.wantErr {
				t.Fatalf("result=%+v error=%v", result, err)
			}
			if test.active {
				if requests != 0 || !errors.Is(err, ErrWorkerBusy) {
					t.Fatalf("busy request count=%d err=%v", requests, err)
				}
				return
			}
			if test.corrupt {
				if _, err := os.Stat(filepath.Join(manager.workerRoot, "installation.json")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("corrupt archive persisted: %v", err)
				}
				return
			}
			if !result.Updated || len(result.Engines) != 1 {
				t.Fatalf("install result = %+v", result)
			}
			reloaded, err := New(cfg, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer closeutil.Discard(reloaded)
			status, err := reloaded.Status(installedWorkerID)
			if err != nil || status.Kind != "ggml-rpc-server" {
				t.Fatalf("reloaded=%+v err=%v", status, err)
			}
			result, err = manager.ensureWorker(context.Background(), "official", client)
			if err != nil || result.Updated {
				t.Fatalf("repeat install=%+v err=%v", result, err)
			}
			if requests != 3 {
				t.Fatalf("repeat install redownloaded archive: requests=%d", requests)
			}
		})
	}
}

func TestWorkerReleaseValidation(t *testing.T) {
	for _, name := range []string{"../escape", "/absolute", `C:\escape`, `dir\escape`} {
		t.Run(name, func(t *testing.T) {
			if _, err := workerArchivePath(t.TempDir(), name); err == nil {
				t.Fatal("unsafe path accepted")
			}
		})
	}
	for _, source := range []string{"official", "xdna"} {
		client := &http.Client{Transport: workerTestTransport(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("[]"))}, nil
		})}
		if _, _, err := findWorkerRelease(context.Background(), client, source, "windows", "amd64"); err == nil {
			t.Fatal("empty releases accepted")
		}
	}
	if _, err := workerRepository("https://attacker.invalid"); err == nil {
		t.Fatal("arbitrary source accepted")
	}
}

func TestWorkerUpdatesPreserveActiveAndPreviousInstallations(t *testing.T) {
	if runtime.GOOS != "windows" && runtime.GOOS != "linux" {
		t.Skipf("managed worker installation is not supported on %s", runtime.GOOS)
	}
	manager, err := New(config.NodeConfig{
		NodeID: "update-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(manager)
	archive, name := workerTestArchive(t)
	digest := sha256.Sum256(archive)
	release := workerRelease{Tag: "b123", Prerelease: true, Assets: []releaseAsset{{
		Name: name, Size: int64(len(archive)), Digest: "sha256:" + hex.EncodeToString(digest[:]),
		URL: "https://github.com/ggml-org/llama.cpp/releases/download/b123/" + name,
	}}}
	requests := 0
	client := &http.Client{Transport: workerTestTransport(func(r *http.Request) (*http.Response, error) {
		requests++
		body := archive
		if r.URL.Hostname() == "api.github.com" {
			body, err = json.Marshal([]workerRelease{release})
			if err != nil {
				return nil, err
			}
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(body))}, nil
	})}
	if _, err := manager.ensureWorker(context.Background(), "official", client); err != nil {
		t.Fatal(err)
	}
	previousBinary := manager.workerInstallation.Binary
	release.Tag = "b124"
	if err := manager.beginWorkerControl(); err != nil {
		t.Fatal(err)
	}
	result, err := manager.ensureWorker(context.Background(), "official", client)
	manager.finishWorkerControl()
	if err != nil || result.Warning == "" || requests != 2 {
		t.Fatalf("active update = %+v, %v; requests=%d", result, err, requests)
	}
	result, err = manager.ensureWorker(context.Background(), "official", client)
	if err != nil || !result.Updated || result.Version != "b124" {
		t.Fatalf("idle update = %+v, %v", result, err)
	}
	if _, err := os.Stat(previousBinary); err != nil {
		t.Fatalf("previous version removed: %v", err)
	}
	if manager.workerInstallation.Binary == previousBinary {
		t.Fatal("update overwrote previous version")
	}
	release.Tag = "b122"
	result, err = manager.ensureWorker(context.Background(), "official", client)
	if err != nil || result.Updated || result.Version != "b124" {
		t.Fatalf("downgrade = %+v, %v", result, err)
	}
	release.Tag = "b125"
	release.Assets[0].Digest = "sha256:" + strings.Repeat("0", 64)
	if _, err := manager.ensureWorker(context.Background(), "official", client); err == nil {
		t.Fatal("corrupt update accepted")
	}
	persisted, err := loadWorkerInstallation(manager.workerRoot)
	if err != nil || persisted.Tag != "b124" {
		t.Fatalf("failed update replaced persistent installation: %+v %v", persisted, err)
	}
}

func TestWorkerArchiveRejectsTraversal(t *testing.T) {
	var data bytes.Buffer
	zw := zip.NewWriter(&data)
	file, err := zw.Create("../outside.exe")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(file, "bad"); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	archive := filepath.Join(root, "bad.zip")
	if err := os.WriteFile(archive, data.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := extractWorkerArchive(archive, filepath.Join(root, "payload"), true); err == nil {
		t.Fatal("archive traversal accepted")
	}
}

func TestOfficialWorkerReleaseInstallSmoke(t *testing.T) {
	if os.Getenv("MAISTR0_VERIFY_WORKER_DOWNLOAD") != "1" {
		t.Skip("set MAISTR0_VERIFY_WORKER_DOWNLOAD=1 to download and verify an official release")
	}
	manager, err := New(config.NodeConfig{
		NodeID: "release-smoke", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(manager)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	result, err := manager.EnsureWorker(ctx, "official")
	if err != nil {
		t.Fatal(err)
	}
	worker, err := manager.engine(installedWorkerID)
	if err != nil {
		t.Fatal(err)
	}
	output, err := exec.CommandContext(ctx, worker.config.BinaryPath, "--help").CombinedOutput()
	if err != nil {
		t.Fatalf("installed worker runtime did not launch: %v\n%s", err, output)
	}
	devices, err := manager.RuntimeDevices(ctx)
	if err != nil {
		t.Fatalf("installed llama-server device probe failed: %v", err)
	}
	t.Logf("verified official %s worker runtime; measured devices: %+v", result.Version, devices)
}

func BenchmarkManagedEngineStatus(b *testing.B) {
	manager := &Manager{engines: map[string]*managedEngine{
		"worker": {config: config.ManagedEngineConfig{ID: "worker"}, status: Status{ID: "worker", State: stateStopped}},
	}}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := manager.Status("worker"); err != nil {
			b.Fatal(err)
		}
	}
}
