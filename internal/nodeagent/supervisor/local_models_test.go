package supervisor

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/config"
)

func TestRuntimeDeviceOutputIsBounded(t *testing.T) {
	var output runtimeDeviceOutput
	const length = 300 << 10
	n, err := io.Copy(&output, strings.NewReader(strings.Repeat("x", length)))
	if err != nil || n != length || output.data.Len() != 256<<10 || !output.truncated {
		t.Fatalf("output bound: copied=%d length=%d truncated=%t err=%v", n, output.data.Len(), output.truncated, err)
	}
}

func writeLocalTestGGUF(t *testing.T, path string) {
	t.Helper()
	var data bytes.Buffer
	write := func(value any) {
		t.Helper()
		if err := binary.Write(&data, binary.LittleEndian, value); err != nil {
			t.Fatal(err)
		}
	}
	text := func(value string) { write(uint64(len(value))); data.WriteString(value) }
	data.WriteString("GGUF")
	write(uint32(3))
	write(uint64(0))
	write(uint64(5))
	text("general.architecture")
	write(uint32(8))
	text("llama")
	for _, entry := range []struct {
		key   string
		value uint32
	}{
		{"llama.block_count", 32}, {"llama.embedding_length", 4096}, {"llama.attention.head_count", 32}, {"llama.context_length", 8192},
	} {
		text(entry.key)
		write(uint32(4))
		write(entry.value)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoverLocalHeadModels(t *testing.T) {
	root := t.TempDir()
	ollama := filepath.Join(root, "ollama")
	lm := filepath.Join(root, "lmstudio")
	digest := strings.Repeat("a", 64)
	writeLocalTestGGUF(t, filepath.Join(ollama, "blobs", "sha256-"+digest))
	manifest := filepath.Join(ollama, "manifests", "registry.ollama.ai", "library", "test", "latest")
	if err := os.MkdirAll(filepath.Dir(manifest), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest, []byte(`{"layers":[{"mediaType":"application/vnd.ollama.image.model","digest":"sha256:`+digest+`"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	writeLocalTestGGUF(t, filepath.Join(lm, "org", "model.gguf"))
	writeLocalTestGGUF(t, filepath.Join(lm, "split-00001-of-00002.gguf"))
	if err := os.WriteFile(filepath.Join(lm, "invalid.gguf"), []byte("not a model"), 0o600); err != nil {
		t.Fatal(err)
	}
	roots := map[string][]string{"ollama": {ollama}, "lmstudio": {lm, lm}}
	got, err := discoverLocalHeadModels(context.Background(), roots, 2048)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Models) != 2 || len(got.Warnings) == 0 {
		t.Fatalf("inventory=%+v", got)
	}
	for _, model := range got.Models {
		if model.Profile.RequestedContext != 2048 || model.Profile.FileSizeBytes == 0 || model.Profile.LayerCount != 32 || len(model.ID) != 64 {
			t.Fatalf("model=%+v", model)
		}
		if got.Models[1].Name != "test:latest" {
			t.Fatalf("Ollama model alias=%q", got.Models[1].Name)
		}
	}
	wire, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(wire), "sha256-"+digest) || strings.Contains(string(wire), root) {
		t.Fatalf("local path leaked: %s", wire)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := discoverLocalHeadModels(ctx, roots, 2048); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
}

func TestConfigureLocalHeadPersistsAndRespectsActiveEngines(t *testing.T) {
	modelRoot := t.TempDir()
	t.Setenv("LMSTUDIO_MODELS", modelRoot)
	t.Setenv("OLLAMA_MODELS", filepath.Join(modelRoot, "absent"))
	modelPath := filepath.Join(modelRoot, "model.gguf")
	writeLocalTestGGUF(t, modelPath)
	cfg := config.NodeConfig{NodeID: "local-head", MemoryPath: filepath.Join(t.TempDir(), "node.db")}
	root, err := installedWorkerRoot(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	installed := workerInstallation{Source: "official", Tag: "b123", Binary: filepath.Join(root, "ggml-rpc-server")}
	if runtime.GOOS == "windows" {
		installed.Binary += ".exe"
	}
	for _, path := range []string{installed.Binary, installedServerBinary(installed)} {
		if err := os.WriteFile(path, []byte("test executable"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	manager := &Manager{workerRoot: root, workerConfig: cfg, workerInstallation: &installed, engines: map[string]*managedEngine{}}
	models, err := manager.LocalHeadModels(context.Background(), 4096)
	if err != nil {
		t.Fatal(err)
	}
	var id string
	for _, entry := range models.Models {
		if entry.path == modelPath {
			id = entry.ID
		}
	}
	if id == "" {
		t.Fatalf("test model not found: %+v", models)
	}
	status, err := manager.ConfigureLocalHead(context.Background(), id, 8192)
	if err != nil {
		t.Fatal(err)
	}
	if status.ID != installedHeadID {
		t.Fatalf("head=%+v", status)
	}
	profile, err := manager.ModelProfile(status.ID, 8192)
	if err != nil || profile.Name != "model" {
		t.Fatalf("profile=%+v err=%v", profile, err)
	}
	restarted, err := New(cfg, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if restarted.engines[installedHeadID].config.Options.LlamaServer.ContextSize != 8192 || restarted.engines[installedHeadID].modelName != "model" {
		t.Fatal("head configuration not restored")
	}
	manager.engines[installedHeadID].status.State = stateReady
	if _, err := manager.ConfigureLocalHead(context.Background(), id, 4096); !errors.Is(err, ErrWorkerBusy) {
		t.Fatalf("active head replacement=%v", err)
	}
	if _, err := manager.ConfigureLocalHead(context.Background(), "not-a-discovered-model", 4096); !errors.Is(err, ErrNotFound) {
		t.Fatalf("arbitrary model ID=%v", err)
	}
}
