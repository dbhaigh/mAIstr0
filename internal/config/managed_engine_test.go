package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/engine"
)

func TestManagedEngineConfigRequiresOperatorAllowLists(t *testing.T) {
	binary, binaryDir := createManagedEngineBinary(t)
	modelDir := t.TempDir()
	modelPath := filepath.Join(modelDir, "model.gguf")
	if err := os.WriteFile(modelPath, []byte("model"), 0o600); err != nil {
		t.Fatal(err)
	}
	configured := ManagedEngineConfig{
		ID:         "llama-local",
		Kind:       ManagedEngineLlamaServer,
		BinaryPath: binary,
		ModelPath:  modelPath,
		Options: ManagedEngineOptions{
			LlamaServer: &LlamaServerOptions{Port: 8081, ContextSize: 4096, GPULayers: 12, Threads: 4},
		},
	}
	if err := ValidateManagedEngines(
		[]ManagedEngineConfig{configured},
		[]string{binaryDir},
		[]string{modelDir},
	); err != nil {
		t.Fatalf("valid allow-listed engine rejected: %v", err)
	}

	if err := ValidateManagedEngines(
		[]ManagedEngineConfig{configured},
		[]string{t.TempDir()},
		[]string{modelDir},
	); err == nil || !strings.Contains(err.Error(), "operator allow-list") {
		t.Fatalf("unapproved binary path error = %v, want allow-list rejection", err)
	}
	if err := ValidateManagedEngines(
		[]ManagedEngineConfig{configured},
		[]string{binaryDir},
		[]string{t.TempDir()},
	); err == nil || !strings.Contains(err.Error(), "operator allow-list") {
		t.Fatalf("unapproved model path error = %v, want allow-list rejection", err)
	}
}

func TestManagedEngineConfigRejectsPathEscapeAndMismatchedOptions(t *testing.T) {
	binary, binaryDir := createManagedEngineBinary(t)
	modelRoot := t.TempDir()
	outsideModel := filepath.Join(t.TempDir(), "outside.gguf")
	if err := os.WriteFile(outsideModel, []byte("model"), 0o600); err != nil {
		t.Fatal(err)
	}
	managed := ManagedEngineConfig{
		ID:         "engine-one",
		Kind:       ManagedEngineLlamaServer,
		BinaryPath: binary,
		ModelPath:  outsideModel,
		Options: ManagedEngineOptions{
			GGMLRPCServer: &GGMLRPCServerOptions{Port: 50052},
		},
	}
	if err := ValidateManagedEngines([]ManagedEngineConfig{managed}, []string{binaryDir}, []string{modelRoot}); err == nil {
		t.Fatal("expected out-of-root model and mismatched option errors")
	}

	escapedRoot := filepath.Join(t.TempDir(), "allowed")
	if err := os.Mkdir(escapedRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(escapedRoot, "escape")
	if err := os.Symlink(filepath.Dir(outsideModel), link); err != nil {
		t.Skipf("directory symlinks are unavailable: %v", err)
	}
	escapedModel := filepath.Join(link, filepath.Base(outsideModel))
	managed.ModelPath = escapedModel
	managed.Options = ManagedEngineOptions{}
	if err := ValidateManagedEngines([]ManagedEngineConfig{managed}, []string{binaryDir}, []string{escapedRoot}); err == nil {
		t.Fatal("expected symlink escape to be rejected")
	}
}

func TestGGMLRPCAlwaysRequiresLoopback(t *testing.T) {
	binary, binaryDir := createManagedEngineBinary(t)
	configured := ManagedEngineConfig{
		ID:         "rpc-worker",
		Kind:       ManagedEngineGGMLRPCServer,
		BinaryPath: binary,
		Options: ManagedEngineOptions{
			GGMLRPCServer: &GGMLRPCServerOptions{BindAddress: "0.0.0.0", Port: 50052},
		},
	}
	for _, allowRemote := range []bool{false, true} {
		configured.Options.GGMLRPCServer.AllowRemoteBind = allowRemote
		if err := ValidateManagedEngines([]ManagedEngineConfig{configured}, []string{binaryDir}, nil); err == nil ||
			!strings.Contains(err.Error(), "must be loopback") {
			t.Fatalf("remote bind (allow_remote_bind=%t) error = %v, want loopback-only rejection", allowRemote, err)
		}
	}
	configured.Options.GGMLRPCServer.BindAddress = "127.0.0.1"
	configured.Options.GGMLRPCServer.AllowRemoteBind = false
	if err := ValidateManagedEngines([]ManagedEngineConfig{configured}, []string{binaryDir}, nil); err != nil {
		t.Fatalf("loopback RPC bind rejected: %v", err)
	}
}

func TestManagedEngineTemplatesAreTypedAndKindChecked(t *testing.T) {
	binary, binaryDir := createManagedEngineBinary(t)
	modelDir := t.TempDir()
	modelPath := filepath.Join(modelDir, "model.gguf")
	if err := os.WriteFile(modelPath, []byte("model"), 0o600); err != nil {
		t.Fatal(err)
	}
	configured := ManagedEngineConfig{
		ID: "intel-npu", Kind: ManagedEngineLlamaServer, BinaryPath: binary,
		ModelPath: modelPath, Template: engine.TemplateLlamaCPPOpenVINONPU,
		Options: ManagedEngineOptions{LlamaServer: &LlamaServerOptions{ContextSize: 4096}},
	}
	if err := ValidateManagedEngines([]ManagedEngineConfig{configured}, []string{binaryDir}, []string{modelDir}); err != nil {
		t.Fatalf("valid OpenVINO template rejected: %v", err)
	}

	configured.ID = "amd-xdna2"
	configured.Template = engine.TemplateLlamaCPPXDNA2NPU
	if err := ValidateManagedEngines([]ManagedEngineConfig{configured}, []string{binaryDir}, []string{modelDir}); err != nil {
		t.Fatalf("valid XDNA template rejected: %v", err)
	}
	configured.Options.LlamaServer.ContextSize = 0
	if err := ValidateManagedEngines([]ManagedEngineConfig{configured}, []string{binaryDir}, []string{modelDir}); err == nil ||
		!strings.Contains(err.Error(), "requires options.llama_server.context_size") {
		t.Fatalf("XDNA template without context size error = %v, want explicit context requirement", err)
	}
	configured.Options.LlamaServer.ContextSize = 4096
	configured.Kind = ManagedEngineVLLM
	if err := ValidateManagedEngines([]ManagedEngineConfig{configured}, []string{binaryDir}, []string{modelDir}); err == nil ||
		!strings.Contains(err.Error(), "requires kind llama-server") {
		t.Fatalf("template kind mismatch error = %v", err)
	}
	configured.Kind = ManagedEngineLlamaServer
	configured.Template = engine.TemplateFastFlowLMXDNA2
	if err := ValidateManagedEngines([]ManagedEngineConfig{configured}, []string{binaryDir}, []string{modelDir}); err == nil ||
		!strings.Contains(err.Error(), "not a managed process template") {
		t.Fatalf("external FastFlowLM template error = %v", err)
	}
}

func createManagedEngineBinary(t *testing.T) (string, string) {
	t.Helper()
	binaryDir := t.TempDir()
	name := "managed-engine"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	binary := filepath.Join(binaryDir, name)
	if err := os.WriteFile(binary, []byte("fake executable"), 0o700); err != nil {
		t.Fatal(err)
	}
	return binary, binaryDir
}
