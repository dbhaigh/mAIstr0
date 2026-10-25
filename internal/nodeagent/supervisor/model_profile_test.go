package supervisor

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/maistr0/maistr0/internal/config"
)

func TestReadConfiguredHuggingFaceModelIncludesWeightSizesAndContext(t *testing.T) {
	modelDir := t.TempDir()
	writeTestHuggingFaceModel(t, modelDir, 2048)

	profile, err := readConfiguredModelPath(modelDir, []string{modelDir}, 8192)
	if err != nil {
		t.Fatal(err)
	}
	if profile.LayerCount != 32 || profile.HiddenSize != 4096 || profile.KVHeads != 8 ||
		profile.HeadDim != 128 || profile.RequestedContext != 8192 || profile.FileSizeBytes != 2048 {
		t.Fatalf("parsed model profile = %+v", profile)
	}
}

func TestReadCachedHuggingFaceModelUsesConfiguredCacheAndModelID(t *testing.T) {
	cacheRoot := t.TempDir()
	t.Setenv("HF_HUB_CACHE", cacheRoot)
	t.Setenv("HF_HOME", "")
	snapshot := filepath.Join(cacheRoot, "models--org--model", "snapshots", "revision")
	writeTestHuggingFaceModel(t, snapshot, 4096)

	profile, err := readCachedHuggingFaceModel("org/model", 2048)
	if err != nil {
		t.Fatal(err)
	}
	if profile.Name != "org/model" || profile.RequestedContext != 2048 || profile.FileSizeBytes != 4096 {
		t.Fatalf("cached model profile = %+v", profile)
	}
}

func TestManagerModelProfileUsesConfiguredHuggingFaceModelID(t *testing.T) {
	cacheRoot := t.TempDir()
	t.Setenv("HF_HUB_CACHE", cacheRoot)
	t.Setenv("HF_HOME", "")
	writeTestHuggingFaceModel(
		t,
		filepath.Join(cacheRoot, "models--org--model", "snapshots", "revision"),
		4096,
	)
	binaryPath, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	manager := &Manager{engines: map[string]*managedEngine{
		"model-engine": {
			config: config.ManagedEngineConfig{
				ID: "model-engine", Kind: config.ManagedEngineVLLM,
				BinaryPath: binaryPath, ModelID: "org/model",
			},
			binaryAllowlist: []string{filepath.Dir(binaryPath)},
		},
	}}

	profile, err := manager.ModelProfile("model-engine", 4096)
	if err != nil {
		t.Fatal(err)
	}
	if profile.Name != "org/model" || profile.RequestedContext != 4096 || profile.FileSizeBytes != 4096 {
		t.Fatalf("managed model profile = %+v", profile)
	}
}

func TestManagerModelProfileUsesAllowListedLocalModelDirectory(t *testing.T) {
	modelDir := t.TempDir()
	writeTestHuggingFaceModel(t, modelDir, 4096)
	binaryPath, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	manager := &Manager{engines: map[string]*managedEngine{
		"model-engine": {
			config: config.ManagedEngineConfig{
				ID: "model-engine", Kind: config.ManagedEngineVLLM,
				BinaryPath: binaryPath, ModelPath: modelDir,
			},
			binaryAllowlist: []string{filepath.Dir(binaryPath)},
			modelAllowlist:  []string{modelDir},
		},
	}}

	profile, err := manager.ModelProfile("model-engine", 4096)
	if err != nil {
		t.Fatal(err)
	}
	if profile.Name != filepath.Base(modelDir) || profile.FileSizeBytes != 4096 {
		t.Fatalf("local managed model profile = %+v", profile)
	}
}

func TestReadCachedHuggingFaceModelRejectsMissingWeights(t *testing.T) {
	cacheRoot := t.TempDir()
	t.Setenv("HF_HUB_CACHE", cacheRoot)
	t.Setenv("HF_HOME", "")
	snapshot := filepath.Join(cacheRoot, "models--org--model", "snapshots", "revision")
	if err := os.MkdirAll(snapshot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(snapshot, "config.json"), []byte(testHuggingFaceConfig), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := readCachedHuggingFaceModel("org/model", 4096); err == nil {
		t.Fatal("cached model without measurable weights was accepted")
	}
}

func TestReadCachedHuggingFaceModelRejectsPathTraversalID(t *testing.T) {
	if _, err := readCachedHuggingFaceModel("../outside", 4096); err == nil {
		t.Fatal("model ID containing path traversal was accepted")
	}
	if _, err := readCachedHuggingFaceModel(`org\outside`, 4096); err == nil {
		t.Fatal("model ID containing a platform path separator was accepted")
	}
}

const testHuggingFaceConfig = `{
  "model_type": "llama",
  "num_hidden_layers": 32,
  "hidden_size": 4096,
  "num_attention_heads": 32,
  "num_key_value_heads": 8,
  "head_dim": 128,
  "max_position_embeddings": 32768,
  "quantization_config": {"bits": 4}
}`

func writeTestHuggingFaceModel(t *testing.T, directory string, weightSize int) {
	t.Helper()
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "config.json"), []byte(testHuggingFaceConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "model.safetensors"), make([]byte, weightSize), 0o600); err != nil {
		t.Fatal(err)
	}
}
