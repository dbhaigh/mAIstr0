package engine

import (
	"strings"
	"testing"
)

func TestModelCatalogFiltersByEngineAndQuery(t *testing.T) {
	models := ModelCatalog(" OLLAMA ", "code")
	if len(models) == 0 {
		t.Fatal("expected code-capable Ollama catalog entries")
	}
	for _, model := range models {
		if model.Engine != "ollama" {
			t.Fatalf("catalog returned wrong engine %q", model.Engine)
		}
	}
	if got := ModelCatalog("vllm", ""); len(got) != 0 {
		t.Fatalf("unsupported engine catalog = %#v", got)
	}
}

func TestModelCatalogReturnsIndependentEntries(t *testing.T) {
	models := ModelCatalog("ollama", "")
	if len(models) == 0 || len(models[0].Tags) == 0 {
		t.Fatal("expected catalog entries with capability tags")
	}
	models[0].Tags[0] = "mutated"
	if ModelCatalog("ollama", "")[0].Tags[0] == "mutated" {
		t.Fatal("catalog caller mutated shared tags")
	}
}

func TestModelCatalogHasActionableMetadataAndRepresentativeChoices(t *testing.T) {
	models := ModelCatalog("ollama", "")
	byName := make(map[string]CatalogModel, len(models))
	for _, model := range models {
		if strings.TrimSpace(model.Description) == "" || len(model.Tags) == 0 {
			t.Errorf("catalog entry %q is missing its description or capability tags", model.Name)
		}
		byName[model.Name] = model
	}

	for _, name := range []string{
		"gemma3:12b",
		"llama3.2-vision:11b",
		"qwen2.5-coder:7b",
		"qwen3:8b",
	} {
		if _, ok := byName[name]; !ok {
			t.Errorf("catalog is missing representative model %q", name)
		}
	}

	visionModels := ModelCatalog("ollama", "vision")
	if len(visionModels) == 0 {
		t.Fatal("expected vision-capable models to be searchable")
	}
}
