package engine

import "testing"

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
