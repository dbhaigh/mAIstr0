package deepseek

import (
	"reflect"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/memory"
)

func TestToolRegistryGeneratesSchemasFromTypedArguments(t *testing.T) {
	harness := New(cluster.NewRegistry(), nil)
	harness.SetMemory(memory.NewInMemoryStore())
	definitions := harness.ListTools()
	if len(definitions) != len(DefaultTools())+4 {
		t.Fatalf("registered tool count = %d, want %d", len(definitions), len(DefaultTools())+4)
	}

	byName := make(map[string]ToolDef, len(definitions))
	for _, definition := range definitions {
		if _, duplicate := byName[definition.Name]; duplicate {
			t.Fatalf("tool %q is registered more than once", definition.Name)
		}
		byName[definition.Name] = definition
		parameters, ok := definition.Parameters["type"]
		if !ok || parameters != "object" {
			t.Errorf("tool %q parameters schema = %#v, want an object schema", definition.Name, definition.Parameters)
		}
		if _, ok := definition.Parameters["properties"].(map[string]any); !ok {
			t.Errorf("tool %q schema has no properties object", definition.Name)
		}
		registration, ok := harness.tools.tools[definition.Name]
		if !ok || registration.argumentType == nil || registration.argumentType.Kind() != reflect.Struct {
			t.Errorf("tool %q has no typed argument registration", definition.Name)
		} else if generated := schemaForType(registration.argumentType); !reflect.DeepEqual(generated, definition.Parameters) {
			t.Errorf("tool %q schema does not match its registered argument type", definition.Name)
		}
	}

	nodeConverse := byName["node_converse"].Parameters
	if got, want := nodeConverse["required"], []string{"node_id", "message"}; !reflect.DeepEqual(got, want) {
		t.Errorf("node_converse required = %#v, want %#v", got, want)
	}
	properties := nodeConverse["properties"].(map[string]any)
	if properties["message"].(map[string]any)["description"] == "" {
		t.Error("node_converse message description was not generated from its argument type")
	}

	workflow := byName["execute_workflow"].Parameters["properties"].(map[string]any)
	subtasks := workflow["subtasks"].(map[string]any)
	items := subtasks["items"].(map[string]any)
	subtaskProperties := items["properties"].(map[string]any)
	role := subtaskProperties["role"].(map[string]any)
	if got, want := role["enum"], []string{"researcher", "analyst", "coder", "writer", "reviewer", "summarizer", "translator", "general"}; !reflect.DeepEqual(got, want) {
		t.Errorf("workflow role enum = %#v, want %#v", got, want)
	}
	if got, want := items["required"], []string{"description"}; !reflect.DeepEqual(got, want) {
		t.Errorf("workflow subtask required = %#v, want %#v", got, want)
	}

	sessionMemory := byName["session_memory"].Parameters["properties"].(map[string]any)
	action := sessionMemory["action"].(map[string]any)
	if got, want := action["enum"], []string{"get", "set", "list", "delete"}; !reflect.DeepEqual(got, want) {
		t.Errorf("session_memory action enum = %#v, want %#v", got, want)
	}
}
