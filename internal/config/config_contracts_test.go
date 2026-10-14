package config

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

var updateConfigContracts = flag.Bool("update", false, "update config contract golden files")

func TestConfigDefaultsAndOverlaysContract(t *testing.T) {
	nodeDefaults := DefaultNodeConfig()
	orchestratorDefaults := DefaultOrchestratorConfig()
	assertConfigGolden(t, "defaults.json", map[string]any{
		"node":         nodeDefaults,
		"orchestrator": orchestratorDefaults,
	})

	directory := t.TempDir()
	nodePath := filepath.Join(directory, "node.json")
	if err := os.WriteFile(nodePath, []byte(`{"node_id":"worker-17","max_concurrent_tasks":5}`), 0o600); err != nil {
		t.Fatal(err)
	}
	node, err := LoadNodeConfig(nodePath)
	if err != nil {
		t.Fatal(err)
	}
	if node.ListenAddr != nodeDefaults.ListenAddr || node.DiscoveryEnabled != nodeDefaults.DiscoveryEnabled ||
		node.MaxConcurrentTasks != 5 || node.NodeID != "worker-17" {
		t.Fatalf("partial node config did not overlay defaults: %+v", node)
	}

	orchestratorPath := filepath.Join(directory, "orchestrator.json")
	if err := os.WriteFile(orchestratorPath, []byte(`{"harness":"deepseek","max_concurrent_dispatches":7}`), 0o600); err != nil {
		t.Fatal(err)
	}
	orchestrator, err := LoadOrchestratorConfig(orchestratorPath)
	if err != nil {
		t.Fatal(err)
	}
	if orchestrator.ListenAddr != orchestratorDefaults.ListenAddr || !orchestrator.DiscoveryEnabled ||
		orchestrator.Harness != "deepseek" || orchestrator.MaxConcurrentDispatches != 7 {
		t.Fatalf("partial orchestrator config did not overlay defaults: %+v", orchestrator)
	}
	assertConfigGolden(t, "overlays.json", map[string]any{"node": node, "orchestrator": orchestrator})
}

func TestConfigMissingFilesKeepDefaults(t *testing.T) {
	directory := t.TempDir()
	node, err := LoadNodeConfig(filepath.Join(directory, "missing-node.json"))
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := LoadOrchestratorConfig(filepath.Join(directory, "missing-orchestrator.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(node, DefaultNodeConfig()) {
		t.Fatalf("missing node config = %+v, want defaults", node)
	}
	if !reflect.DeepEqual(orchestrator, DefaultOrchestratorConfig()) {
		t.Fatalf("missing orchestrator config = %+v, want defaults", orchestrator)
	}
}

func TestConfigMalformedFilesReturnErrors(t *testing.T) {
	directory := t.TempDir()
	for _, test := range []struct {
		name string
		load func(string) error
	}{
		{"node", func(path string) error { _, err := LoadNodeConfig(path); return err }},
		{"orchestrator", func(path string) error { _, err := LoadOrchestratorConfig(path); return err }},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(directory, test.name+".json")
			if err := os.WriteFile(path, []byte(`{"broken":`), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := test.load(path); err == nil {
				t.Fatal("malformed JSON was accepted")
			}
		})
	}
}

func assertConfigGolden(t *testing.T, name string, value any) {
	t.Helper()
	actual, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	actual = append(actual, '\n')
	path := filepath.Join("testdata", "contracts", name)
	if *updateConfigContracts {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, actual, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	expected, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read config contract %s (use -update to create it): %v", path, err)
	}
	if !bytes.Equal(actual, expected) {
		t.Errorf("config contract %s changed\n--- got ---\n%s\n--- want ---\n%s", name, actual, expected)
	}
}
