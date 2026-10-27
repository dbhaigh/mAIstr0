package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/discovery"
)

func TestPrintNormalizedConfigRedactsDiscoveryAndPublisherKeys(t *testing.T) {
	discoveryKey := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("d", 32)))
	publisherKey := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("p", 32)))
	nodePath := filepath.Join(t.TempDir(), "node.json")
	orchestratorPath := filepath.Join(t.TempDir(), "orchestrator.json")
	nodeConfig := []byte(`{"discovery_hmac_secret":"` + discoveryKey + `","peer_update_publisher_key":"` + publisherKey + `"}`)
	orchestratorConfig := []byte(`{"discovery_hmac_secret":"` + discoveryKey + `"}`)
	if err := os.WriteFile(nodePath, nodeConfig, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(orchestratorPath, orchestratorConfig, 0o600); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	if err := printNormalizedConfig(&output, configPrintOptions{
		role: "both", nodeConfigPath: nodePath, orchConfigPath: orchestratorPath,
	}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{discoveryKey, publisherKey} {
		if strings.Contains(output.String(), key) {
			t.Fatal("normalized configuration exposed a configured security key")
		}
	}
	if strings.Count(output.String(), "[REDACTED]") < 3 {
		t.Fatalf("normalized configuration omitted security key redactions: %s", output.String())
	}
}

func TestStartupElectionWindowCoversDiscoveryLease(t *testing.T) {
	if startupElectionWindow < 15*time.Second {
		t.Fatalf("startup election window = %s, want at least the 15-second discovery lease", startupElectionWindow)
	}
}

func TestPrintNormalizedConfigWritesEffectiveNodeSettings(t *testing.T) {
	var output bytes.Buffer
	err := printNormalizedConfig(&output, configPrintOptions{
		role: "node", nodeListenAddr: ":7551", noBrowser: true,
	})
	if err != nil {
		t.Fatalf("print normalized node config: %v", err)
	}
	var result struct {
		Node config.NodeConfig `json:"node"`
	}
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatalf("decode printed config: %v", err)
	}
	if result.Node.ListenAddr != ":7551" || result.Node.MaxConcurrentTasks != config.DefaultNodeConcurrency ||
		result.Node.AutoOpenBrowser || result.Node.NodeID == "" || result.Node.AdvertiseAddr == "" {
		t.Fatalf("printed node config = %+v", result.Node)
	}
}

func TestPrintNormalizedConfigRedactsEngineAPIKeys(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "node.json")
	const apiKey = "never-print-this-key"
	if err := os.WriteFile(configPath, []byte(`{"engines":[{"name":"vllm","url":"http://127.0.0.1:8000","api_key":"`+apiKey+`"}]}`), 0600); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	if err := printNormalizedConfig(&output, configPrintOptions{
		role: "node", nodeConfigPath: configPath,
	}); err != nil {
		t.Fatalf("print normalized config: %v", err)
	}
	if strings.Contains(output.String(), apiKey) || !strings.Contains(output.String(), `"api_key": "[REDACTED]"`) {
		t.Fatalf("printed config does not safely redact the engine key: %s", output.String())
	}
}

func TestPrintNormalizedConfigRejectsInvalidConfiguration(t *testing.T) {
	var output bytes.Buffer
	err := printNormalizedConfig(&output, configPrintOptions{
		role: "orchestrator", noDiscovery: true,
	})
	if err == nil || !strings.Contains(err.Error(), "discovery_enabled must be true") {
		t.Fatalf("invalid normalized config error = %v, want discovery validation error", err)
	}
	if output.Len() != 0 {
		t.Fatalf("invalid configuration printed partial output: %s", output.String())
	}
}

func TestPrintNormalizedConfigRedactsPINAndURLCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.json")
	const raw = `{"pairing_pin":"012345","engines":[{"name":"ollama","url":"https://private-user:private-password@example.test/v1?opaque=private-query#private-fragment","registry_url":"https://private-registry:private-key@example.test/models?opaque=private-metadata"}]}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"node", "both"} {
		t.Run(role, func(t *testing.T) {
			var output bytes.Buffer
			if err := printNormalizedConfig(&output, configPrintOptions{
				role: role, nodeConfigPath: path, orchestratorFingerprint: strings.Repeat("a", 64),
			}); err != nil {
				t.Fatal(err)
			}
			for _, secret := range []string{"012345", "private-user", "private-password", "private-query", "private-fragment", "private-registry", "private-key", "private-metadata"} {
				if strings.Contains(output.String(), secret) {
					t.Fatalf("normalized config exposed %q", secret)
				}
			}
			if !strings.Contains(output.String(), strings.Repeat("a", 64)) || !strings.Contains(output.String(), "https://example.test/v1") {
				t.Fatal("public fingerprint or useful endpoint path was removed")
			}
		})
	}
	original := config.NodeConfig{PairingPIN: "012345", Engines: []config.EngineConfig{{APIKey: "private-api-key", URL: "https://private-user:private-password@example.test"}}}
	if _, err := redactNodeConfig(original); err != nil {
		t.Fatal(err)
	}
	if original.Engines[0].APIKey != "private-api-key" || original.PairingPIN != "012345" {
		t.Fatal("redaction changed executable configuration")
	}
}

func TestStartupOrchestratorWinnerPrefersActiveCoordinatorInAutoMode(t *testing.T) {
	candidate := discovery.Announcement{
		Role: "candidate", ID: "new-node", PeerAddr: "https://new-node:7450", Score: 100,
	}
	peers := []discovery.Peer{
		{Announcement: discovery.Announcement{
			Role: "candidate", ID: "another-starting-node", PeerAddr: "https://other:7450", Score: 200,
		}},
		{Announcement: discovery.Announcement{
			Role: "orchestrator", ID: "existing-coordinator", PeerAddr: "https://leader:7450", Score: 10,
		}},
	}

	winner := startupOrchestratorWinner(candidate, peers, true)
	if winner.ID != "existing-coordinator" || winner.PeerAddr != "https://leader:7450" {
		t.Fatalf("automatic startup winner = %#v, want existing coordinator", winner)
	}
}

func TestStartupOrchestratorWinnerElectsAmongCandidatesWhenNoCoordinatorIsActive(t *testing.T) {
	candidate := discovery.Announcement{
		Role: "candidate", ID: "new-node", PeerAddr: "https://new-node:7450", Score: 100,
	}
	peers := []discovery.Peer{{
		Announcement: discovery.Announcement{
			Role: "candidate", ID: "another-starting-node", PeerAddr: "https://other:7450", Score: 200,
		},
	}}

	winner := startupOrchestratorWinner(candidate, peers, true)
	if winner.ID != "another-starting-node" {
		t.Fatalf("automatic startup winner = %#v, want highest-ranked starting candidate", winner)
	}
}

func TestExplicitOrchestratorStartupCanStillChallengeActiveCoordinator(t *testing.T) {
	candidate := discovery.Announcement{
		Role: "candidate", ID: "new-node", PeerAddr: "https://new-node:7450", Score: 100,
	}
	peers := []discovery.Peer{{
		Announcement: discovery.Announcement{
			Role: "orchestrator", ID: "existing-coordinator", PeerAddr: "https://leader:7450", Score: 10,
		},
	}}

	winner := startupOrchestratorWinner(candidate, peers, false)
	if winner.ID != "new-node" {
		t.Fatalf("explicit orchestrator startup winner = %#v, want new-node", winner)
	}
}
