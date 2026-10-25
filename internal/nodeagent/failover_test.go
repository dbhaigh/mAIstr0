package nodeagent

import (
	"path/filepath"
	"testing"

	"github.com/maistr0/maistr0/internal/config"
)

func TestPromoteToOrchestratorKeepsNodeIdentity(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID:     "promoted-node",
		MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := agent.Close(); err != nil {
			t.Errorf("close agent: %v", err)
		}
	})

	const address = "https://192.0.2.15:7450"
	agent.BeginElection(address)
	if err := agent.PromoteToOrchestrator(address); err != nil {
		t.Fatal(err)
	}
	if agent.getOrchestratorID() != agent.ID() || agent.getOrchestratorAddr() != address {
		t.Fatalf("promoted coordinator state = (%q, %q)", agent.getOrchestratorID(), agent.getOrchestratorAddr())
	}
	if agent.localOrchestratorIDValue() != agent.ID() {
		t.Fatalf("local coordinator ID = %q, want %q", agent.localOrchestratorIDValue(), agent.ID())
	}
	if !agent.identity.IsTrustedOrchestratorID(agent.ID()) {
		t.Fatal("promoted node identity was not trusted as the local orchestrator")
	}
	if got := agent.orchestratorLink.electionCandidateAddrValue(); got != "" {
		t.Fatalf("candidate announcement address = %q after promotion, want empty", got)
	}

	const winnerAddr = "https://192.0.2.20:7450"
	agent.DemoteFromOrchestrator("orchestrator-original", winnerAddr)
	if agent.localOrchestratorIDValue() != "" {
		t.Fatalf("local coordinator ID = %q after demotion, want empty", agent.localOrchestratorIDValue())
	}
	if agent.getOrchestratorID() != "orchestrator-original" || agent.getOrchestratorAddr() != winnerAddr {
		t.Fatalf("demoted coordinator state = (%q, %q)", agent.getOrchestratorID(), agent.getOrchestratorAddr())
	}
	if agent.isJoined() {
		t.Fatal("demoted node still reports a joined orchestrator before re-registering")
	}
}
