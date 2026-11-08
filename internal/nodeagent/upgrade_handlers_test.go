package nodeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/cluster"
	"github.com/maistr0/maistr0/internal/config"
	"github.com/maistr0/maistr0/internal/selfupdate"
)

func TestUpgradeHandlerValidatesAndQueuesManualRequest(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "upgrade-handler-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	if err := agent.InitializeIdentity(); err != nil {
		t.Fatal(err)
	}
	var queued []selfupdate.UpgradeRequest
	agent.SetUpgradeRequester(func(_ context.Context, request selfupdate.UpgradeRequest) error {
		queued = append(queued, request)
		if request.PeerID != "" {
			return selfupdate.ErrPeerUpdatesDisabled
		}
		return nil
	})

	path := filepath.Join(t.TempDir(), "replacement.exe")
	requestBody, err := json.Marshal(selfupdate.UpgradeRequest{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/upgrade", bytes.NewReader(requestBody))
	response := httptest.NewRecorder()
	agent.handleUpgrade(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("local upgrade request status = %d, want 202: %s", response.Code, response.Body.String())
	}
	if len(queued) != 1 || queued[0].Path != path {
		t.Fatalf("queued requests = %+v, want the local path request", queued)
	}

	invalid := httptest.NewRequest(http.MethodPost, "/api/upgrade", strings.NewReader(`{"path":"relative.exe"}`))
	invalidResponse := httptest.NewRecorder()
	agent.handleUpgrade(invalidResponse, invalid)
	if invalidResponse.Code != http.StatusBadRequest {
		t.Fatalf("relative upgrade path status = %d, want 400", invalidResponse.Code)
	}
	if len(queued) != 1 {
		t.Fatalf("invalid upgrade was queued: %+v", queued)
	}

	peerBody, err := json.Marshal(selfupdate.UpgradeRequest{PeerID: "trusted-peer"})
	if err != nil {
		t.Fatal(err)
	}
	peerRequest := httptest.NewRequest(http.MethodPost, "/api/upgrade", bytes.NewReader(peerBody))
	peerResponse := httptest.NewRecorder()
	agent.handleUpgrade(peerResponse, peerRequest)
	if peerResponse.Code != http.StatusForbidden {
		t.Fatalf("disabled peer upgrade status = %d, want 403", peerResponse.Code)
	}
}

func TestUpgradeHandlerReportsQueueConflicts(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "upgrade-handler-conflict-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	agent.SetUpgradeRequester(func(context.Context, selfupdate.UpgradeRequest) error {
		return selfupdate.ErrUpgradeInProgress
	})
	body, err := json.Marshal(selfupdate.UpgradeRequest{Path: filepath.Join(t.TempDir(), "replacement.exe")})
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, "/api/upgrade", bytes.NewReader(body))
	response := httptest.NewRecorder()
	agent.handleUpgrade(response, request)
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), selfupdate.ErrUpgradeInProgress.Error()) {
		t.Fatalf("queue conflict response = %d %q", response.Code, response.Body.String())
	}
}

func TestUpgradeStatusIsReportedToNodeAndLifecycleSnapshots(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "upgrade-status-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	agent.setUpgradeStatus(cluster.UpgradeStatus{
		State: "running", Phase: "requesting_rejoin_ticket", Progress: 75,
		TicketStatus: "requesting",
	})

	nodeStatus := agent.status()
	if nodeStatus.UpgradeStatus == nil || nodeStatus.UpgradeStatus.State != "running" ||
		nodeStatus.UpgradeStatus.TicketStatus != "requesting" ||
		nodeStatus.UpgradeStatus.UpdatedAt.IsZero() {
		t.Fatalf("node status upgrade state = %+v", nodeStatus.UpgradeStatus)
	}
	select {
	case <-agent.orchestratorLink.statusUpdates:
	default:
		t.Fatal("upgrade status change did not request an immediate registration")
	}

	response := httptest.NewRecorder()
	agent.handleLifecycleStatus(response, httptest.NewRequest(http.MethodGet, "/api/lifecycle/status", nil))
	var lifecycle lifecycleSnapshot
	if err := json.Unmarshal(response.Body.Bytes(), &lifecycle); err != nil {
		t.Fatal(err)
	}
	if lifecycle.UpgradeStatus == nil || lifecycle.UpgradeStatus.State != "running" {
		t.Fatalf("lifecycle upgrade state = %+v", lifecycle.UpgradeStatus)
	}
}
