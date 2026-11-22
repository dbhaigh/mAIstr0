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
	"github.com/maistr0/maistr0/internal/version"
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

func TestUpgradeHandlerQueuesCentralReleaseAndSkipsCurrentVersion(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "central-upgrade-handler-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	var queued []selfupdate.UpgradeRequest
	agent.SetUpgradeRequester(func(_ context.Context, request selfupdate.UpgradeRequest) error {
		queued = append(queued, request)
		return nil
	})
	request := centralUpgradeHandlerRequest("99.0.0")
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	agent.handleUpgrade(response, httptest.NewRequest(http.MethodPost, "/api/upgrade", bytes.NewReader(body)))
	if response.Code != http.StatusAccepted || len(queued) != 1 || queued[0].Central == nil {
		t.Fatalf("central update request: status=%d queued=%+v body=%s", response.Code, queued, response.Body.String())
	}

	request = centralUpgradeHandlerRequest(version.String())
	body, err = json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	current := httptest.NewRecorder()
	agent.handleUpgrade(current, httptest.NewRequest(http.MethodPost, "/api/upgrade", bytes.NewReader(body)))
	if current.Code != http.StatusOK || len(queued) != 1 {
		t.Fatalf("current release request: status=%d queued=%+v body=%s", current.Code, queued, current.Body.String())
	}
}

func TestUpgradeHandlerRejectsDisabledCentralUpdates(t *testing.T) {
	agent := mustNewTestAgent(t, config.NodeConfig{
		NodeID: "central-upgrade-disabled-test", MemoryPath: filepath.Join(t.TempDir(), "node.db"),
	})
	agent.SetUpgradeRequester(func(context.Context, selfupdate.UpgradeRequest) error {
		return selfupdate.ErrCentralUpdatesDisabled
	})
	body, err := json.Marshal(centralUpgradeHandlerRequest("99.0.0"))
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	agent.handleUpgrade(response, httptest.NewRequest(http.MethodPost, "/api/upgrade", bytes.NewReader(body)))
	if response.Code != http.StatusForbidden {
		t.Fatalf("disabled central update status = %d, want 403: %s", response.Code, response.Body.String())
	}
}

func centralUpgradeHandlerRequest(release string) selfupdate.UpgradeRequest {
	return selfupdate.UpgradeRequest{Central: &selfupdate.CentralUpdateRequest{
		ManifestURL: "https://updates.example.test/latest.json",
		Catalog: selfupdate.ReleaseCatalog{
			Version: release, Signature: strings.Repeat("a", 86) + "==",
			Artifacts: []selfupdate.ReleaseArtifact{
				{OS: "windows", Arch: "amd64", Path: "v" + release + "/windows-amd64/maistr0.exe", SHA256: strings.Repeat("a", 64), Size: 1024},
				{OS: "linux", Arch: "amd64", Path: "v" + release + "/linux-amd64/maistr0", SHA256: strings.Repeat("a", 64), Size: 1024},
				{OS: "linux", Arch: "arm64", Path: "v" + release + "/linux-arm64/maistr0", SHA256: strings.Repeat("a", 64), Size: 1024},
				{OS: "darwin", Arch: "amd64", Path: "v" + release + "/darwin-amd64/maistr0", SHA256: strings.Repeat("a", 64), Size: 1024},
				{OS: "darwin", Arch: "arm64", Path: "v" + release + "/darwin-arm64/maistr0", SHA256: strings.Repeat("a", 64), Size: 1024},
			},
		},
	}}
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
