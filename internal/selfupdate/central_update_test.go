package selfupdate

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestReleaseCatalogRequiresEverySupportedPlatformAndSafePaths(t *testing.T) {
	catalog := centralCatalogFixture("9.0.0")
	if err := catalog.Validate(); err != nil {
		t.Fatalf("complete catalog rejected: %v", err)
	}
	catalog.Artifacts = catalog.Artifacts[:len(catalog.Artifacts)-1]
	if err := catalog.Validate(); err == nil {
		t.Fatal("catalog missing a platform was accepted")
	}

	catalog = centralCatalogFixture("9.0.0")
	catalog.Artifacts[0].Path = "../replacement.exe"
	if err := catalog.Validate(); err == nil {
		t.Fatal("catalog path traversal was accepted")
	}
}

func TestReleaseCatalogSignatureCoversAllArtifacts(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	catalog := centralCatalogFixture("9.0.0")
	if err := catalog.Sign(privateKey); err != nil {
		t.Fatal(err)
	}
	if err := catalog.VerifySignature(publicKey); err != nil {
		t.Fatalf("signed catalog rejected: %v", err)
	}
	catalog.Artifacts[0].Path += ".tampered"
	if err := catalog.VerifySignature(publicKey); err == nil {
		t.Fatal("modified catalog passed signature verification")
	}
}

func TestFetchReleaseCatalogRequiresHTTPSAndTrustedSignature(t *testing.T) {
	publicKey, catalog := signedCentralCatalog(t, "9.0.0")
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(catalog)
	}))
	defer server.Close()

	got, err := FetchReleaseCatalog(context.Background(), server.Client(), server.URL+"/latest.json", publicKey)
	if err != nil || got.Version != catalog.Version {
		t.Fatalf("FetchReleaseCatalog() = %+v, %v", got, err)
	}
	wrongKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := FetchReleaseCatalog(context.Background(), server.Client(), server.URL+"/latest.json", wrongKey); err == nil {
		t.Fatal("catalog signed by another key was accepted")
	}
	if _, err := FetchReleaseCatalog(context.Background(), server.Client(), "http://updates.example.test/latest.json", publicKey); err == nil {
		t.Fatal("plain HTTP update manifest URL was accepted")
	}
}

func TestDownloadCentralArtifactVerifiesDigestAndReportedVersion(t *testing.T) {
	if _, ok := centralCatalogFixture("9.0.0").Artifact(runtime.GOOS, runtime.GOARCH); !ok {
		t.Skipf("unsupported test host %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	fixture := buildReleaseFixture(t, "9.0.0")
	fixtureData, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	fixtureManifest, err := Inspect(fixture, "9.0.0")
	if err != nil {
		t.Fatal(err)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	catalog := centralCatalogFixture("9.0.0")
	for i := range catalog.Artifacts {
		if catalog.Artifacts[i].OS == runtime.GOOS && catalog.Artifacts[i].Arch == runtime.GOARCH {
			catalog.Artifacts[i].SHA256 = fixtureManifest.SHA256
			catalog.Artifacts[i].Size = fixtureManifest.Size
		}
	}
	if err := catalog.Sign(privateKey); err != nil {
		t.Fatal(err)
	}
	_, ok := catalog.Artifact(runtime.GOOS, runtime.GOARCH)
	if !ok {
		t.Fatalf("catalog lacks the test host platform %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(fixtureData)
	}))
	defer server.Close()

	targetName := "maistr0"
	if runtime.GOOS == "windows" {
		targetName += ".exe"
	}
	target := filepath.Join(t.TempDir(), targetName)
	update := CentralUpdateRequest{ManifestURL: server.URL + "/latest.json", Catalog: catalog}
	staged, received, err := DownloadCentralArtifact(context.Background(), server.Client(), update, target, publicKey)
	if err != nil {
		t.Fatalf("DownloadCentralArtifact() failed: %v", err)
	}
	if !sameArtifact(received, fixtureManifest) {
		t.Fatalf("received manifest = %+v, want %+v", received, fixtureManifest)
	}
	if err := os.Remove(staged); err != nil {
		t.Fatal(err)
	}

	badDigest := catalog
	badDigest.Artifacts = append([]ReleaseArtifact(nil), catalog.Artifacts...)
	for i := range badDigest.Artifacts {
		if badDigest.Artifacts[i].OS == runtime.GOOS && badDigest.Artifacts[i].Arch == runtime.GOARCH {
			badDigest.Artifacts[i].SHA256 = strings.Repeat("0", 64)
		}
	}
	if err := badDigest.Sign(privateKey); err != nil {
		t.Fatal(err)
	}
	if _, _, err := DownloadCentralArtifact(context.Background(), server.Client(),
		CentralUpdateRequest{ManifestURL: server.URL + "/latest.json", Catalog: badDigest}, target, publicKey); err == nil {
		t.Fatal("artifact with a mismatched digest was accepted")
	}

	wrongVersion := centralCatalogFixture("9.0.1")
	for i := range wrongVersion.Artifacts {
		if wrongVersion.Artifacts[i].OS == runtime.GOOS && wrongVersion.Artifacts[i].Arch == runtime.GOARCH {
			wrongVersion.Artifacts[i].SHA256 = fixtureManifest.SHA256
			wrongVersion.Artifacts[i].Size = fixtureManifest.Size
		}
	}
	if err := wrongVersion.Sign(privateKey); err != nil {
		t.Fatal(err)
	}
	if _, _, err := DownloadCentralArtifact(context.Background(), server.Client(),
		CentralUpdateRequest{ManifestURL: server.URL + "/latest.json", Catalog: wrongVersion}, target, publicKey); err == nil {
		t.Fatal("artifact reporting a different release version was accepted")
	}
}

func TestCentralUpdatePollsSelfThenResumesClusterRollout(t *testing.T) {
	publicKey, catalog := signedCentralCatalog(t, "9.0.1")
	var requests int
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		_ = json.NewEncoder(w).Encode(catalog)
	}))
	defer server.Close()

	executable := filepath.Join(t.TempDir(), "maistr0")
	coordinator := &centralUpdateTestCoordinator{elected: true, nodesBehind: true}
	var queued UpgradeRequest
	options := CentralUpdateOptions{
		ManifestURL:        server.URL + "/latest.json",
		PublisherPublicKey: publicKey,
		Distribution:       Distribution{Executable: executable, Version: "9.0.0"},
		Client:             server.Client(),
		Coordinator:        func() CentralUpdateCoordinator { return coordinator },
		QueueSelfUpdate: func(_ context.Context, request UpgradeRequest) error {
			queued = request
			return nil
		},
	}
	if err := pollCentralUpdate(context.Background(), options); err != nil {
		t.Fatal(err)
	}
	if queued.Central == nil || queued.Central.Catalog.Version != catalog.Version {
		t.Fatalf("self-update request = %+v, want central release %s", queued, catalog.Version)
	}
	if len(coordinator.broadcasts) != 0 {
		t.Fatalf("cluster received update before orchestrator restart: %+v", coordinator.broadcasts)
	}
	if _, err := os.Stat(executable + ".central-update-pending"); err != nil {
		t.Fatalf("pending rollout was not persisted: %v", err)
	}

	options.Distribution.Version = catalog.Version
	if err := pollCentralUpdate(context.Background(), options); err != nil {
		t.Fatal(err)
	}
	if requests != 1 || len(coordinator.broadcasts) != 1 ||
		coordinator.broadcasts[0].Central.Catalog.Version != catalog.Version {
		t.Fatalf("post-restart rollout: fetches=%d broadcasts=%+v", requests, coordinator.broadcasts)
	}
	if _, err := os.Stat(executable + ".central-update-pending"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed rollout marker remains: %v", err)
	}
}

func signedCentralCatalog(t *testing.T, release string) (ed25519.PublicKey, ReleaseCatalog) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	catalog := centralCatalogFixture(release)
	if err := catalog.Sign(privateKey); err != nil {
		t.Fatal(err)
	}
	return publicKey, catalog
}

func centralCatalogFixture(release string) ReleaseCatalog {
	catalog := ReleaseCatalog{Version: release}
	for _, target := range centralUpdateTargets {
		catalog.Artifacts = append(catalog.Artifacts, ReleaseArtifact{
			OS: target.OS, Arch: target.Arch,
			Path:   "v" + release + "/" + target.Folder + "/" + target.Name,
			SHA256: strings.Repeat("a", 64),
			Size:   1024,
		})
	}
	return catalog
}

type centralUpdateTestCoordinator struct {
	elected     bool
	nodesBehind bool
	broadcasts  []UpgradeRequest
}

func (coordinator *centralUpdateTestCoordinator) Elected() bool {
	return coordinator.elected
}

func (coordinator *centralUpdateTestCoordinator) HasNodesBehind(string) bool {
	return coordinator.nodesBehind
}

func (coordinator *centralUpdateTestCoordinator) BroadcastCentralUpdate(_ context.Context, request UpgradeRequest) error {
	coordinator.broadcasts = append(coordinator.broadcasts, request)
	return nil
}
