package selfupdate

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

func TestDistributionRequiresPairedIdentity(t *testing.T) {
	publisher, err := peeridentity.Open(filepath.Join(t.TempDir(), "publisher"), "publisher")
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := peeridentity.Open(filepath.Join(t.TempDir(), "receiver"), "receiver")
	if err != nil {
		t.Fatal(err)
	}
	if err := receiver.TrustPeer("publisher", publisher.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	executable, _ := os.Executable()
	distribution := Distribution{Executable: executable, Version: "99.0.0"}
	server := httptest.NewUnstartedServer(publisher.RequireTrustedPeer(distribution))
	server.TLS = publisher.ServerTLSConfig()
	server.StartTLS()
	defer server.Close()
	transport := &http.Transport{TLSClientConfig: receiver.PinnedClientTLSConfig("publisher")}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second}
	if response, err := client.Get(server.URL + "/api/updates/manifest"); err == nil {
		_ = response.Body.Close()
		if response.StatusCode != http.StatusForbidden {
			t.Fatalf("unpaired receiver got %s", response.Status)
		}
	}
	if err := publisher.TrustPeer("receiver", receiver.CertificatePEM()); err != nil {
		t.Fatal(err)
	}
	transport.CloseIdleConnections()
	response, err := client.Get(server.URL + "/api/updates/manifest?os=" + runtime.GOOS + "&arch=" + runtime.GOARCH)
	if err != nil {
		t.Fatal(err)
	}
	defer closeutil.Discard(response.Body)
	var manifest Manifest
	if err := json.NewDecoder(response.Body).Decode(&manifest); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 || manifest.Version != "99.0.0" {
		t.Fatalf("paired offer: %s %+v", response.Status, manifest)
	}
}

func TestGateFencesNewWorkWithoutInterruptingRequests(t *testing.T) {
	var gate Gate
	release, ok := gate.Enter()
	if !ok || gate.Freeze() {
		t.Fatal("froze while request was active")
	}
	release()
	if !gate.Freeze() {
		t.Fatal("idle gate did not freeze")
	}
	handler := gate.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
	for _, tc := range []struct {
		method string
		want   int
	}{{"POST", 503}, {"GET", 204}, {"DELETE", 503}} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(tc.method, "/", nil))
		if response.Code != tc.want {
			t.Fatalf("%s got %d", tc.method, response.Code)
		}
	}
	gate.Thaw()
	release, ok = gate.Enter()
	if !ok {
		t.Fatal("thaw did not reopen admission")
	}
	release()
}

func TestManifestValidation(t *testing.T) {
	valid := Manifest{Version: "0.0.24.1", OS: "linux", Arch: "amd64", SHA256: strings.Repeat("a", 64), Size: 1024}
	for _, tc := range []struct {
		name   string
		change func(*Manifest)
	}{
		{"invalid version", func(m *Manifest) { m.Version = "latest" }},
		{"too many components", func(m *Manifest) { m.Version = "0.0.24.1.2" }},
		{"empty component", func(m *Manifest) { m.Version = "0.0.24." }},
		{"oversized", func(m *Manifest) { m.Size = MaxBinaryBytes + 1 }},
		{"empty", func(m *Manifest) { m.Size = 0 }},
		{"digest", func(m *Manifest) { m.SHA256 = "abc" }},
		{"platform", func(m *Manifest) { m.OS = "../windows" }},
		{"arch", func(m *Manifest) { m.Arch = "unknown" }},
		{"windows arm64", func(m *Manifest) { m.OS, m.Arch = "windows", "arm64" }},
		{"linux arm64", func(m *Manifest) { m.OS, m.Arch = "linux", "arm64" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := valid
			tc.change(&m)
			if m.Validate() == nil {
				t.Fatal("accepted invalid manifest")
			}
		})
	}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, arch := range []string{"amd64", "arm64"} {
		manifest := valid
		manifest.OS, manifest.Arch = "darwin", arch
		if err := manifest.Validate(); err != nil {
			t.Fatalf("valid darwin/%s manifest rejected: %v", arch, err)
		}
	}
}

func TestReplacementLockSerializesConcurrentHelpers(t *testing.T) {
	target := filepath.Join(t.TempDir(), "maistr0")
	release, acquired, err := acquireReplacementLock(target)
	if err != nil || !acquired {
		t.Fatalf("first lock = (%v, %v), want (true, nil)", acquired, err)
	}
	if secondRelease, secondAcquired, err := acquireReplacementLock(target); err != nil || secondAcquired {
		if secondRelease != nil {
			_ = secondRelease()
		}
		t.Fatalf("concurrent lock = (%v, %v), want (false, nil)", secondAcquired, err)
	}
	if err := release(); err != nil {
		t.Fatal(err)
	}
	nextRelease, nextAcquired, err := acquireReplacementLock(target)
	if err != nil || !nextAcquired {
		t.Fatalf("lock after release = (%v, %v), want (true, nil)", nextAcquired, err)
	}
	if err := nextRelease(); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactManifestRollbackRestoresPreviousSidecar(t *testing.T) {
	target := filepath.Join(t.TempDir(), "maistr0.exe")
	path := target + ".peer.json"
	previous := Manifest{
		Version: "0.1.1.6", OS: runtime.GOOS, Arch: runtime.GOARCH,
		SHA256: strings.Repeat("a", 64), Size: 1,
	}
	next := previous
	next.Version = "0.1.1.7"
	next.SHA256 = strings.Repeat("b", 64)
	previousData, err := json.Marshal(previous)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, previousData, 0600); err != nil {
		t.Fatal(err)
	}
	rollback, err := installArtifactManifest(target, next)
	if err != nil {
		t.Fatal(err)
	}
	currentData, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var current Manifest
	if err := json.Unmarshal(currentData, &current); err != nil || current != next {
		t.Fatalf("installed sidecar = %+v, error %v; want %+v", current, err, next)
	}
	if err := rollback(); err != nil {
		t.Fatal(err)
	}
	restoredData, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var restored Manifest
	if err := json.Unmarshal(restoredData, &restored); err != nil || restored != previous {
		t.Fatalf("restored sidecar = %+v, error %v; want %+v", restored, err, previous)
	}
}

func TestNewestPeerReleasesSelectsOnlyTheHighestRelease(t *testing.T) {
	offers := []peerRelease{
		{peer: Peer{ID: "older"}, manifest: Manifest{Version: "0.1.1.3"}},
		{peer: Peer{ID: "intermediate"}, manifest: Manifest{Version: "0.1.1.4"}},
		{peer: Peer{ID: "newest-a"}, manifest: Manifest{Version: "0.1.1.5"}},
		{peer: Peer{ID: "newest-b"}, manifest: Manifest{Version: "0.1.1.5"}},
	}

	got := newestPeerReleases("0.1.1.3", offers)
	if len(got) != 2 || got[0].peer.ID != "newest-a" || got[1].peer.ID != "newest-b" {
		t.Fatalf("newestPeerReleases() = %+v, want only both 0.1.1.5 publishers", got)
	}
	if got := newestPeerReleases("0.1.1.5", offers); len(got) != 0 {
		t.Fatalf("newestPeerReleases() at the cluster maximum = %+v, want no update", got)
	}
}

func TestDistributionUsesRunningExecutableVersionAsUpdateBaseline(t *testing.T) {
	if testing.Short() {
		t.Skip("version probe fixture builds an executable")
	}
	extension := ""
	if runtime.GOOS == "windows" {
		extension = ".exe"
	}
	executable := filepath.Join(t.TempDir(), "maistr0"+extension)
	command := exec.Command("go", "build", "-ldflags", "-X main.release=9.0.1", "-o", executable, "."+string(filepath.Separator)+"testdata"+string(filepath.Separator)+"helper")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build version probe fixture: %v\n%s", err, output)
	}
	distribution, err := distributionForExecutable(executable)
	if err != nil {
		t.Fatal(err)
	}
	if distribution.Version != "9.0.1" {
		t.Fatalf("running executable version = %q, want 9.0.1", distribution.Version)
	}
	offers := []peerRelease{
		{manifest: Manifest{Version: "9.0.1"}},
		{manifest: Manifest{Version: "9.0.2"}},
	}
	if got := newestPeerReleases(distribution.Version, offers); len(got) != 1 || got[0].manifest.Version != "9.0.2" {
		t.Fatalf("updates after restart = %+v, want only a newer 9.0.2 release", got)
	}
}

func TestDownloadVerifiesBytesVersionPlatformAndCleansPartialFiles(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	valid, err := Inspect(executable, "99.0.0")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []string{"valid", "corrupt", "truncated", "oversized", "same-version", "wrong-platform", "redirect", "cancelled"} {
		t.Run(tc, func(t *testing.T) {
			manifest := valid
			bytes := data
			switch tc {
			case "corrupt":
				manifest.SHA256 = strings.Repeat("f", 64)
			case "truncated":
				bytes = data[:len(data)/2]
			case "oversized":
				manifest.Size = MaxBinaryBytes + 1
			case "same-version":
				manifest.Version = "0.0.1"
			case "wrong-platform":
				if runtime.GOOS == "windows" {
					manifest.OS = "linux"
				} else {
					manifest.OS = "windows"
				}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc == "redirect" {
					http.Redirect(w, r, "https://example.invalid", http.StatusFound)
					return
				}
				if strings.HasSuffix(r.URL.Path, "manifest") {
					_ = json.NewEncoder(w).Encode(manifest)
					return
				}
				// Mismatch cases may stop reading early, so write errors are expected.
				_, _ = w.Write(bytes)
			}))
			defer server.Close()
			root := t.TempDir()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc == "cancelled" {
				cancel()
			}
			staged, got, err := Download(ctx, server.Client(), server.URL, filepath.Join(root, "maistr0"+filepath.Ext(executable)))
			if tc == "valid" {
				if err != nil || got != manifest {
					t.Fatalf("download manifest=%+v error=%v", got, err)
				}
				if _, err := os.Stat(staged); err != nil {
					t.Fatal(err)
				}
			} else {
				if err == nil {
					t.Fatal("invalid download succeeded")
				}
				files, _ := os.ReadDir(root)
				if len(files) != 0 {
					t.Fatalf("partial files left behind: %v", files)
				}
			}
		})
	}
}

func TestControllerHandoffRequestMatchesBothProcesses(t *testing.T) {
	target := filepath.Join(t.TempDir(), "maistr0")
	if requested, err := ControllerHandoffRequested(target, 71, 83); err != nil || requested {
		t.Fatalf("missing handoff = (%v, %v), want (false, nil)", requested, err)
	}
	path := controllerHandoffPath(target, 71, 83)
	if err := writeControllerHandoff(path, controllerHandoffRequest{ControllerPID: 71, ParentPID: 83}); err != nil {
		t.Fatal(err)
	}
	if requested, err := ControllerHandoffRequested(target, 71, 83); err != nil || !requested {
		t.Fatalf("matching handoff = (%v, %v), want (true, nil)", requested, err)
	}
	if requested, err := ControllerHandoffRequested(target, 71, 84); err != nil || requested {
		t.Fatalf("different child handoff = (%v, %v), want (false, nil)", requested, err)
	}
	if requested, err := ControllerHandoffRequested(target, 72, 83); err != nil || requested {
		t.Fatalf("different controller handoff = (%v, %v), want (false, nil)", requested, err)
	}
}

func TestReplacementStartArgsRespectControllerOwnership(t *testing.T) {
	job := applyJob{Target: "maistr0", Staged: ".maistr0-update-new", Args: []string{"--role", "node"}}
	got, shouldStart := replacementStartArgs(job, true)
	if !shouldStart || strings.Join(got, " ") != "--role node -cleanup-peer-update .maistr0-update-new" {
		t.Fatalf("standalone app restart = (%v, %v)", got, shouldStart)
	}
	job.ControllerPID = 71
	job.ControllerArgs = []string{"controller", "--runtime-config", "runtime.json"}
	got, shouldStart = replacementStartArgs(job, true)
	if !shouldStart || strings.Join(got, " ") != "controller --runtime-config runtime.json -cleanup-peer-update .maistr0-update-new" {
		t.Fatalf("unmanaged controller restart = (%v, %v)", got, shouldStart)
	}
	job.ControllerServiceManaged = true
	if got, shouldStart = replacementStartArgs(job, true); shouldStart || len(got) != 0 {
		t.Fatalf("service-managed update should defer restart to the service manager: (%v, %v)", got, shouldStart)
	}
}

func TestDeferredCleanupPersistsTheStagedHelperPath(t *testing.T) {
	target := filepath.Join(t.TempDir(), "maistr0.exe")
	staged := filepath.Join(filepath.Dir(target), ".maistr0-update-version.exe")
	if err := writeDeferredCleanup(target, staged); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(target + ".peer-update-cleanup")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(data)) != staged {
		t.Fatalf("deferred cleanup path = %q, want %q", strings.TrimSpace(string(data)), staged)
	}
}

func TestReplacementHelperPreservesArgumentsAndBackup(t *testing.T) {
	if testing.Short() {
		t.Skip("process replacement fixture builds two executables")
	}
	if runtime.GOOS != "windows" && runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("unsupported replacement platform")
	}
	root := t.TempDir()
	extension := ""
	if runtime.GOOS == "windows" {
		extension = ".exe"
	}
	target := filepath.Join(root, "maistr0"+extension)
	staged := filepath.Join(root, ".maistr0-update-test"+extension)
	for _, build := range []struct{ path, version string }{{target, "9.0.0"}, {staged, "9.0.1"}} {
		command := exec.Command("go", "build", "-ldflags", "-X main.release="+build.version, "-o", build.path, filepath.Join("testdata", "helper"))
		command.Args[len(command.Args)-1] = "." + string(filepath.Separator) + command.Args[len(command.Args)-1]
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("build helper: %v\n%s", err, output)
		}
	}
	record := filepath.Join(root, "restarted.json")
	command := exec.Command(target, "-replace", staged, "-record", record)
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("launch parent: %v\n%s", err, output)
	}
	deadline := time.Now().Add(10 * time.Second)
	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()
	var result struct {
		Version, Directory string
		Args               []string
	}
	for {
		data, err := os.ReadFile(record)
		if err == nil && json.Unmarshal(data, &result) == nil {
			break
		}
		if time.Now().After(deadline) {
			log, _ := os.ReadFile(target + ".peer-update.log")
			t.Fatalf("replacement did not restart: %v\n%s", err, log)
		}
		<-poll.C
	}
	if result.Version != "9.0.1" || result.Directory != root || len(result.Args) != 2 || result.Args[1] != record {
		t.Fatalf("restart lost version/arguments/cwd: %+v", result)
	}
	backup := exec.Command(target+".previous", "-version")
	if output, err := backup.Output(); err != nil || strings.TrimSpace(string(output)) != "maistr0 9.0.0" {
		t.Fatalf("backup missing or wrong: %q %v", output, err)
	}
	installedManifest, err := Inspect(target, "9.0.1")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(target + ".peer.json")
	if err != nil {
		t.Fatalf("updated peer manifest is missing: %v", err)
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil || !sameArtifact(installedManifest, manifest) {
		t.Fatalf("updated peer manifest = %+v, error %v; want executable metadata %+v", manifest, err, installedManifest)
	}

	duplicateRecord := filepath.Join(root, "duplicate-restart.json")
	duplicateStaged := filepath.Join(root, ".maistr0-update-duplicate"+extension)
	duplicateImage, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(duplicateStaged, duplicateImage, 0700); err != nil {
		t.Fatal(err)
	}
	duplicate := exec.Command(target, "-replace", duplicateStaged, "-record", duplicateRecord)
	duplicate.Dir = root
	if output, err := duplicate.CombinedOutput(); err != nil {
		t.Fatalf("launch duplicate replacement: %v\n%s", err, output)
	}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(duplicateRecord); err == nil {
			t.Fatal("duplicate helper restarted the already-installed release")
		} else if !os.IsNotExist(err) {
			t.Fatalf("inspect duplicate restart record: %v", err)
		}
		<-poll.C
	}

	t.Run("replacement failure restarts original", func(t *testing.T) {
		directory := t.TempDir()
		installed := filepath.Join(directory, "maistr0"+extension)
		pending := filepath.Join(directory, ".maistr0-update-failure"+extension)
		old, err := os.ReadFile(target + ".previous")
		if err != nil {
			t.Fatal(err)
		}
		newImage, err := os.ReadFile(target)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(installed, old, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(pending, newImage, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(installed+".previous", 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(installed+".previous", "block"), []byte("do not delete"), 0600); err != nil {
			t.Fatal(err)
		}
		record := filepath.Join(directory, "recovered.json")
		command := exec.Command(installed, "-replace", pending, "-record", record)
		command.Dir = directory
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("launch recovery parent: %v\n%s", err, output)
		}
		deadline := time.Now().Add(10 * time.Second)
		var recovered struct{ Version string }
		for {
			data, err := os.ReadFile(record)
			if err == nil && json.Unmarshal(data, &recovered) == nil {
				break
			}
			if time.Now().After(deadline) {
				log, _ := os.ReadFile(installed + ".peer-update.log")
				t.Fatalf("original did not restart after blocked replacement: %s", log)
			}
			<-poll.C
		}
		if recovered.Version != "9.0.0" {
			t.Fatalf("recovered wrong executable: %+v", recovered)
		}
		for {
			err := os.Remove(pending)
			if err == nil || os.IsNotExist(err) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("helper did not exit: %v", err)
			}
			<-poll.C
		}
	})
}

func BenchmarkAdmission(b *testing.B) {
	var gate Gate
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	handler := gate.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	response := httptest.NewRecorder()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		handler.ServeHTTP(response, request)
	}
}
