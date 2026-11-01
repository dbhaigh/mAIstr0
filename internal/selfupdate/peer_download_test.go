package selfupdate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestLegacyPeerDownloadUsesChecksumWithoutPublisherKey(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	manifest := peerUpdateTestManifest(t)
	manifest.Signature = ""
	badChecksum := strings.Repeat("0", len(manifest.SHA256))
	if badChecksum == manifest.SHA256 {
		badChecksum = strings.Repeat("1", len(manifest.SHA256))
	}

	for _, test := range []struct {
		name     string
		checksum string
		wantErr  bool
	}{
		{name: "matching checksum", checksum: manifest.SHA256},
		{name: "checksum mismatch", checksum: badChecksum, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			offered := manifest
			offered.SHA256 = test.checksum
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/manifest") {
					_ = json.NewEncoder(w).Encode(offered)
					return
				}
				_, _ = w.Write(binary)
			}))
			defer server.Close()

			targetDir := t.TempDir()
			target := filepath.Join(targetDir, "maistr0"+filepath.Ext(executable))
			staged, received, err := Download(context.Background(), server.Client(), server.URL, target)
			if test.wantErr {
				if err == nil || !strings.Contains(err.Error(), "SHA256 mismatch") {
					t.Fatalf("checksum mismatch error = %v, want SHA256 rejection", err)
				}
				assertNoStagedPeerUpdate(t, targetDir)
				return
			}
			if err != nil {
				t.Fatalf("unsigned legacy update with matching checksum rejected: %v", err)
			}
			if received.Signature != "" || received.SHA256 != manifest.SHA256 {
				t.Fatalf("legacy manifest = %+v, want unsigned manifest with its checksum", received)
			}
			if _, err := os.Stat(staged); err != nil {
				t.Fatalf("verified legacy executable was not staged: %v", err)
			}
			if err := os.Remove(staged); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDistributionReturnsNotFoundForUnsupportedPlatform(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	distribution := Distribution{Executable: executable, Version: "99.0.0"}
	for _, path := range []string{
		"/api/updates/manifest?os=linux&arch=arm64",
		"/api/updates/binary?os=linux&arch=arm64",
	} {
		t.Run(path, func(t *testing.T) {
			response := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, path, nil)
			distribution.ServeHTTP(response, request)
			if response.Code != http.StatusNotFound {
				t.Fatalf("unsupported target returned %d, want 404: %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestDownloadPropagatesPeerAuthenticationAndServiceErrors(t *testing.T) {
	manifest := peerUpdateTestManifest(t)
	for _, status := range []int{
		http.StatusUnauthorized,
		http.StatusNotImplemented,
		http.StatusBadGateway,
	} {
		for _, failingEndpoint := range []string{"manifest", "binary"} {
			t.Run(fmt.Sprintf("%s/%d", failingEndpoint, status), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if strings.HasSuffix(r.URL.Path, "/manifest") {
						if failingEndpoint == "manifest" {
							w.WriteHeader(status)
							return
						}
						_ = json.NewEncoder(w).Encode(manifest)
						return
					}
					w.WriteHeader(status)
				}))
				defer server.Close()

				targetDir := t.TempDir()
				target := filepath.Join(targetDir, "maistr0"+executableSuffix())
				_, _, err := Download(context.Background(), server.Client(), server.URL, target)
				if err == nil {
					t.Fatal("peer error response was accepted")
				}
				if errors.Is(err, ErrNoUpdate) {
					t.Fatalf("peer status %d was treated as no update: %v", status, err)
				}
				if !strings.Contains(err.Error(), fmt.Sprintf("%d %s", status, http.StatusText(status))) {
					t.Fatalf("peer error = %v, want status %d", err, status)
				}
				assertNoStagedPeerUpdate(t, targetDir)
			})
		}
	}
}

func peerUpdateTestManifest(t *testing.T) Manifest {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := Inspect(executable, "99.0.0")
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func executableSuffix() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}

func assertNoStagedPeerUpdate(t *testing.T, directory string) {
	t.Helper()
	files, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatalf("rejected update left staged files behind: %v", files)
	}
}
