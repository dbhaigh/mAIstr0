package selfupdate

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestManifestSignatureRejectsTamperingAndWrongPublisher(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{
		Version: "99.0.0", OS: runtime.GOOS, Arch: runtime.GOARCH,
		SHA256: strings.Repeat("a", 64), Size: 1024,
	}
	if err := manifest.Sign(privateKey); err != nil {
		t.Fatal(err)
	}
	if err := manifest.VerifySignature(publicKey); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}

	wrongPublicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := manifest.VerifySignature(wrongPublicKey); err == nil {
		t.Fatal("manifest signed by another publisher was accepted")
	}

	tampered := manifest
	tampered.SHA256 = strings.Repeat("b", 64)
	if err := tampered.VerifySignature(publicKey); err == nil {
		t.Fatal("tampered manifest was accepted")
	}

	badSignature := manifest
	badSignature.Signature = strings.Repeat("A", len(manifest.Signature))
	if err := badSignature.VerifySignature(publicKey); err == nil {
		t.Fatal("invalid signature was accepted")
	}
}

func TestDownloadRequiresConfiguredPublisherSignature(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := Inspect(executable, "99.0.0")
	if err != nil {
		t.Fatal(err)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := manifest.Sign(privateKey); err != nil {
		t.Fatal(err)
	}
	tampered := manifest
	tampered.Size++
	unsigned := manifest
	unsigned.Signature = ""
	wrongPublicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name        string
		manifest    Manifest
		publicKey   ed25519.PublicKey
		wantSuccess bool
	}{
		{name: "valid", manifest: manifest, publicKey: publicKey, wantSuccess: true},
		{name: "bad signature", manifest: func() Manifest {
			invalid := manifest
			invalid.Signature = strings.Repeat("A", len(invalid.Signature))
			return invalid
		}(), publicKey: publicKey},
		{name: "missing signature", manifest: unsigned, publicKey: publicKey},
		{name: "wrong key", manifest: manifest, publicKey: wrongPublicKey},
		{name: "tampered manifest", manifest: tampered, publicKey: publicKey},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/manifest") {
					_ = json.NewEncoder(w).Encode(test.manifest)
					return
				}
				_, _ = w.Write(binary)
			}))
			defer server.Close()

			directory := t.TempDir()
			target := filepath.Join(directory, "maistr0"+filepath.Ext(executable))
			staged, _, err := Download(context.Background(), server.Client(), server.URL, target, test.publicKey)
			if test.wantSuccess {
				if err != nil {
					t.Fatalf("valid signed update rejected: %v", err)
				}
				if err := os.Remove(staged); err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				_ = os.Remove(staged)
				t.Fatal("invalid signed update was accepted")
			}
			files, err := os.ReadDir(directory)
			if err != nil {
				t.Fatal(err)
			}
			if len(files) != 0 {
				t.Fatalf("rejected update left files behind: %v", files)
			}
		})
	}
}
