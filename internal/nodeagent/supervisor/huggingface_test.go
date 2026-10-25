package supervisor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRecommendedGGUFDownloadVerificationAndRediscovery(t *testing.T) {
	fixture := filepath.Join(t.TempDir(), "fixture.gguf")
	writeLocalTestGGUF(t, fixture)
	data, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	for _, test := range []struct {
		name      string
		checksum  string
		size      int64
		corrupt   bool
		status    int
		wantError bool
	}{
		{"verified", hex.EncodeToString(digest[:]), int64(len(data)), false, 200, false},
		{"checksum mismatch", strings.Repeat("0", 64), int64(len(data)), false, 200, true},
		{"truncated", hex.EncodeToString(digest[:]), int64(len(data) + 1), false, 200, true},
		{"oversized", hex.EncodeToString(digest[:]), 1 << 30, false, 200, true},
		{"missing hash", "", int64(len(data)), false, 200, true},
		{"invalid GGUF", hex.EncodeToString(digest[:]), int64(len(data)), true, 200, true},
		{"gated", hex.EncodeToString(digest[:]), int64(len(data)), false, 403, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			downloads := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, "/api/models/") {
					hash := test.checksum
					if test.corrupt {
						d := sha256.Sum256(make([]byte, len(data)))
						hash = hex.EncodeToString(d[:])
					}
					_ = json.NewEncoder(w).Encode([]any{map[string]any{"path": "model.gguf", "size": test.size, "lfs": map[string]any{"oid": hash}}})
					return
				}
				downloads++
				w.WriteHeader(test.status)
				if test.corrupt {
					_, _ = w.Write(make([]byte, len(data)))
				} else {
					_, _ = w.Write(data)
				}
			}))
			defer server.Close()
			root := t.TempDir()
			model := GGUFRecommendation{ID: "test", Repository: "owner/model", Filename: "model.gguf", MaxBytes: 1 << 20}
			result, err := downloadRecommendedGGUF(context.Background(), server.Client(), server.URL, root, model, 4096)
			if test.wantError {
				if err == nil {
					t.Fatal("invalid download accepted")
				}
				_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
					if err == nil && !entry.IsDir() {
						t.Errorf("failed download left file %s", path)
					}
					return err
				})
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if result.Name != "model" || result.Source != "huggingface" || len(result.ID) != 64 {
				t.Fatalf("model=%+v", result)
			}
			cached, err := downloadRecommendedGGUF(context.Background(), server.Client(), server.URL, root, model, 4096)
			if err != nil || cached.ID != result.ID || downloads != 1 {
				t.Fatalf("cache=%+v downloads=%d err=%v", cached, downloads, err)
			}
			inventory, err := discoverLocalHeadModels(context.Background(), map[string][]string{"huggingface": {root}}, 4096)
			if err != nil || len(inventory.Models) != 1 || inventory.Models[0].ID != result.ID || inventory.Models[0].Name != result.Name {
				t.Fatalf("persistent discovery=%+v err=%v", inventory, err)
			}
			if err := os.WriteFile(result.path, []byte("corrupt"), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := downloadRecommendedGGUF(context.Background(), server.Client(), server.URL, root, model, 4096); err == nil {
				t.Fatal("corrupt cached file accepted")
			}
		})
	}
}

func TestHeadDownloadRejectsUnknownModelAndCancellation(t *testing.T) {
	manager := &Manager{}
	if _, err := manager.DownloadHeadModel(context.Background(), "https://untrusted/model.gguf", 4096); err == nil {
		t.Fatal("arbitrary model URL accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	model := GGUFRecommendation{ID: "test", Repository: "owner/model", Filename: "model.gguf", MaxBytes: 1024}
	if _, err := downloadRecommendedGGUF(ctx, http.DefaultClient, "http://127.0.0.1:1", t.TempDir(), model, 4096); err == nil {
		t.Fatal("cancelled download succeeded")
	}
}

func TestHuggingFaceRedirectPolicy(t *testing.T) {
	for _, test := range []struct {
		address string
		allowed bool
	}{
		{"https://huggingface.co/model", true},
		{"https://cdn-lfs.hf.co/model", true},
		{"https://cas-bridge.xethub.hf.co/model?signature=temporary", true},
		{"http://huggingface.co/model", false},
		{"https://huggingface.co.example.com/model", false},
		{"https://127.0.0.1/model", false},
		{"https://user:secret@huggingface.co/model", false},
		{"https://huggingface.co:8443/model", false},
	} {
		t.Run(test.address, func(t *testing.T) {
			request, err := http.NewRequest(http.MethodGet, test.address, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got := approvedHFRedirect(request, nil) == nil; got != test.allowed {
				t.Fatalf("allowed=%v want=%v", got, test.allowed)
			}
		})
	}
	err := modelTransferError("download", &url.Error{URL: "https://cdn-lfs.hf.co/model?signature=private", Err: context.Canceled})
	if strings.Contains(err.Error(), "signature") || !strings.Contains(err.Error(), "canceled") {
		t.Fatalf("unsafe transfer error: %v", err)
	}
}
