package supervisor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/httpx"
)

// RecommendedGGUF lists public, single-file text models supported by llama.cpp.
// Acquisition accepts these IDs only, never caller-supplied URLs or paths.
type GGUFRecommendation struct {
	ID         string `json:"id"`
	Repository string `json:"repository"`
	Filename   string `json:"filename"`
	MaxBytes   int64  `json:"max_bytes"`
	SizeBytes  int64  `json:"size_bytes"`
}

func RecommendedGGUF() []GGUFRecommendation {
	return []GGUFRecommendation{
		{ID: "qwen2.5-3b-instruct-q4", Repository: "Qwen/Qwen2.5-3B-Instruct-GGUF", Filename: "qwen2.5-3b-instruct-q4_k_m.gguf", MaxBytes: 3 << 30, SizeBytes: 2104932768},
		{ID: "qwen2.5-7b-instruct-q4", Repository: "bartowski/Qwen2.5-7B-Instruct-GGUF", Filename: "Qwen2.5-7B-Instruct-Q4_K_M.gguf", MaxBytes: 6 << 30, SizeBytes: 4683074240},
	}
}

func (m *Manager) DownloadHeadModel(ctx context.Context, id string, requestedContext int) (LocalHeadModel, error) {
	var selected *GGUFRecommendation
	for _, recommendation := range RecommendedGGUF() {
		if recommendation.ID == id {
			selected = &recommendation
			break
		}
	}
	if selected == nil || requestedContext < 1 || requestedContext > 1<<20 {
		return LocalHeadModel{}, fmt.Errorf("%w: unknown recommended GGUF or invalid context", ErrNotFound)
	}
	m.mu.Lock()
	root := m.workerRoot
	closed := m.closed
	m.mu.Unlock()
	if closed {
		return LocalHeadModel{}, ErrClosed
	}
	if root == "" {
		return LocalHeadModel{}, errors.New("install the managed runtime before downloading models")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Hour)
	defer cancel()
	client := httpx.NewClient(httpx.ClientOptions{Timeout: httpx.DownloadTimeout, CheckRedirect: approvedHFRedirect})
	return downloadRecommendedGGUF(ctx, client, "https://huggingface.co", filepath.Join(root, "models"), *selected, requestedContext)
}

func approvedHFRedirect(r *http.Request, via []*http.Request) error {
	if len(via) > 8 || r.URL.Scheme != "https" || r.URL.User != nil || (r.URL.Port() != "" && r.URL.Port() != "443") {
		return errors.New("unsafe Hugging Face download redirect")
	}
	host := r.URL.Hostname()
	if host != "huggingface.co" && !strings.HasSuffix(host, ".huggingface.co") && !strings.HasSuffix(host, ".hf.co") {
		return errors.New("Hugging Face download redirected to an unapproved host")
	}
	return nil
}

func modelTransferError(operation string, err error) error {
	var requestError *url.Error
	if errors.As(err, &requestError) {
		err = requestError.Err
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func downloadRecommendedGGUF(ctx context.Context, client *http.Client, base, root string, model GGUFRecommendation, requestedContext int) (LocalHeadModel, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/models/"+model.Repository+"/tree/main?recursive=true", nil)
	if err != nil {
		return LocalHeadModel{}, fmt.Errorf("create Hugging Face metadata request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return LocalHeadModel{}, modelTransferError("read Hugging Face metadata", err)
	}
	var files []struct {
		Path string `json:"path"`
		Size int64  `json:"size"`
		LFS  struct {
			OID  string `json:"oid"`
			Size int64  `json:"size"`
		} `json:"lfs"`
	}
	err = func() error {
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("Hugging Face metadata returned %s (only public ungated models are supported)", response.Status)
		}
		return json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(&files)
	}()
	if err != nil {
		return LocalHeadModel{}, err
	}
	var digest string
	var size int64
	for _, file := range files {
		if file.Path == model.Filename {
			digest = file.LFS.OID
			size = file.Size
			break
		}
	}
	hashBytes, err := hex.DecodeString(digest)
	if err != nil || len(hashBytes) != sha256.Size || size <= 0 || size > model.MaxBytes {
		return LocalHeadModel{}, errors.New("Hugging Face GGUF is missing verified LFS SHA256/size metadata or exceeds its download limit")
	}
	directory := filepath.Join(root, model.ID, digest)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return LocalHeadModel{}, fmt.Errorf("create model cache: %w", err)
	}
	target := filepath.Join(directory, model.Filename)
	// Content-addressed names retain verified files across retries without
	// overwriting a model another task or saved group may already reference.
	if _, err := os.Stat(target); err == nil {
		cached, err := os.Open(target)
		if err != nil {
			return LocalHeadModel{}, fmt.Errorf("open cached GGUF: %w", err)
		}
		hash := sha256.New()
		n, readErr := io.Copy(hash, io.LimitReader(cached, size+1))
		if err := errors.Join(readErr, cached.Close()); err != nil {
			return LocalHeadModel{}, fmt.Errorf("verify cached GGUF: %w", err)
		}
		if n != size || hex.EncodeToString(hash.Sum(nil)) != digest {
			return LocalHeadModel{}, errors.New("cached GGUF checksum is invalid; remove the corrupted cached file before retrying")
		}
		return downloadedHeadModel(target, root, model.Filename, requestedContext)
	} else if !errors.Is(err, os.ErrNotExist) {
		return LocalHeadModel{}, fmt.Errorf("inspect model cache: %w", err)
	}
	file, err := os.CreateTemp(directory, ".download-")
	if err != nil {
		return LocalHeadModel{}, fmt.Errorf("create model download: %w", err)
	}
	defer os.Remove(file.Name())
	request, err = http.NewRequestWithContext(ctx, http.MethodGet, base+"/"+model.Repository+"/resolve/main/"+model.Filename, nil)
	if err != nil {
		file.Close()
		return LocalHeadModel{}, err
	}
	response, err = client.Do(request)
	if err != nil {
		file.Close()
		return LocalHeadModel{}, modelTransferError("download GGUF", err)
	}
	hasher := sha256.New()
	n, copyErr := func() (int64, error) {
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return 0, fmt.Errorf("Hugging Face download returned %s", response.Status)
		}
		return io.Copy(io.MultiWriter(file, hasher), io.LimitReader(response.Body, size+1))
	}()
	if err := errors.Join(copyErr, file.Close()); err != nil {
		return LocalHeadModel{}, fmt.Errorf("save GGUF download: %w", err)
	}
	if n != size || hex.EncodeToString(hasher.Sum(nil)) != digest {
		return LocalHeadModel{}, errors.New("GGUF download size or SHA256 verification failed")
	}
	if _, err := readLocalGGUF(file.Name(), requestedContext); err != nil {
		return LocalHeadModel{}, fmt.Errorf("validate downloaded GGUF: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return LocalHeadModel{}, err
	}
	if err := os.Rename(file.Name(), target); err != nil {
		return LocalHeadModel{}, fmt.Errorf("publish verified GGUF: %w", err)
	}
	return downloadedHeadModel(target, root, model.Filename, requestedContext)
}

func downloadedHeadModel(path, root, name string, requestedContext int) (LocalHeadModel, error) {
	profile, err := readLocalGGUF(path, requestedContext)
	if err != nil {
		return LocalHeadModel{}, fmt.Errorf("read cached GGUF profile: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return LocalHeadModel{}, err
	}
	id := sha256.Sum256([]byte(resolved))
	profile.Name = strings.TrimSuffix(name, ".gguf")
	return LocalHeadModel{ID: hex.EncodeToString(id[:]), Name: profile.Name, Source: "huggingface", Profile: profile, path: resolved, root: root}, nil
}
