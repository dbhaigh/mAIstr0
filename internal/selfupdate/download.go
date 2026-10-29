package selfupdate

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/version"
)

var ErrNoUpdate = errors.New("peer offers no newer compatible executable")

// Download uses the caller's pinned mutual-TLS transport. Redirects are refused:
// a trusted peer cannot redirect executable acquisition to an unrelated host.
func Download(ctx context.Context, client *http.Client, address, target string, trustedPublisherKey ...ed25519.PublicKey) (string, Manifest, error) {
	if len(trustedPublisherKey) > 1 {
		return "", Manifest{}, errors.New("only one trusted peer update publisher key may be configured")
	}
	manifest, err := fetchManifest(ctx, client, address, trustedPublisherKey...)
	if err != nil {
		return "", manifest, err
	}
	if version.Compare(manifest.Version, version.String()) <= 0 {
		return "", manifest, ErrNoUpdate
	}
	path, err := downloadManifest(ctx, client, address, target, manifest)
	return path, manifest, err
}

func fetchManifest(ctx context.Context, client *http.Client, address string, trustedPublisherKey ...ed25519.PublicKey) (Manifest, error) {
	if len(trustedPublisherKey) > 1 {
		return Manifest{}, errors.New("only one trusted peer update publisher key may be configured")
	}
	copyClient := configuredUpdateClient(client)
	response, err := peerUpdateRequest(ctx, copyClient, address, "/api/updates/manifest")
	if err != nil {
		return Manifest{}, err
	}
	var manifest Manifest
	err = json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&manifest)
	_ = response.Body.Close()
	if err != nil {
		return manifest, fmt.Errorf("decode peer update manifest: %w", err)
	}
	if err := manifest.Validate(); err != nil {
		return manifest, err
	}
	if len(trustedPublisherKey) == 1 {
		if err := manifest.VerifySignature(trustedPublisherKey[0]); err != nil {
			return manifest, err
		}
	}
	if manifest.OS != runtime.GOOS || manifest.Arch != runtime.GOARCH {
		return manifest, errors.New("peer executable platform does not match requested target")
	}
	return manifest, nil
}

func downloadManifest(ctx context.Context, client *http.Client, address, target string, manifest Manifest) (string, error) {
	if err := manifest.Validate(); err != nil {
		return "", err
	}
	if manifest.OS != runtime.GOOS || manifest.Arch != runtime.GOARCH {
		return "", errors.New("peer executable platform does not match requested target")
	}
	copyClient := configuredUpdateClient(client)
	directory := filepath.Dir(target)
	file, err := os.CreateTemp(directory, ".maistr0-update-*"+filepath.Ext(target))
	if err != nil {
		return "", fmt.Errorf("create staged executable beside current binary: %w", err)
	}
	path := file.Name()
	ok := false
	defer func() {
		if !ok {
			_ = file.Close()
			_ = os.Remove(path)
		}
	}()
	response, err := peerUpdateRequest(ctx, copyClient, address, "/api/updates/binary")
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(file, hash), io.LimitReader(response.Body, manifest.Size+1))
	_ = response.Body.Close()
	if err := errors.Join(copyErr, file.Sync(), file.Close()); err != nil {
		return "", fmt.Errorf("save peer executable: %w", err)
	}
	if size != manifest.Size || hex.EncodeToString(hash.Sum(nil)) != manifest.SHA256 {
		return "", errors.New("peer executable size or SHA256 mismatch")
	}
	actual, err := Inspect(path, manifest.Version)
	if err != nil || !sameArtifact(actual, manifest) {
		return "", errors.New("downloaded executable platform/build metadata mismatch")
	}
	if err := os.Chmod(path, 0700); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	ok = true
	return path, nil
}

func configuredUpdateClient(client *http.Client) *http.Client {
	copyClient := *client
	copyClient.Timeout = 10 * time.Minute
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("peer update redirects are not permitted")
	}
	return &copyClient
}

func peerUpdateRequest(ctx context.Context, client *http.Client, address, path string) (*http.Response, error) {
	query := "?os=" + runtime.GOOS + "&arch=" + runtime.GOARCH
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(address, "/")+path+query, nil)
	if err != nil {
		return nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		_ = response.Body.Close()
		if response.StatusCode == http.StatusNotFound {
			return nil, ErrNoUpdate
		}
		return nil, fmt.Errorf("peer update returned %s", response.Status)
	}
	return response, nil
}
