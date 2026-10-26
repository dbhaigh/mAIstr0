package selfupdate

import (
	"context"
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
func Download(ctx context.Context, client *http.Client, address, target string) (string, Manifest, error) {
	copyClient := *client
	copyClient.Timeout = 10 * time.Minute
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("peer update redirects are not permitted")
	}
	query := "?os=" + runtime.GOOS + "&arch=" + runtime.GOARCH
	get := func(path string) (*http.Response, error) {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(address, "/")+path+query, nil)
		if err != nil {
			return nil, err
		}
		response, err := copyClient.Do(request)
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
	response, err := get("/api/updates/manifest")
	if err != nil {
		return "", Manifest{}, err
	}
	var manifest Manifest
	err = json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&manifest)
	_ = response.Body.Close()
	if err != nil {
		return "", manifest, fmt.Errorf("decode peer update manifest: %w", err)
	}
	if err := manifest.Validate(); err != nil {
		return "", manifest, err
	}
	if manifest.OS != runtime.GOOS || manifest.Arch != runtime.GOARCH {
		return "", manifest, errors.New("peer executable platform does not match requested target")
	}
	if version.Compare(manifest.Version, version.String()) <= 0 {
		return "", manifest, ErrNoUpdate
	}
	directory := filepath.Dir(target)
	file, err := os.CreateTemp(directory, ".maistr0-update-*"+filepath.Ext(target))
	if err != nil {
		return "", manifest, fmt.Errorf("create staged executable beside current binary: %w", err)
	}
	path := file.Name()
	ok := false
	defer func() {
		if !ok {
			_ = file.Close()
			_ = os.Remove(path)
		}
	}()
	response, err = get("/api/updates/binary")
	if err != nil {
		return "", manifest, err
	}
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(file, hash), io.LimitReader(response.Body, manifest.Size+1))
	_ = response.Body.Close()
	if err := errors.Join(copyErr, file.Sync(), file.Close()); err != nil {
		return "", manifest, fmt.Errorf("save peer executable: %w", err)
	}
	if size != manifest.Size || hex.EncodeToString(hash.Sum(nil)) != manifest.SHA256 {
		return "", manifest, errors.New("peer executable size or SHA256 mismatch")
	}
	actual, err := Inspect(path, manifest.Version)
	if err != nil || actual != manifest {
		return "", manifest, errors.New("downloaded executable platform/build metadata mismatch")
	}
	if err := os.Chmod(path, 0700); err != nil {
		return "", manifest, err
	}
	if err := ctx.Err(); err != nil {
		return "", manifest, err
	}
	ok = true
	return path, manifest, nil
}
