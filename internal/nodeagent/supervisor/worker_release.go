package supervisor

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
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
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/httpx"
)

const (
	maxWorkerArchive  = 512 << 20
	maxWorkerExpanded = 2 << 30
)

var officialWorkerBuildTag = regexp.MustCompile(`^b[0-9]+$`)

type releaseAsset struct {
	Name   string `json:"name"`
	URL    string `json:"browser_download_url"`
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
}

type workerRelease struct {
	Tag        string         `json:"tag_name"`
	Draft      bool           `json:"draft"`
	Prerelease bool           `json:"prerelease"`
	Assets     []releaseAsset `json:"assets"`
}

type workerInstallation struct {
	Source string         `json:"source"`
	Tag    string         `json:"tag"`
	Digest string         `json:"digest"`
	Binary string         `json:"binary"`
	Head   *installedHead `json:"head,omitempty"`
}

func workerRepository(source string) (string, error) {
	switch source {
	case "official":
		return "ggml-org/llama.cpp", nil
	case "xdna":
		return "dbhaigh/llama-xdna.cpp", nil
	default:
		return "", errors.New("worker source must be official or xdna")
	}
}

func workerHTTPClient() *http.Client {
	return httpx.NewClient(httpx.ClientOptions{
		Timeout: httpx.InferenceTimeout,
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return errors.New("too many worker download redirects")
			}
			return validateWorkerURL(r.URL)
		},
	})
}

func validateWorkerURL(u *url.URL) error {
	if u.Scheme != "https" || u.User != nil || u.Port() != "" {
		return errors.New("worker downloads require HTTPS without credentials or a custom port")
	}
	switch u.Hostname() {
	case "api.github.com", "github.com", "release-assets.githubusercontent.com", "objects.githubusercontent.com":
		return nil
	default:
		return errors.New("worker download host is not approved")
	}
}

func workerRequest(ctx context.Context, client *http.Client, endpoint string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	if err := validateWorkerURL(req.URL); err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "maistr0-worker-installer")
	response, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request worker release: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		_ = response.Body.Close()
		return nil, fmt.Errorf("worker release returned HTTP %d", response.StatusCode)
	}
	return response, nil
}

func findWorkerRelease(ctx context.Context, client *http.Client, source, goos, arch string) (workerRelease, releaseAsset, error) {
	repository, err := workerRepository(source)
	if err != nil {
		return workerRelease{}, releaseAsset{}, err
	}
	if arch != "amd64" || (goos != "windows" && goos != "linux") {
		return workerRelease{}, releaseAsset{}, ErrUnsupportedPlatform
	}
	response, err := workerRequest(ctx, client, "https://api.github.com/repos/"+repository+"/releases?per_page=10")
	if err != nil {
		return workerRelease{}, releaseAsset{}, err
	}
	defer closeutil.Discard(response.Body)
	var releases []workerRelease
	if err := json.NewDecoder(io.LimitReader(response.Body, 4<<20)).Decode(&releases); err != nil {
		return workerRelease{}, releaseAsset{}, fmt.Errorf("decode worker releases: %w", err)
	}
	for _, release := range releases {
		if release.Draft || (release.Prerelease && (source != "official" || !officialWorkerBuildTag.MatchString(release.Tag))) {
			continue
		}
		asset, ok := selectWorkerAsset(release, source, goos)
		if ok {
			expectedPrefix := "https://github.com/" + repository + "/releases/download/"
			if !strings.HasPrefix(asset.URL, expectedPrefix) {
				return workerRelease{}, releaseAsset{}, errors.New("worker archive does not belong to the approved repository")
			}
			digest, err := hex.DecodeString(strings.TrimPrefix(asset.Digest, "sha256:"))
			if err != nil || !strings.HasPrefix(asset.Digest, "sha256:") || len(digest) != sha256.Size {
				return workerRelease{}, releaseAsset{}, errors.New("worker release archive has no valid SHA-256 digest")
			}
			return release, asset, nil
		}
	}
	return workerRelease{}, releaseAsset{}, fmt.Errorf("%s has no supported %s/amd64 worker release archive", repository, goos)
}

func selectWorkerAsset(release workerRelease, source, goos string) (releaseAsset, bool) {
	suffix := "-bin-ubuntu-vulkan-x64.tar.gz"
	if goos == "windows" {
		suffix = "-bin-win-vulkan-x64.zip"
	}
	if source == "xdna" {
		suffix = "-bin-ubuntu-xdna-x64.tar.gz"
		if goos == "windows" {
			suffix = "-bin-win-xdna-x64.zip"
		}
	}
	for _, asset := range release.Assets {
		if strings.HasSuffix(asset.Name, suffix) && !strings.HasPrefix(asset.Name, "cudart-") {
			return asset, true
		}
	}
	return releaseAsset{}, false
}

func downloadWorkerArchive(ctx context.Context, client *http.Client, asset releaseAsset, destination string) error {
	if asset.Size <= 0 || asset.Size > maxWorkerArchive {
		return errors.New("worker archive exceeds the supported download size")
	}
	response, err := workerRequest(ctx, client, asset.URL)
	if err != nil {
		return err
	}
	defer closeutil.Discard(response.Body)
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create worker archive: %w", err)
	}
	hasher := sha256.New()
	n, copyErr := io.Copy(io.MultiWriter(file, hasher), io.LimitReader(response.Body, asset.Size+1))
	closeErr := file.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return fmt.Errorf("download worker archive: %w", err)
	}
	if n != asset.Size || hex.EncodeToString(hasher.Sum(nil)) != strings.TrimPrefix(asset.Digest, "sha256:") {
		return errors.New("worker archive size or SHA-256 verification failed")
	}
	return nil
}

func workerArchivePath(root, name string) (string, error) {
	if strings.Contains(name, "\\") || strings.Contains(name, ":") || path.IsAbs(name) {
		return "", errors.New("unsafe worker archive path")
	}
	clean := path.Clean(name)
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", errors.New("worker archive path escapes installation directory")
	}
	return filepath.Join(root, filepath.FromSlash(clean)), nil
}

func extractWorkerArchive(archive, destination string, zipped bool) (string, error) {
	var expanded int64
	var binary string
	count := 0
	links := make(map[string]string)
	write := func(name string, size int64, reader io.Reader) error {
		count++
		if size < 0 || size > maxWorkerExpanded-expanded || count > 4096 {
			return errors.New("worker archive exceeds extraction limits")
		}
		expanded += size
		target, err := workerArchivePath(destination, name)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			return err
		}
		file, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o700)
		if err != nil {
			return err
		}
		n, copyErr := io.Copy(file, io.LimitReader(reader, size+1))
		if err := errors.Join(copyErr, file.Close()); err != nil {
			return err
		}
		if n != size {
			return errors.New("worker archive entry size mismatch")
		}
		base := filepath.Base(target)
		if base == "ggml-rpc-server" || base == "ggml-rpc-server.exe" {
			if binary != "" {
				return errors.New("worker archive has multiple RPC executables")
			}
			binary = target
		}
		return nil
	}
	if zipped {
		archiveReader, err := zip.OpenReader(archive)
		if err != nil {
			return "", err
		}
		defer closeutil.Discard(archiveReader)
		for _, entry := range archiveReader.File {
			if entry.Mode().IsDir() {
				continue
			}
			if !entry.Mode().IsRegular() {
				return "", errors.New("worker ZIP contains a non-regular entry")
			}
			reader, err := entry.Open()
			if err != nil {
				return "", err
			}
			err = write(entry.Name, int64(entry.UncompressedSize64), reader)
			closeErr := reader.Close()
			if err := errors.Join(err, closeErr); err != nil {
				return "", err
			}
		}
	} else {
		file, err := os.Open(archive)
		if err != nil {
			return "", err
		}
		defer closeutil.Discard(file)
		gz, err := gzip.NewReader(file)
		if err != nil {
			return "", err
		}
		defer closeutil.Discard(gz)
		tr := tar.NewReader(gz)
		for {
			entry, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return "", err
			}
			if entry.Typeflag == tar.TypeDir {
				continue
			}
			if entry.Typeflag == tar.TypeSymlink {
				if _, err := workerArchivePath(destination, entry.Name); err != nil {
					return "", err
				}
				if strings.HasPrefix(entry.Linkname, "/") || strings.ContainsAny(entry.Linkname, "\\:") {
					return "", errors.New("unsafe worker library link")
				}
				target := path.Join(path.Dir(entry.Name), entry.Linkname)
				if _, err := workerArchivePath(destination, target); err != nil {
					return "", err
				}
				if len(links) >= 4096 {
					return "", errors.New("too many worker library links")
				}
				links[path.Clean(entry.Name)] = target
				continue
			}
			if entry.Typeflag != tar.TypeReg {
				return "", errors.New("worker tar archive contains a link or non-regular entry")
			}
			if err := write(entry.Name, entry.Size, tr); err != nil {
				return "", err
			}
		}
		// Materialize in-archive library aliases instead of creating filesystem links.
		for name, target := range links {
			for depth := 0; links[target] != ""; depth++ {
				if depth >= 16 {
					return "", errors.New("worker archive library link cycle")
				}
				target = links[target]
			}
			source, err := workerArchivePath(destination, target)
			if err != nil {
				return "", err
			}
			reader, err := os.Open(source)
			if err != nil {
				return "", err
			}
			info, err := reader.Stat()
			if err != nil || !info.Mode().IsRegular() {
				_ = reader.Close()
				return "", errors.New("worker library link target is not a regular file")
			}
			err = write(name, info.Size(), reader)
			closeErr := reader.Close()
			if err := errors.Join(err, closeErr); err != nil {
				return "", err
			}
		}
	}
	if binary == "" {
		return "", errors.New("release archive does not contain ggml-rpc-server")
	}
	return binary, nil
}
