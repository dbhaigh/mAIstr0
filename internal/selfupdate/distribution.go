package selfupdate

import (
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"github.com/maistr0/maistr0/internal/closeutil"
)

const MaxBinaryBytes int64 = 128 << 20

var releaseVersion = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(?:\.[0-9]+)?$`)

type Manifest struct {
	Version   string `json:"version"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	SHA256    string `json:"sha256"`
	Size      int64  `json:"size"`
	Signature string `json:"signature,omitempty"`
}

func (m Manifest) Validate() error {
	validPlatform := (m.OS == "windows" && m.Arch == "amd64") ||
		(m.OS == "linux" && m.Arch == "amd64") ||
		(m.OS == "darwin" && (m.Arch == "amd64" || m.Arch == "arm64"))
	if !releaseVersion.MatchString(m.Version) || !validPlatform ||
		m.Size <= 0 || m.Size > MaxBinaryBytes {
		return errors.New("unsupported peer update version, platform, or executable size")
	}
	digest, err := hex.DecodeString(m.SHA256)
	if err != nil || len(digest) != sha256.Size {
		return errors.New("invalid peer executable SHA256")
	}
	return nil
}

func Inspect(path, release string) (Manifest, error) {
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		return Manifest{}, fmt.Errorf("inspect executable build information: %w", err)
	}
	manifest := Manifest{Version: release}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "GOOS":
			manifest.OS = setting.Value
		case "GOARCH":
			manifest.Arch = setting.Value
		}
	}
	file, err := os.Open(path)
	if err != nil {
		return Manifest{}, err
	}
	defer closeutil.Discard(file)
	hash := sha256.New()
	manifest.Size, err = io.Copy(hash, io.LimitReader(file, MaxBinaryBytes+1))
	if err != nil {
		return Manifest{}, err
	}
	manifest.SHA256 = hex.EncodeToString(hash.Sum(nil))
	return manifest, manifest.Validate()
}

// Distribution serves the running image plus packaged cross-platform siblings
// carrying a generated .peer.json manifest. Paths never come from HTTP input.
type Distribution struct {
	Executable string
	Version    string
}

func (d Distribution) artifact(goos, arch string) (string, Manifest, error) {
	validTarget := (goos == "windows" && arch == "amd64") ||
		(goos == "linux" && arch == "amd64") ||
		(goos == "darwin" && (arch == "amd64" || arch == "arm64"))
	if !validTarget {
		return "", Manifest{}, errors.New("unsupported update target")
	}
	if goos == runtime.GOOS && arch == runtime.GOARCH {
		manifest, err := Inspect(d.Executable, d.Version)
		if err != nil {
			return "", Manifest{}, err
		}
		manifest, err = readArtifactManifest(d.Executable, manifest)
		return d.Executable, manifest, err
	}
	name := "maistr0"
	if goos == "windows" {
		name += ".exe"
	}
	root := filepath.Join(filepath.Dir(filepath.Dir(d.Executable)), goos+"-"+arch)
	paths := []string{filepath.Join(root, name)}
	if goos == "windows" {
		paths = append(paths, filepath.Join(root, "maistr0-update.exe"))
	}
	var result error
	for _, path := range paths {
		actual, err := Inspect(path, d.Version)
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		manifest, err := readArtifactManifest(path, actual)
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		if manifest.Version != d.Version || manifest.OS != goos || manifest.Arch != arch {
			result = errors.Join(result, errors.New("target bundle does not match running release"))
			continue
		}
		return path, manifest, nil
	}
	return "", Manifest{}, fmt.Errorf("target binary bundle is unavailable: %w", result)
}

func readArtifactManifest(path string, actual Manifest) (Manifest, error) {
	data, err := os.ReadFile(path + ".peer.json")
	if errors.Is(err, os.ErrNotExist) {
		return actual, nil
	}
	if err != nil {
		return Manifest{}, fmt.Errorf("read peer update manifest: %w", err)
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode peer update manifest: %w", err)
	}
	if err := manifest.Validate(); err != nil {
		return Manifest{}, err
	}
	if !sameArtifact(actual, manifest) {
		return Manifest{}, errors.New("peer update manifest does not match executable")
	}
	return manifest, nil
}

func (d Distribution) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	goos, arch := r.URL.Query().Get("os"), r.URL.Query().Get("arch")
	if goos == "" {
		goos = runtime.GOOS
	}
	if arch == "" {
		arch = runtime.GOARCH
	}
	path, manifest, err := d.artifact(goos, arch)
	if err != nil {
		http.Error(w, "requested peer update bundle is unavailable for this platform", http.StatusNotFound)
		return
	}
	if strings.HasSuffix(r.URL.Path, "/manifest") {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(manifest); err != nil {
			return
		}
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Maistr0-Version", manifest.Version)
	file, err := os.Open(path)
	if err != nil {
		http.Error(w, "peer executable unavailable", http.StatusServiceUnavailable)
		return
	}
	defer closeutil.Discard(file)
	stat, err := file.Stat()
	if err != nil {
		http.Error(w, "peer executable unavailable", http.StatusServiceUnavailable)
		return
	}
	http.ServeContent(w, r, filepath.Base(path), stat.ModTime(), file)
}

func CurrentDistribution() (Distribution, error) {
	path, err := os.Executable()
	if err != nil {
		return Distribution{}, err
	}
	return distributionForExecutable(path)
}

func distributionForExecutable(path string) (Distribution, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return Distribution{}, fmt.Errorf("resolve running executable path: %w", err)
	}
	release, err := executableVersion(resolved)
	if err != nil {
		return Distribution{}, fmt.Errorf("read running executable version: %w", err)
	}
	return Distribution{Executable: resolved, Version: release}, nil
}
