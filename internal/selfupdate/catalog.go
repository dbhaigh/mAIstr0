package selfupdate

import (
	"bytes"
	"context"
	"crypto/ed25519"
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
	"runtime"
	"strings"
	"time"

	"github.com/maistr0/maistr0/internal/httpx"
)

const (
	MaxCatalogBytes                  int64 = 1 << 20
	DefaultCentralUpdatePollInterval       = 5 * time.Minute
)

var catalogAssetPathPattern = regexp.MustCompile(`^[A-Za-z0-9._/-]+$`)

var centralUpdateTargets = []struct {
	OS, Arch, Folder, Name string
}{
	{OS: "windows", Arch: "amd64", Folder: "windows-amd64", Name: "maistr0.exe"},
	{OS: "linux", Arch: "amd64", Folder: "linux-amd64", Name: "maistr0"},
	{OS: "linux", Arch: "arm64", Folder: "linux-arm64", Name: "maistr0"},
	{OS: "darwin", Arch: "amd64", Folder: "darwin-amd64", Name: "maistr0"},
	{OS: "darwin", Arch: "arm64", Folder: "darwin-arm64", Name: "maistr0"},
}

type ReleaseArtifact struct {
	OS     string `json:"os"`
	Arch   string `json:"arch"`
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

func (artifact ReleaseArtifact) manifest(release string) Manifest {
	return Manifest{
		Version: release,
		OS:      artifact.OS,
		Arch:    artifact.Arch,
		SHA256:  artifact.SHA256,
		Size:    artifact.Size,
	}
}

type ReleaseCatalog struct {
	Version   string            `json:"version"`
	Artifacts []ReleaseArtifact `json:"artifacts"`
	Signature string            `json:"signature,omitempty"`
}

func (catalog ReleaseCatalog) Validate() error {
	if !releaseVersion.MatchString(catalog.Version) {
		return errors.New("central update catalog has an invalid release version")
	}
	if len(catalog.Artifacts) != len(centralUpdateTargets) {
		return fmt.Errorf("central update catalog must contain exactly %d platform artifacts", len(centralUpdateTargets))
	}
	seen := make(map[string]struct{}, len(catalog.Artifacts))
	for _, artifact := range catalog.Artifacts {
		if err := artifact.manifest(catalog.Version).Validate(); err != nil {
			return fmt.Errorf("central update catalog artifact: %w", err)
		}
		key := artifact.OS + "/" + artifact.Arch
		if _, exists := seen[key]; exists {
			return fmt.Errorf("central update catalog contains duplicate platform %s", key)
		}
		seen[key] = struct{}{}
		if !validCatalogAssetPath(artifact.Path) {
			return fmt.Errorf("central update catalog artifact %s has an invalid relative path", key)
		}
	}
	for _, target := range centralUpdateTargets {
		key := target.OS + "/" + target.Arch
		if _, exists := seen[key]; !exists {
			return fmt.Errorf("central update catalog is missing platform %s", key)
		}
	}
	if catalog.Signature != "" {
		signature, err := decodeSignature(catalog.Signature)
		if err != nil {
			return err
		}
		if len(signature) != ed25519.SignatureSize {
			return errors.New("central update catalog signature is malformed")
		}
	}
	return nil
}

func validCatalogAssetPath(assetPath string) bool {
	if assetPath == "" || strings.TrimSpace(assetPath) != assetPath ||
		strings.ContainsAny(assetPath, "\\\x00\r\n?#") ||
		!catalogAssetPathPattern.MatchString(assetPath) ||
		path.IsAbs(assetPath) || path.Clean(assetPath) != assetPath {
		return false
	}
	for _, segment := range strings.Split(assetPath, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return false
		}
	}
	return true
}

func (catalog ReleaseCatalog) Artifact(goos, arch string) (ReleaseArtifact, bool) {
	for _, artifact := range catalog.Artifacts {
		if artifact.OS == goos && artifact.Arch == arch {
			return artifact, true
		}
	}
	return ReleaseArtifact{}, false
}

type CentralUpdateRequest struct {
	ManifestURL string         `json:"manifest_url"`
	Catalog     ReleaseCatalog `json:"catalog"`
}

func (request CentralUpdateRequest) Validate() error {
	if err := ValidateUpdateManifestURL(request.ManifestURL); err != nil {
		return err
	}
	if request.Catalog.Signature == "" {
		return errors.New("central update catalog signature is required")
	}
	return request.Catalog.Validate()
}

func ValidateUpdateManifestURL(rawURL string) error {
	if strings.ContainsAny(rawURL, "\x00\r\n ") {
		return errors.New("central update manifest URL contains invalid characters")
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || !strings.EqualFold(parsed.Scheme, "https") || parsed.Hostname() == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" {
		return errors.New("central update manifest URL must be an absolute HTTPS URL without credentials, query, or fragment")
	}
	return nil
}

func FetchReleaseCatalog(ctx context.Context, client *http.Client, manifestURL string, publisherKey ed25519.PublicKey) (ReleaseCatalog, error) {
	if err := ValidateUpdateManifestURL(manifestURL); err != nil {
		return ReleaseCatalog{}, err
	}
	if len(publisherKey) != ed25519.PublicKeySize {
		return ReleaseCatalog{}, errors.New("central update publisher public key is not configured")
	}
	client = configuredCentralUpdateClient(client, 30*time.Second)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, manifestURL, nil)
	if err != nil {
		return ReleaseCatalog{}, fmt.Errorf("create central update manifest request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return ReleaseCatalog{}, fmt.Errorf("fetch central update manifest: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return ReleaseCatalog{}, fmt.Errorf("central update manifest returned %s", response.Status)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, MaxCatalogBytes+1))
	if err != nil {
		return ReleaseCatalog{}, fmt.Errorf("read central update manifest: %w", err)
	}
	if int64(len(data)) > MaxCatalogBytes {
		return ReleaseCatalog{}, errors.New("central update manifest exceeds the size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var catalog ReleaseCatalog
	if err := decoder.Decode(&catalog); err != nil {
		return ReleaseCatalog{}, fmt.Errorf("decode central update manifest: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return ReleaseCatalog{}, errors.New("central update manifest contains trailing JSON")
		}
		return ReleaseCatalog{}, fmt.Errorf("decode central update manifest trailer: %w", err)
	}
	if err := catalog.Validate(); err != nil {
		return ReleaseCatalog{}, err
	}
	if err := catalog.VerifySignature(publisherKey); err != nil {
		return ReleaseCatalog{}, fmt.Errorf("verify central update manifest: %w", err)
	}
	return catalog, nil
}

func DownloadCentralArtifact(
	ctx context.Context,
	client *http.Client,
	update CentralUpdateRequest,
	target string,
	publisherKey ed25519.PublicKey,
) (string, Manifest, error) {
	if err := update.Validate(); err != nil {
		return "", Manifest{}, err
	}
	if err := update.Catalog.VerifySignature(publisherKey); err != nil {
		return "", Manifest{}, fmt.Errorf("verify central update manifest: %w", err)
	}
	artifact, ok := update.Catalog.Artifact(runtime.GOOS, runtime.GOARCH)
	if !ok {
		return "", Manifest{}, fmt.Errorf("central update has no artifact for %s/%s", runtime.GOOS, runtime.GOARCH)
	}
	manifest := artifact.manifest(update.Catalog.Version)
	artifactURL, err := catalogArtifactURL(update.ManifestURL, artifact.Path)
	if err != nil {
		return "", Manifest{}, err
	}
	client = configuredCentralUpdateClient(client, 10*time.Minute)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, artifactURL, nil)
	if err != nil {
		return "", Manifest{}, fmt.Errorf("create central update artifact request: %w", err)
	}
	request.Header.Set("Accept", "application/octet-stream")
	response, err := client.Do(request)
	if err != nil {
		return "", Manifest{}, fmt.Errorf("download central update artifact: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		_ = response.Body.Close()
		return "", Manifest{}, fmt.Errorf("central update artifact returned %s", response.Status)
	}
	file, err := os.CreateTemp(filepath.Dir(target), ".maistr0-central-update-*"+filepath.Ext(target))
	if err != nil {
		_ = response.Body.Close()
		return "", Manifest{}, fmt.Errorf("create staged central update executable: %w", err)
	}
	staged := file.Name()
	keep := false
	defer func() {
		if !keep {
			_ = os.Remove(staged)
		}
	}()
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(file, hash), io.LimitReader(response.Body, manifest.Size+1))
	bodyErr := response.Body.Close()
	writeErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(copyErr, bodyErr, writeErr, closeErr); err != nil {
		return "", Manifest{}, fmt.Errorf("save central update artifact: %w", err)
	}
	if size != manifest.Size || hex.EncodeToString(hash.Sum(nil)) != manifest.SHA256 {
		return "", Manifest{}, errors.New("central update artifact size or SHA256 mismatch")
	}
	actual, err := Inspect(staged, update.Catalog.Version)
	if err != nil || !sameArtifact(actual, manifest) {
		return "", Manifest{}, errors.New("central update artifact platform or build metadata mismatch")
	}
	reportedVersion, err := executableVersion(staged)
	if err != nil {
		return "", Manifest{}, fmt.Errorf("read central update artifact version: %w", err)
	}
	if reportedVersion != update.Catalog.Version {
		return "", Manifest{}, fmt.Errorf("central update artifact reports version %s, expected %s", reportedVersion, update.Catalog.Version)
	}
	if err := os.Chmod(staged, 0o700); err != nil {
		return "", Manifest{}, fmt.Errorf("make central update artifact executable: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return "", Manifest{}, err
	}
	keep = true
	return staged, manifest, nil
}

func catalogArtifactURL(manifestURL, assetPath string) (string, error) {
	if err := ValidateUpdateManifestURL(manifestURL); err != nil {
		return "", err
	}
	if !validCatalogAssetPath(assetPath) {
		return "", errors.New("central update artifact path is invalid")
	}
	base, err := url.Parse(manifestURL)
	if err != nil {
		return "", fmt.Errorf("parse central update manifest URL: %w", err)
	}
	reference := &url.URL{Path: assetPath}
	resolved := base.ResolveReference(reference)
	if !strings.EqualFold(resolved.Scheme, base.Scheme) ||
		!strings.EqualFold(resolved.Host, base.Host) || resolved.User != nil ||
		resolved.RawQuery != "" || resolved.Fragment != "" {
		return "", errors.New("central update artifact must use the manifest HTTPS origin")
	}
	return resolved.String(), nil
}

func configuredCentralUpdateClient(client *http.Client, timeout time.Duration) *http.Client {
	if client == nil {
		client = httpx.NewClient(httpx.ClientOptions{Timeout: timeout})
	}
	clone := *client
	clone.Timeout = timeout
	clone.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("central update redirects are not permitted")
	}
	return &clone
}

func BuildReleaseCatalog(root, release, windowsBinary string) (ReleaseCatalog, error) {
	if windowsBinary == "" {
		windowsBinary = "maistr0.exe"
	}
	catalog := ReleaseCatalog{Version: release}
	for _, target := range centralUpdateTargets {
		name := target.Name
		if target.OS == "windows" {
			name = windowsBinary
		}
		binaryPath := filepath.Join(root, target.Folder, name)
		manifest, err := Inspect(binaryPath, release)
		if err != nil {
			return ReleaseCatalog{}, fmt.Errorf("inspect %s/%s release binary: %w", target.OS, target.Arch, err)
		}
		if manifest.OS != target.OS || manifest.Arch != target.Arch {
			return ReleaseCatalog{}, fmt.Errorf("release binary %s reports %s/%s, expected %s/%s",
				binaryPath, manifest.OS, manifest.Arch, target.OS, target.Arch)
		}
		catalog.Artifacts = append(catalog.Artifacts, ReleaseArtifact{
			OS: target.OS, Arch: target.Arch,
			Path:   "v" + release + "/" + target.Folder + "/" + name,
			SHA256: manifest.SHA256,
			Size:   manifest.Size,
		})
	}
	if err := catalog.Validate(); err != nil {
		return ReleaseCatalog{}, err
	}
	return catalog, nil
}
