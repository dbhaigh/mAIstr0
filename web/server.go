package web

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var hashedAssetName = regexp.MustCompile(`(?:^|/)[A-Za-z0-9_-]+\.[a-f0-9]{12}\.[A-Za-z0-9]+$`)
var appAssetReference = regexp.MustCompile(`src="/(app\.[a-f0-9]{12}\.js)"`)
var styleAssetReference = regexp.MustCompile(`href="/(style\.[a-f0-9]{12}\.css)"`)

// NewHandler serves embedded dashboard assets with cached compressed
// representations, validators, and cache lifetimes appropriate to their URLs.
func NewHandler(files fs.FS) http.Handler {
	aliases, aliasErr := readCompatibilityAliases(files)
	var compressedCache sync.Map
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		requestedName := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
		if requestedName == "" || requestedName == "." {
			requestedName = "index.html"
		}
		if !fs.ValidPath(requestedName) {
			http.NotFound(w, r)
			return
		}
		name := requestedName
		if alias, ok := aliases[requestedName]; ok {
			name = alias
		} else if aliasErr != nil && (requestedName == "app.js" || requestedName == "style.css") {
			http.Error(w, "could not resolve dashboard asset alias", http.StatusInternalServerError)
			return
		}

		data, err := fs.ReadFile(files, name)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				http.NotFound(w, r)
				return
			}
			http.Error(w, "could not read embedded asset", http.StatusInternalServerError)
			return
		}

		encoding, compressed, err := readCompressedAsset(
			files, name, data, r.Header.Get("Accept-Encoding"), &compressedCache,
		)
		if err != nil {
			http.Error(w, "could not read compressed embedded asset", http.StatusInternalServerError)
			return
		}
		if encoding != "" {
			data = compressed
			w.Header().Set("Content-Encoding", encoding)
		}
		w.Header().Add("Vary", "Accept-Encoding")
		if contentType := mime.TypeByExtension(path.Ext(requestedName)); contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
		if hashedAssetName.MatchString(requestedName) {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			w.Header().Set("Cache-Control", "no-cache")
		}
		sum := sha256.Sum256(data)
		etag := `"` + hex.EncodeToString(sum[:]) + `"`
		w.Header().Set("ETag", etag)
		if matchesETag(r.Header.Get("If-None-Match"), etag) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		http.ServeContent(w, r, path.Base(requestedName), time.Time{}, bytes.NewReader(data))
	})
}

func readCompatibilityAliases(files fs.FS) (map[string]string, error) {
	index, err := fs.ReadFile(files, "index.html")
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read dashboard index for asset aliases: %w", err)
	}

	aliases := make(map[string]string, 2)
	for _, alias := range []struct {
		name    string
		pattern *regexp.Regexp
	}{
		{name: "app.js", pattern: appAssetReference},
		{name: "style.css", pattern: styleAssetReference},
	} {
		match := alias.pattern.FindSubmatch(index)
		if len(match) != 2 {
			continue
		}
		target := string(match[1])
		if _, err := fs.Stat(files, target); err != nil {
			return nil, fmt.Errorf("resolve dashboard alias %q to %q: %w", alias.name, target, err)
		}
		aliases[alias.name] = target
	}
	return aliases, nil
}

func readCompressedAsset(files fs.FS, name string, assetData []byte, acceptEncoding string, cache *sync.Map) (string, []byte, error) {
	for _, encoding := range []string{"br", "gzip"} {
		if !acceptsEncoding(acceptEncoding, encoding) {
			continue
		}
		suffix := ".gz"
		if encoding == "br" {
			suffix = ".br"
		}
		compressedData, err := fs.ReadFile(files, name+suffix)
		if err == nil {
			return encoding, compressedData, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", nil, fmt.Errorf("read compressed dashboard asset %q: %w", name+suffix, err)
		}
		if encoding == "gzip" {
			if cached, ok := cache.Load(name); ok {
				return encoding, cached.([]byte), nil
			}
			var compressed bytes.Buffer
			writer := gzip.NewWriter(&compressed)
			if _, err := writer.Write(assetData); err != nil {
				return "", nil, fmt.Errorf("compress dashboard asset %q: %w", name, err)
			}
			if err := writer.Close(); err != nil {
				return "", nil, fmt.Errorf("finish compression for dashboard asset %q: %w", name, err)
			}
			value, _ := cache.LoadOrStore(name, compressed.Bytes())
			return encoding, value.([]byte), nil
		}
	}
	return "", nil, nil
}

func acceptsEncoding(header, encoding string) bool {
	wildcard := -1.0
	for _, value := range strings.Split(header, ",") {
		parts := strings.Split(strings.TrimSpace(value), ";")
		name := strings.TrimSpace(parts[0])
		quality := 1.0
		for _, parameter := range parts[1:] {
			parameterName, parameterValue, ok := strings.Cut(strings.TrimSpace(parameter), "=")
			if ok && strings.EqualFold(parameterName, "q") {
				var err error
				quality, err = strconv.ParseFloat(parameterValue, 64)
				if err != nil || quality < 0 || quality > 1 {
					quality = 0
				}
				break
			}
		}
		if strings.EqualFold(name, encoding) {
			return quality > 0
		}
		if name == "*" {
			wildcard = quality
		}
	}
	return wildcard > 0
}

func matchesETag(header, etag string) bool {
	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" || strings.TrimPrefix(candidate, "W/") == etag {
			return true
		}
	}
	return false
}
