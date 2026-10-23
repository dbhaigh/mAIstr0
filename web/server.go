package web

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var hashedAssetName = regexp.MustCompile(`(?:^|/)[A-Za-z0-9_-]+\.[a-f0-9]{12}\.[A-Za-z0-9]+$`)

// NewHandler serves embedded dashboard assets with precompressed representations
// when present, validators, and cache lifetimes appropriate to their filenames.
func NewHandler(files fs.FS) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		name := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
		if name == "" || name == "." {
			name = "index.html"
		}
		if !fs.ValidPath(name) {
			http.NotFound(w, r)
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

		encoding, compressed, err := readCompressedAsset(files, name, r.Header.Get("Accept-Encoding"))
		if err != nil {
			http.Error(w, "could not read compressed embedded asset", http.StatusInternalServerError)
			return
		}
		if encoding != "" {
			data = compressed
			w.Header().Set("Content-Encoding", encoding)
		}
		w.Header().Add("Vary", "Accept-Encoding")
		if contentType := mime.TypeByExtension(path.Ext(name)); contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
		if hashedAssetName.MatchString(name) {
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
		http.ServeContent(w, r, path.Base(name), time.Time{}, bytes.NewReader(data))
	})
}

func readCompressedAsset(files fs.FS, name, acceptEncoding string) (string, []byte, error) {
	for _, encoding := range []string{"br", "gzip"} {
		if !acceptsEncoding(acceptEncoding, encoding) {
			continue
		}
		suffix := ".gz"
		if encoding == "br" {
			suffix = ".br"
		}
		data, err := fs.ReadFile(files, name+suffix)
		if err == nil {
			return encoding, data, nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", nil, err
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
