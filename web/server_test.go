package web

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
)

func TestNewHandlerCompressedRepresentationsAndCaching(t *testing.T) {
	const asset = "app.0123456789ab.js"
	files := fstest.MapFS{
		"index.html":  &fstest.MapFile{Data: []byte(`<script src="/app.0123456789ab.js"></script>`)},
		"app.js":      &fstest.MapFile{Data: []byte("stable script")},
		asset:         &fstest.MapFile{Data: []byte("plain script")},
		asset + ".br": &fstest.MapFile{Data: []byte("brotli representation")},
		asset + ".gz": &fstest.MapFile{Data: []byte("gzip representation")},
	}
	handler := NewHandler(files)

	request := httptest.NewRequest(http.MethodGet, "/"+asset, nil)
	request.Header.Set("Accept-Encoding", "gzip, br")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("compressed asset status = %d, want 200", response.Code)
	}
	if got := response.Header().Get("Content-Encoding"); got != "br" {
		t.Fatalf("Content-Encoding = %q, want br", got)
	}
	if got := response.Body.String(); got != "brotli representation" {
		t.Fatalf("compressed body = %q", got)
	}
	if got := response.Header().Get("Cache-Control"); got != "public, max-age=31536000, immutable" {
		t.Fatalf("Cache-Control = %q", got)
	}
	etag := response.Header().Get("ETag")
	if etag == "" {
		t.Fatal("compressed asset did not include ETag")
	}

	conditional := httptest.NewRequest(http.MethodGet, "/"+asset, nil)
	conditional.Header.Set("Accept-Encoding", "br")
	conditional.Header.Set("If-None-Match", etag)
	conditionalResponse := httptest.NewRecorder()
	handler.ServeHTTP(conditionalResponse, conditional)
	if conditionalResponse.Code != http.StatusNotModified {
		t.Fatalf("conditional status = %d, want 304", conditionalResponse.Code)
	}
	if conditionalResponse.Body.Len() != 0 {
		t.Fatalf("304 response body = %q, want empty", conditionalResponse.Body.String())
	}

	stable := httptest.NewRequest(http.MethodGet, "/app.js", nil)
	stable.Header.Set("Accept-Encoding", "br")
	stableResponse := httptest.NewRecorder()
	handler.ServeHTTP(stableResponse, stable)
	if got := stableResponse.Header().Get("Cache-Control"); got != "no-cache" {
		t.Fatalf("stable asset Cache-Control = %q, want no-cache", got)
	}
	if got := stableResponse.Header().Get("Content-Encoding"); got != "br" {
		t.Fatalf("stable alias Content-Encoding = %q, want br", got)
	}
	if got := stableResponse.Body.String(); got != "brotli representation" {
		t.Fatalf("stable alias body = %q, want the hashed asset representation", got)
	}
}

func TestNewHandlerAliasesAndCachesOnDemandGzip(t *testing.T) {
	const app = "app.0123456789ab.js"
	const style = "style.abcdef012345.css"
	files := fstest.MapFS{
		"index.html": &fstest.MapFile{Data: []byte(`<script src="/app.0123456789ab.js"></script><link href="/style.abcdef012345.css">`)},
		app:          &fstest.MapFile{Data: []byte("plain script")},
		style:        &fstest.MapFile{Data: []byte("plain style")},
	}
	handler := NewHandler(files)

	for _, path := range []struct {
		url  string
		body string
	}{
		{url: "/app.js", body: "plain script"},
		{url: "/style.css", body: "plain style"},
	} {
		request := httptest.NewRequest(http.MethodGet, path.url, nil)
		request.Header.Set("Accept-Encoding", "gzip")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want 200", path.url, response.Code)
		}
		if got := response.Header().Get("Content-Encoding"); got != "gzip" {
			t.Fatalf("%s Content-Encoding = %q, want gzip", path.url, got)
		}
		reader, err := gzip.NewReader(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		if got := string(body); got != path.body {
			t.Fatalf("%s body = %q, want %q", path.url, got, path.body)
		}
		if got := response.Header().Get("Cache-Control"); got != "no-cache" {
			t.Fatalf("%s Cache-Control = %q, want no-cache for compatibility alias", path.url, got)
		}
	}
}

func TestNewHandlerGzipNegotiationAndHead(t *testing.T) {
	const asset = "style.abcdef012345.css"
	var compressed strings.Builder
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write([]byte("compressed css")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	files := fstest.MapFS{
		asset:         &fstest.MapFile{Data: []byte("css")},
		asset + ".gz": &fstest.MapFile{Data: []byte(compressed.String())},
	}
	handler := NewHandler(files)

	request := httptest.NewRequest(http.MethodGet, "/"+asset, nil)
	request.Header.Set("Accept-Encoding", "gzip;q=0.8, br;q=0")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if got := response.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", got)
	}
	reader, err := gzip.NewReader(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(body); got != "compressed css" {
		t.Fatalf("decoded body = %q", got)
	}

	head := httptest.NewRequest(http.MethodHead, "/"+asset, nil)
	head.Header.Set("Accept-Encoding", "gzip")
	headResponse := httptest.NewRecorder()
	handler.ServeHTTP(headResponse, head)
	if headResponse.Code != http.StatusOK || headResponse.Body.Len() != 0 {
		t.Fatalf("HEAD status/body = %d/%d, want 200/0", headResponse.Code, headResponse.Body.Len())
	}
}

func TestNewHandlerRejectsMethodsAndMissingFiles(t *testing.T) {
	handler := NewHandler(fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("ok")}})
	post := httptest.NewRecorder()
	handler.ServeHTTP(post, httptest.NewRequest(http.MethodPost, "/", nil))
	if post.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", post.Code)
	}
	if got := post.Header().Get("Allow"); got != "GET, HEAD" {
		t.Fatalf("Allow = %q", got)
	}

	missing := httptest.NewRecorder()
	handler.ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/missing.js", nil))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing asset status = %d, want 404", missing.Code)
	}
}
