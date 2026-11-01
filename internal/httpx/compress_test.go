package httpx

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestAcceptsGzip(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		values []string
		want   bool
	}{
		{name: "gzip", values: []string{"gzip"}, want: true},
		{name: "weighted gzip", values: []string{"br, gzip;q=0.4"}, want: true},
		{name: "explicitly refused", values: []string{"gzip;q=0"}, want: false},
		{name: "unsupported coding", values: []string{"br"}, want: false},
		{name: "wildcard", values: []string{"*;q=0.5"}, want: true},
		{name: "explicit refusal overrides wildcard", values: []string{"*;q=1, gzip;q=0"}, want: false},
		{name: "case insensitive", values: []string{"GZip;Q=0.8"}, want: true},
		{name: "invalid quality", values: []string{"gzip;q=invalid"}, want: false},
		{name: "missing quality value", values: []string{"gzip;q"}, want: false},
		{name: "duplicate quality refusal", values: []string{"gzip;q=1;q=0"}, want: false},
		{name: "non-finite quality", values: []string{"gzip;q=NaN"}, want: false},
		{name: "multiple header values", values: []string{"br", "gzip;q=0.4"}, want: true},
		{name: "duplicate refusal", values: []string{"gzip", "gzip;q=0"}, want: false},
		{name: "quality above one", values: []string{"gzip;q=1.1"}, want: false},
		{name: "identity only", values: []string{"identity;q=0"}, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			header := make(http.Header)
			for _, value := range test.values {
				header.Add("Accept-Encoding", value)
			}
			if got := AcceptsGzip(header); got != test.want {
				t.Fatalf("AcceptsGzip(%v) = %v, want %v", test.values, got, test.want)
			}
		})
	}
}

func TestCompressJSONThresholdAndVary(t *testing.T) {
	t.Parallel()
	for _, size := range []int{jsonCompressionThreshold, jsonCompressionThreshold + 1} {
		body := jsonBodyOfSize(size)
		response := httptest.NewRecorder()
		handler := CompressJSON(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.example+json; charset=utf-8")
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.Header().Add("Vary", "Origin")
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write(body); err != nil {
				t.Errorf("write JSON response: %v", err)
			}
		}))
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		request.Header.Set("Accept-Encoding", "gzip")
		handler.ServeHTTP(response, request)

		if response.Code != http.StatusOK {
			t.Fatalf("response status = %d, want %d", response.Code, http.StatusOK)
		}
		if !varyContains(response.Header(), "Origin") || !varyContains(response.Header(), "Accept-Encoding") {
			t.Fatalf("Vary header = %v, want Origin and Accept-Encoding", response.Header().Values("Vary"))
		}
		if size == jsonCompressionThreshold {
			if got := response.Header().Get("Content-Encoding"); got != "" {
				t.Fatalf("threshold-sized response encoding = %q, want none", got)
			}
			if got := response.Header().Get("Content-Length"); got != strconv.Itoa(size) {
				t.Fatalf("identity Content-Length = %q, want %d", got, size)
			}
			if !bytes.Equal(response.Body.Bytes(), body) {
				t.Fatal("threshold-sized response body changed")
			}
			continue
		}

		if got := response.Header().Get("Content-Encoding"); got != "gzip" {
			t.Fatalf("large response encoding = %q, want gzip", got)
		}
		if got := response.Header().Get("Content-Length"); got != "" {
			t.Fatalf("compressed Content-Length = %q, want unset", got)
		}
		if got := readGzipBody(t, response); !bytes.Equal(got, body) {
			t.Fatal("decompressed response body differs from original")
		}
	}
}

func TestCompressJSONFormatsAndCompressesLargeErrors(t *testing.T) {
	t.Parallel()
	message := strings.Repeat("failure ", jsonCompressionThreshold/8+10) + "end"
	response := httptest.NewRecorder()
	handler := Chain(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, message, http.StatusBadRequest)
	}), CompressJSON, FormatErrors)
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Accept-Encoding", "gzip")
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("response status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	if response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("error Content-Type = %q, want application/json", response.Header().Get("Content-Type"))
	}
	if response.Header().Get("Content-Encoding") != "gzip" {
		t.Fatalf("error Content-Encoding = %q, want gzip", response.Header().Get("Content-Encoding"))
	}
	body := readGzipBody(t, response)
	if !strings.Contains(string(body), message) {
		prefix := body
		if len(prefix) > 160 {
			prefix = prefix[:160]
		}
		t.Fatalf("formatted error response length %d does not contain original message; prefix %q",
			len(body), prefix)
	}
}

func TestCompressJSONRespectsAcceptEncoding(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		header string
		want   bool
	}{
		{header: "gzip", want: true},
		{header: "gzip;q=0", want: false},
		{header: "*;q=1, gzip;q=0", want: false},
		{header: "*;q=0.5", want: true},
		{header: "br", want: false},
	} {
		t.Run(test.header, func(t *testing.T) {
			body := jsonBodyOfSize(jsonCompressionThreshold + 1)
			response := httptest.NewRecorder()
			handler := CompressJSON(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(body)
			}))
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request.Header.Set("Accept-Encoding", test.header)
			handler.ServeHTTP(response, request)

			encoded := response.Header().Get("Content-Encoding") == "gzip"
			if encoded != test.want {
				t.Fatalf("compressed = %v, want %v; headers = %v", encoded, test.want, response.Header())
			}
			if encoded {
				if got := readGzipBody(t, response); !bytes.Equal(got, body) {
					t.Fatal("decompressed response body differs from original")
				}
			} else if !bytes.Equal(response.Body.Bytes(), body) {
				t.Fatal("identity response body differs from original")
			}
		})
	}
}

func TestCompressJSONSkipsCookiesAndExistingRepresentations(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name           string
		header         string
		value          string
		wantEncoding   string
		wantHeaderName string
	}{
		{name: "set-cookie", header: "Set-Cookie", value: "session=private", wantHeaderName: "Set-Cookie"},
		{name: "content encoding", header: "Content-Encoding", value: "br", wantEncoding: "br", wantHeaderName: "Content-Encoding"},
		{name: "no-transform", header: "Cache-Control", value: "no-transform", wantHeaderName: "Cache-Control"},
		{name: "etag", header: "ETag", value: `"stable"`, wantHeaderName: "ETag"},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := jsonBodyOfSize(jsonCompressionThreshold + 1)
			response := httptest.NewRecorder()
			handler := CompressJSON(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set(test.header, test.value)
				_, _ = w.Write(body)
			}))
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request.Header.Set("Accept-Encoding", "gzip")
			handler.ServeHTTP(response, request)

			if got := response.Header().Get("Content-Encoding"); got != test.wantEncoding {
				t.Fatalf("Content-Encoding = %q, want %q", got, test.wantEncoding)
			}
			if got := response.Header().Get(test.wantHeaderName); got != test.value {
				t.Fatalf("%s = %q, want %q", test.wantHeaderName, got, test.value)
			}
			if !bytes.Equal(response.Body.Bytes(), body) {
				t.Fatal("uncompressed response body changed")
			}
		})
	}
}

func TestCompressJSONPreservesSSEFlushing(t *testing.T) {
	t.Parallel()
	response := httptest.NewRecorder()
	handler := CompressJSON(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("streaming response writer does not implement http.Flusher")
			return
		}
		if _, err := io.WriteString(w, "data: first\n\n"); err != nil {
			t.Errorf("write first event: %v", err)
			return
		}
		flusher.Flush()
		if _, err := io.WriteString(w, "data: second\n\n"); err != nil {
			t.Errorf("write second event: %v", err)
		}
	}))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Accept-Encoding", "gzip")
	handler.ServeHTTP(response, request)

	if response.Header().Get("Content-Encoding") != "" {
		t.Fatalf("SSE Content-Encoding = %q, want none", response.Header().Get("Content-Encoding"))
	}
	if !response.Flushed {
		t.Fatal("SSE flush did not reach the underlying response writer")
	}
	if got, want := response.Body.String(), "data: first\n\ndata: second\n\n"; got != want {
		t.Fatalf("SSE body = %q, want %q", got, want)
	}
}

func TestCompressJSONPassesProtocolUpgradesThrough(t *testing.T) {
	t.Parallel()
	response := &hijackingResponseRecorder{ResponseRecorder: httptest.NewRecorder()}
	var gotWriter http.ResponseWriter
	handler := CompressJSON(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		gotWriter = w
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Error("upgrade response writer does not implement http.Hijacker")
			return
		}
		if _, _, err := hijacker.Hijack(); err != nil {
			t.Errorf("hijack response: %v", err)
		}
	}))
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Accept-Encoding", "gzip")
	request.Header.Set("Connection", "keep-alive, Upgrade")
	request.Header.Set("Upgrade", "websocket")
	handler.ServeHTTP(response, request)

	if gotWriter != response {
		t.Fatal("protocol-upgrade response writer was wrapped")
	}
	if !response.hijacked {
		t.Fatal("protocol-upgrade request was not passed through")
	}
}

type hijackingResponseRecorder struct {
	*httptest.ResponseRecorder
	hijacked bool
}

func (w *hijackingResponseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.hijacked = true
	return nil, nil, nil
}

func jsonBodyOfSize(size int) []byte {
	const prefix = `{"payload":"`
	const suffix = `"}`
	return []byte(prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix)
}

func readGzipBody(t *testing.T, response *httptest.ResponseRecorder) []byte {
	t.Helper()
	reader, err := gzip.NewReader(bytes.NewReader(response.Body.Bytes()))
	if err != nil {
		t.Fatalf("create gzip reader: %v", err)
	}
	body, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil {
		t.Fatalf("read gzip response: %v", readErr)
	}
	if closeErr != nil {
		t.Fatalf("close gzip reader: %v", closeErr)
	}
	return body
}

func varyContains(header http.Header, wanted string) bool {
	for _, value := range header.Values("Vary") {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), wanted) {
				return true
			}
		}
	}
	return false
}
