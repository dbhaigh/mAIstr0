package httpx

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log/slog"
	"math"
	"mime"
	"net/http"
	"strconv"
	"strings"
)

const jsonCompressionThreshold = 10 * 1024

// AcceptsGzip reports whether an Accept-Encoding header permits gzip.
func AcceptsGzip(header http.Header) bool {
	gzipSeen := false
	gzipQuality := 0.0
	wildcardSeen := false
	wildcardQuality := 0.0

	for _, line := range header.Values("Accept-Encoding") {
		for _, value := range strings.Split(line, ",") {
			parts := strings.Split(value, ";")
			coding := strings.TrimSpace(parts[0])
			if coding == "" {
				continue
			}

			quality := 1.0
			for _, parameter := range parts[1:] {
				name, rawValue, ok := strings.Cut(strings.TrimSpace(parameter), "=")
				if !strings.EqualFold(strings.TrimSpace(name), "q") {
					continue
				}
				if !ok {
					quality = 0
					continue
				}
				parsed, err := strconv.ParseFloat(strings.TrimSpace(rawValue), 64)
				if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) || parsed < 0 || parsed > 1 {
					parsed = 0
				}
				if parsed < quality {
					quality = parsed
				}
			}

			switch {
			case strings.EqualFold(coding, "gzip"):
				if !gzipSeen || quality < gzipQuality {
					gzipQuality = quality
				}
				gzipSeen = true
			case coding == "*":
				if !wildcardSeen || quality < wildcardQuality {
					wildcardQuality = quality
				}
				wildcardSeen = true
			}
		}
	}

	if gzipSeen {
		return gzipQuality > 0
	}
	return wildcardSeen && wildcardQuality > 0
}

// CompressJSON gzip-encodes JSON responses larger than 10 KiB when accepted
// by the client. Non-JSON response types and protocol upgrades pass through.
func CompressJSON(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isProtocolUpgrade(r) {
			next.ServeHTTP(w, r)
			return
		}

		writer := &jsonCompressionWriter{
			ResponseWriter: w,
			method:         r.Method,
			rangeRequested: r.Header.Get("Range") != "",
			acceptsGzip:    AcceptsGzip(r.Header),
		}
		var wrapped http.ResponseWriter = writer
		if flusher, ok := w.(http.Flusher); ok {
			wrapped = &flushableJSONCompressionWriter{
				jsonCompressionWriter: writer,
				flusher:               flusher,
			}
		}

		defer writer.finish()
		next.ServeHTTP(wrapped, r)
	})
}

type jsonCompressionWriter struct {
	http.ResponseWriter
	method         string
	rangeRequested bool
	acceptsGzip    bool
	statusCode     int
	committed      bool
	body           bytes.Buffer
	gzipWriter     *gzip.Writer
}

type flushableJSONCompressionWriter struct {
	*jsonCompressionWriter
	flusher http.Flusher
}

func (w *jsonCompressionWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *jsonCompressionWriter) WriteHeader(statusCode int) {
	if statusCode >= 100 && statusCode < 200 && statusCode != http.StatusSwitchingProtocols {
		w.ResponseWriter.WriteHeader(statusCode)
		return
	}
	if w.statusCode != 0 || w.committed {
		return
	}

	w.statusCode = statusCode
	if isJSONContentType(w.Header().Get("Content-Type")) {
		addVaryHeader(w.Header(), "Accept-Encoding")
	}
	if !w.canCompress() {
		if err := w.commitIdentity(); err != nil {
			slog.Error("failed to write HTTP response", "component", "http", "error", err)
		}
	}
}

func (w *jsonCompressionWriter) Write(data []byte) (int, error) {
	if w.statusCode == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if w.gzipWriter != nil {
		return w.gzipWriter.Write(data)
	}
	if w.committed {
		return w.ResponseWriter.Write(data)
	}
	if !w.canCompress() {
		if err := w.commitIdentity(); err != nil {
			return 0, err
		}
		return w.ResponseWriter.Write(data)
	}
	if len(data) <= jsonCompressionThreshold-w.body.Len() {
		return w.body.Write(data)
	}
	if err := w.startGzip(); err != nil {
		return 0, err
	}
	if w.gzipWriter == nil {
		return w.ResponseWriter.Write(data)
	}
	return w.gzipWriter.Write(data)
}

func (w *jsonCompressionWriter) canCompress() bool {
	if !w.acceptsGzip || w.method == http.MethodHead || w.rangeRequested ||
		w.statusCode < 200 || w.statusCode == http.StatusNoContent ||
		w.statusCode == http.StatusResetContent || w.statusCode == http.StatusNotModified ||
		w.statusCode == http.StatusSwitchingProtocols ||
		!isJSONContentType(w.Header().Get("Content-Type")) ||
		w.Header().Get("Content-Encoding") != "" ||
		w.Header().Get("Content-Range") != "" ||
		w.Header().Get("ETag") != "" ||
		len(w.Header().Values("Set-Cookie")) != 0 ||
		hasNoTransform(w.Header().Values("Cache-Control")) {
		return false
	}
	return true
}

func (w *jsonCompressionWriter) startGzip() error {
	if !w.canCompress() {
		return w.commitIdentity()
	}

	w.Header().Set("Content-Encoding", "gzip")
	w.Header().Del("Content-Length")
	addVaryHeader(w.Header(), "Accept-Encoding")
	w.committed = true
	w.ResponseWriter.WriteHeader(w.statusCode)
	w.gzipWriter = gzip.NewWriter(w.ResponseWriter)

	pending := w.body.Bytes()
	if len(pending) == 0 {
		return nil
	}
	written, err := w.gzipWriter.Write(pending)
	w.body.Reset()
	if err != nil {
		return fmt.Errorf("compress buffered JSON response: %w", err)
	}
	if written != len(pending) {
		return io.ErrShortWrite
	}
	return nil
}

func (w *jsonCompressionWriter) commitIdentity() error {
	if w.committed {
		return nil
	}
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}
	w.committed = true
	w.ResponseWriter.WriteHeader(w.statusCode)

	pending := w.body.Bytes()
	if len(pending) == 0 {
		return nil
	}
	written, err := w.ResponseWriter.Write(pending)
	w.body.Reset()
	if err != nil {
		return err
	}
	if written != len(pending) {
		return io.ErrShortWrite
	}
	return nil
}

func (w *jsonCompressionWriter) finish() {
	if w.gzipWriter != nil {
		if err := w.gzipWriter.Close(); err != nil {
			slog.Error("failed to close compressed HTTP response", "component", "http", "error", err)
		}
		return
	}
	if w.committed || (w.statusCode == 0 && w.body.Len() == 0) {
		return
	}
	if err := w.commitIdentity(); err != nil {
		slog.Error("failed to write buffered HTTP response", "component", "http", "error", err)
	}
}

func (w *jsonCompressionWriter) flush() {
	if w.gzipWriter != nil {
		if err := w.gzipWriter.Flush(); err != nil {
			slog.Error("failed to flush compressed HTTP response", "component", "http", "error", err)
		}
		return
	}
	if !w.committed {
		if err := w.commitIdentity(); err != nil {
			slog.Error("failed to flush buffered HTTP response", "component", "http", "error", err)
		}
	}
}

func (w *flushableJSONCompressionWriter) Flush() {
	w.jsonCompressionWriter.flush()
	w.flusher.Flush()
}

func isJSONContentType(contentType string) bool {
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false
	}
	return strings.EqualFold(mediaType, "application/json") ||
		strings.HasSuffix(strings.ToLower(mediaType), "+json")
}

func addVaryHeader(header http.Header, value string) {
	for _, existing := range header.Values("Vary") {
		for _, token := range strings.Split(existing, ",") {
			token = strings.TrimSpace(token)
			if token == "*" || strings.EqualFold(token, value) {
				return
			}
		}
	}
	header.Add("Vary", value)
}

func hasNoTransform(values []string) bool {
	for _, value := range values {
		for _, directive := range strings.Split(value, ",") {
			name, _, _ := strings.Cut(strings.TrimSpace(directive), "=")
			if strings.EqualFold(name, "no-transform") {
				return true
			}
		}
	}
	return false
}

func isProtocolUpgrade(r *http.Request) bool {
	if r.Method == http.MethodConnect || strings.TrimSpace(r.Header.Get("Upgrade")) != "" {
		return true
	}
	for _, value := range r.Header.Values("Connection") {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), "upgrade") {
				return true
			}
		}
	}
	return false
}

var _ http.ResponseWriter = (*jsonCompressionWriter)(nil)
var _ http.Flusher = (*flushableJSONCompressionWriter)(nil)
