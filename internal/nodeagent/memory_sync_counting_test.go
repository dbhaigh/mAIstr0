package nodeagent

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/maistr0/maistr0/internal/syncio"
)

type discardMemorySyncResponseWriter struct{}

func (discardMemorySyncResponseWriter) Header() http.Header         { return nil }
func (discardMemorySyncResponseWriter) WriteHeader(int)             {}
func (discardMemorySyncResponseWriter) Write(p []byte) (int, error) { return len(p), nil }

func BenchmarkMemorySyncCountingIO(b *testing.B) {
	b.Run("reader", func(b *testing.B) {
		source := strings.NewReader("sync payload")
		readCloser := io.NopCloser(source)
		buffer := make([]byte, len("sync payload"))
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			source.Reset("sync payload")
			reader := &syncio.CountingReadCloser{ReadCloser: readCloser}
			if _, err := reader.Read(buffer); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("response-writer", func(b *testing.B) {
		target := discardMemorySyncResponseWriter{}
		payload := []byte("sync payload")
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			writer := &syncio.CountingResponseWriter{ResponseWriter: target}
			if _, err := writer.Write(payload); err != nil {
				b.Fatal(err)
			}
		}
	})
}
