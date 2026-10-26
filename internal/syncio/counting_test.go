package syncio

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type countingWriteResult struct {
	n   int
	err error
}

type scriptedResponseWriter struct {
	header    http.Header
	results   []countingWriteResult
	nextWrite int
}

func (w *scriptedResponseWriter) Header() http.Header { return w.header }

func (w *scriptedResponseWriter) WriteHeader(int) {}

func (w *scriptedResponseWriter) Write([]byte) (int, error) {
	result := w.results[w.nextWrite]
	w.nextWrite++
	return result.n, result.err
}

func TestCountingReadCloser(t *testing.T) {
	reader := &CountingReadCloser{ReadCloser: io.NopCloser(strings.NewReader("input"))}
	buffer := make([]byte, 2)
	for _, want := range []int64{2, 4, 5} {
		if _, err := reader.Read(buffer); err != nil {
			t.Fatal(err)
		}
		if reader.BytesRead != want {
			t.Fatalf("BytesRead = %d, want %d", reader.BytesRead, want)
		}
	}
	if _, err := reader.Read(buffer); !errors.Is(err, io.EOF) || reader.BytesRead != 5 {
		t.Fatalf("final read = (%d bytes, %v), want (0, EOF)", reader.BytesRead, err)
	}
}

func TestCountingResponseWriter(t *testing.T) {
	firstErr := errors.New("first write error")
	secondErr := errors.New("second write error")
	writer := &CountingResponseWriter{
		ResponseWriter: &scriptedResponseWriter{
			header: make(http.Header),
			results: []countingWriteResult{
				{n: 2, err: firstErr},
				{n: 1, err: secondErr},
			},
		},
	}
	if n, err := writer.Write([]byte("abcd")); n != 2 || !errors.Is(err, firstErr) {
		t.Fatalf("first write = (%d, %v), want (2, first error)", n, err)
	}
	if n, err := writer.Write([]byte("xyz")); n != 1 || !errors.Is(err, secondErr) {
		t.Fatalf("second write = (%d, %v), want (1, second error)", n, err)
	}
	if writer.BytesWritten != 3 || !errors.Is(writer.WriteErr, firstErr) {
		t.Fatalf("writer counters = (%d, %v), want (3, first error)", writer.BytesWritten, writer.WriteErr)
	}
}
