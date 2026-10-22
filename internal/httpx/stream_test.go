package httpx

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

type flushCounter struct {
	*httptest.ResponseRecorder
	calls int
}

func (w *flushCounter) Flush() {
	w.calls++
	w.ResponseRecorder.Flush()
}

func TestStreamWriterFormatsAndFlushesSSEJSONLinesAndCopiedBytes(t *testing.T) {
	target := &flushCounter{ResponseRecorder: httptest.NewRecorder()}
	stream := NewStreamWriter(target)
	for _, write := range []func() error{
		func() error { return stream.WriteSSEData([]byte(`{"output":"delta"}`)) },
		func() error { return stream.WriteSSEEvent("message_start", []byte(`{"type":"start"}`)) },
		func() error { return stream.WriteJSONLine(map[string]int{"count": 1}) },
		func() error { return stream.WriteJSONLine(map[string]string{"text": "<&>"}) },
		func() error { return stream.WriteLine("line") },
		func() error { return stream.Copy(bytes.NewBufferString("raw"), nil) },
	} {
		if err := write(); err != nil {
			t.Fatal(err)
		}
	}
	want := "data: {\"output\":\"delta\"}\n\n" +
		"event: message_start\ndata: {\"type\":\"start\"}\n\n" +
		"{\"count\":1}\n" +
		"{\"text\":\"\\u003c\\u0026\\u003e\"}\nline\nraw"
	if got := target.Body.String(); got != want {
		t.Fatalf("stream output = %q, want %q", got, want)
	}
	if target.calls != 6 {
		t.Fatalf("flush count = %d, want 6", target.calls)
	}
}

func TestStreamWriterRejectsMalformedEventNameAndReturnsWriteErrors(t *testing.T) {
	stream := NewStreamWriter(&failingResponseWriter{})
	if err := stream.WriteSSEEvent("bad\nevent", nil); err == nil {
		t.Fatal("WriteSSEEvent accepted a newline in the event name")
	}
	if err := stream.WriteSSEData([]byte("data")); err == nil {
		t.Fatal("WriteSSEData ignored the response writer error")
	}
	if err := stream.Copy(failingReader{}, nil); !errors.Is(err, errStreamRead) {
		t.Fatalf("Copy error = %v, want %v", err, errStreamRead)
	}
}

type failingResponseWriter struct{}

func (*failingResponseWriter) Header() http.Header { return make(http.Header) }

func (*failingResponseWriter) WriteHeader(int) {}

func (*failingResponseWriter) Write([]byte) (int, error) {
	return 0, io.ErrClosedPipe
}

var errStreamRead = errors.New("stream read failed")

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) {
	return 0, errStreamRead
}
