package syncio

import (
	"io"
	"net/http"
)

type CountingReadCloser struct {
	io.ReadCloser
	BytesRead int64
}

func (r *CountingReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.BytesRead += int64(n)
	return n, err
}

type CountingResponseWriter struct {
	http.ResponseWriter
	BytesWritten int64
	WriteErr     error
}

func (w *CountingResponseWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseWriter.Write(p)
	w.BytesWritten += int64(n)
	if err != nil && w.WriteErr == nil {
		w.WriteErr = err
	}
	return n, err
}
