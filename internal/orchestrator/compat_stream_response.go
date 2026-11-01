package orchestrator

import (
	"net/http"

	"github.com/maistr0/maistr0/internal/httpx"
)

type compatStreamResponse struct {
	target      http.ResponseWriter
	header      http.Header
	status      int
	contentType string
	connection  string
	stream      *httpx.StreamWriter
}

func newCompatStreamResponse(
	target http.ResponseWriter,
	bufferedHeader http.Header,
	contentType string,
	connection string,
) compatStreamResponse {
	return compatStreamResponse{
		target: target, header: bufferedHeader, contentType: contentType,
		connection: connection, stream: httpx.NewStreamWriter(target),
	}
}

// Header returns the buffered response headers, or the target's headers when
// no buffer was configured.
//
// Returns: the header map used to prepare the compatibility response.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [compat_streaming_test.go](compat_streaming_test.go).
func (w *compatStreamResponse) Header() http.Header {
	if w.header != nil {
		return w.header
	}
	return w.target.Header()
}

// WriteHeader commits status once and applies the compatibility stream headers
// before writing the response body.
//
// Parameters: status (int) is the HTTP status code to commit.
//
// References: [request paths](../../docs/architecture.md#request-paths),
// [compat_streaming_test.go](compat_streaming_test.go).
func (w *compatStreamResponse) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	if w.header != nil {
		for key, values := range w.header {
			for _, value := range values {
				w.target.Header().Add(key, value)
			}
		}
	}
	if status < http.StatusBadRequest {
		w.target.Header().Set("Content-Type", w.contentType)
		w.target.Header().Set("Cache-Control", "no-cache")
		if w.connection != "" {
			w.target.Header().Set("Connection", w.connection)
		}
	}
	w.target.WriteHeader(status)
}

func (w *compatStreamResponse) write(
	data []byte,
	parser *compatStreamParser,
	consume func([]byte) error,
) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if w.status >= http.StatusBadRequest {
		return w.target.Write(data)
	}
	if err := parser.write(data, consume); err != nil {
		return len(data), err
	}
	return len(data), nil
}

func (w *compatStreamResponse) flush() {
	w.stream.Flush()
}
