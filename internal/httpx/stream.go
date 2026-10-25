package httpx

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

type StreamWriter struct {
	target  http.ResponseWriter
	flusher http.Flusher
}

func NewStreamWriter(target http.ResponseWriter) *StreamWriter {
	flusher, _ := target.(http.Flusher)
	return &StreamWriter{target: target, flusher: flusher}
}

func (w *StreamWriter) Write(data []byte) error {
	written, err := w.target.Write(data)
	if err == nil && written != len(data) {
		err = io.ErrShortWrite
	}
	if err == nil {
		w.Flush()
	}
	return err
}

func (w *StreamWriter) WriteSSEData(data []byte) error {
	frame := make([]byte, 0, len(data)+8)
	frame = append(frame, "data: "...)
	frame = append(frame, data...)
	frame = append(frame, '\n', '\n')
	return w.Write(frame)
}

func (w *StreamWriter) WriteSSEEvent(event string, data []byte) error {
	if strings.ContainsAny(event, "\r\n") {
		return errors.New("SSE event name must not contain a newline")
	}
	frame := make([]byte, 0, len(event)+len(data)+16)
	frame = append(frame, "event: "...)
	frame = append(frame, event...)
	frame = append(frame, '\n')
	frame = append(frame, "data: "...)
	frame = append(frame, data...)
	frame = append(frame, '\n', '\n')
	return w.Write(frame)
}

func (w *StreamWriter) WriteJSONLine(value any) error {
	var frame bytes.Buffer
	if err := json.NewEncoder(&frame).Encode(value); err != nil {
		return err
	}
	return w.Write(frame.Bytes())
}

func (w *StreamWriter) WriteLine(line string) error {
	frame := make([]byte, 0, len(line)+1)
	frame = append(frame, line...)
	frame = append(frame, '\n')
	return w.Write(frame)
}

func (w *StreamWriter) Copy(source io.Reader, buffer []byte) error {
	if len(buffer) == 0 {
		buffer = make([]byte, 32<<10)
	}
	_, err := io.CopyBuffer(streamCopyWriter{writer: w}, source, buffer)
	return err
}

func (w *StreamWriter) Flush() {
	if w.flusher != nil {
		w.flusher.Flush()
	}
}

type streamCopyWriter struct {
	writer *StreamWriter
}

func (w streamCopyWriter) Write(data []byte) (int, error) {
	if err := w.writer.Write(data); err != nil {
		return 0, err
	}
	return len(data), nil
}
