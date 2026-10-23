package logging

import (
	"bytes"
	"io"
)

var fileOnlyPrefix = []byte("FILE_ONLY: ")
var fileOnlyAttribute = []byte(`"file_only":true`)

type consoleWriter struct {
	writer io.Writer
}

func NewConsoleWriter(writer io.Writer) io.Writer {
	return consoleWriter{writer: writer}
}

func (w consoleWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, fileOnlyPrefix) || bytes.Contains(p, fileOnlyAttribute) {
		return len(p), nil
	}
	return w.writer.Write(p)
}
