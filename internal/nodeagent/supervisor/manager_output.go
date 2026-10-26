package supervisor

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/logging"
)

const maxLoggedOutputLine = 16 * 1024

func (m *Manager) logProcessOutput(reader *os.File, id string) {
	defer closeutil.Discard(reader)
	buffered := bufio.NewReaderSize(reader, 4096)
	line := make([]byte, 0, 4096)
	truncated := false
	for {
		part, err := buffered.ReadSlice('\n')
		if len(line) < maxLoggedOutputLine {
			available := maxLoggedOutputLine - len(line)
			if len(part) > available {
				line = append(line, part[:available]...)
				truncated = true
			} else {
				line = append(line, part...)
			}
		} else {
			truncated = true
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		if len(line) > 0 {
			m.logOutputLine(id, line, truncated)
			line = line[:0]
			truncated = false
		}
		if err == io.EOF {
			return
		}
		if err != nil {
			logging.FileWarnf("managed_engine", "managed engine %s output stream ended unexpectedly", id)
			return
		}
	}
}

func (m *Manager) logOutputLine(id string, line []byte, truncated bool) {
	if message := processOutputSummary(line, truncated); message != "" {
		logging.FileInfof("managed_engine", "managed engine %s: %s", id, message)
	}
}

func processOutputSummary(line []byte, truncated bool) string {
	if len(strings.TrimSpace(string(line))) == 0 {
		return ""
	}
	return fmt.Sprintf("output received (bytes=%d, truncated=%t); raw output omitted for privacy", len(line), truncated)
}
