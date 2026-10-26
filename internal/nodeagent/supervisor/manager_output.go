package supervisor

import (
	"bufio"
	"io"
	"os"
	"regexp"
	"strings"
	"unicode"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/logging"
)

const maxLoggedOutputLine = 16 * 1024

var (
	processSecretAssignment = regexp.MustCompile(`(?i)([A-Za-z0-9_.-]*(?:api[_-]?key|access[_-]?token|token|password|secret|credential)[A-Za-z0-9_.-]*["']?\s*[:=]\s*["']?)[^"' \t,;}&]+`)
	processAuthorization    = regexp.MustCompile(`(?i)(authorization\s*[:=]\s*)(?:bearer|basic)\s+[^\s,;]+`)
	processBearer           = regexp.MustCompile(`(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+`)
	processURLUserInfo      = regexp.MustCompile(`(?i)(https?://)[^/\s:@]+:[^/@\s]+@`)
)

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
	message := sanitizedProcessOutput(line, truncated)
	if message != "" {
		logging.FileInfof("managed_engine", "managed engine %s: %s", id, message)
	}
}

func sanitizedProcessOutput(line []byte, truncated bool) string {
	message := strings.TrimSpace(string(line))
	message = strings.Map(func(char rune) rune {
		if unicode.IsControl(char) && char != '\t' {
			return -1
		}
		return char
	}, strings.ToValidUTF8(message, "\uFFFD"))
	message = processSecretAssignment.ReplaceAllString(message, "$1[redacted]")
	message = processAuthorization.ReplaceAllString(message, "$1[redacted]")
	message = processBearer.ReplaceAllString(message, "Bearer [redacted]")
	message = processURLUserInfo.ReplaceAllString(message, "${1}[redacted]@")
	if truncated {
		message += " [truncated]"
	}
	return message
}
