package nodeagent

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/maistr0/maistr0/internal/inferroute"
)

var syncLogSequence atomic.Uint64

func syncLogRequestID(r *http.Request) string {
	if r != nil {
		if requestID := strings.TrimSpace(r.Header.Get(inferroute.HeaderRequestID)); requestID != "" {
			return requestID
		}
	}
	return "sync-" + strconv.FormatUint(syncLogSequence.Add(1), 10)
}

func syncLogFields(requestID, nodeID string, elapsed time.Duration, failureClass string) string {
	return fmt.Sprintf(
		"request_id=%q attempt_id=none node_id=%q model=none engine=none duration_ms=%d failure_class=%q",
		requestID, nodeID, elapsed.Milliseconds(), failureClass,
	)
}
