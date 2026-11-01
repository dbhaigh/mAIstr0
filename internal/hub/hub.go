// Package hub implements a tiny Server-Sent Events fan-out broadcaster so
// the orchestrator and every node can push cluster state to connected
// dashboards in real time instead of the browser having to poll for it.
package hub

import (
	"compress/gzip"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maistr0/maistr0/internal/httpx"
)

const defaultHeartbeatInterval = 15 * time.Second

// Hub fans a stream of JSON snapshots out to any number of subscribed HTTP
// clients (typically browser EventSource connections).
type Hub struct {
	// mu protects subscriptions; Broadcast makes only nonblocking sends while
	// holding it, and there is no nested hub lock or I/O.
	mu      sync.Mutex
	clients map[chan []byte]bool
	// subscribers mirrors len(clients) so publishers can skip building
	// snapshots without taking mu.
	subscribers       atomic.Int64
	generation        atomic.Uint64
	subscribed        chan struct{}
	heartbeatInterval time.Duration
}

func New() *Hub {
	return &Hub{
		clients:           make(map[chan []byte]bool),
		subscribed:        make(chan struct{}, 1),
		heartbeatInterval: defaultHeartbeatInterval,
	}
}

// HasSubscribers reports whether any client is currently subscribed.
// Publishers should check it before building an expensive payload.
func (h *Hub) HasSubscribers() bool {
	return h.subscribers.Load() > 0
}

// Broadcast sends data to every currently-subscribed client. Updates for slow
// clients are dropped rather than allowed to block publishers.
func (h *Hub) Broadcast(data []byte) {
	h.broadcast(data)
}

func (h *Hub) broadcast(data []byte) bool {
	if !h.HasSubscribers() {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	delivered := len(h.clients) > 0
	for ch := range h.clients {
		select {
		case ch <- data:
		default:
			delivered = false
			// client isn't keeping up; drop this update for it rather than block.
		}
	}
	return delivered
}

func (h *Hub) subscribe() chan []byte {
	ch := make(chan []byte, 8)
	h.mu.Lock()
	h.clients[ch] = true
	h.subscribers.Add(1)
	h.generation.Add(1)
	h.mu.Unlock()
	select {
	case h.subscribed <- struct{}{}:
	default:
	}
	return ch
}

func (h *Hub) unsubscribe(ch chan []byte) {
	h.mu.Lock()
	delete(h.clients, ch)
	h.subscribers.Add(-1)
	h.mu.Unlock()
	close(ch)
}

// ServeHTTP streams Server-Sent Events to the caller until it disconnects.
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Add("Vary", "Accept-Encoding")
	var compressed *gzip.Writer
	if httpx.AcceptsGzip(r.Header) {
		w.Header().Set("Content-Encoding", "gzip")
		compressed = gzip.NewWriter(w)
		defer func() {
			if err := compressed.Close(); err != nil {
				slog.Error("failed to close compressed SSE stream", "component", "hub", "error", err)
			}
		}()
	}
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ch := h.subscribe()
	defer h.unsubscribe(ch)
	heartbeatInterval := h.heartbeatInterval
	if heartbeatInterval <= 0 {
		heartbeatInterval = defaultHeartbeatInterval
	}
	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case data, ok := <-ch:
			if !ok {
				return
			}
			if compressed != nil {
				if _, err := fmt.Fprintf(compressed, "data: %s\n\n", data); err != nil {
					return
				}
				if err := compressed.Flush(); err != nil {
					return
				}
			} else if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				return
			}
			flusher.Flush()
		case <-heartbeat.C:
			const frame = "event: heartbeat\ndata: ping\n\n"
			if compressed != nil {
				if _, err := fmt.Fprint(compressed, frame); err != nil {
					return
				}
				if err := compressed.Flush(); err != nil {
					return
				}
			} else if _, err := fmt.Fprint(w, frame); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
