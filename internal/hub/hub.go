// Package hub implements a tiny Server-Sent Events fan-out broadcaster so
// the orchestrator and every node can push cluster state to connected
// dashboards in real time instead of the browser having to poll for it.
package hub

import (
	"compress/gzip"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Hub fans a stream of JSON snapshots out to any number of subscribed HTTP
// clients (typically browser EventSource connections).
type Hub struct {
	// mu protects subscriptions; Broadcast makes only nonblocking sends while
	// holding it, and there is no nested hub lock or I/O.
	mu      sync.Mutex
	clients map[chan []byte]bool
	// subscribers mirrors len(clients) so publishers can skip building
	// snapshots without taking mu.
	subscribers atomic.Int64
	generation  atomic.Uint64
	subscribed  chan struct{}
}

func New() *Hub {
	return &Hub{clients: make(map[chan []byte]bool), subscribed: make(chan struct{}, 1)}
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
	if acceptsGzip(r.Header.Get("Accept-Encoding")) {
		w.Header().Set("Content-Encoding", "gzip")
		compressed = gzip.NewWriter(w)
		defer compressed.Close()
	}
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ch := h.subscribe()
	defer h.unsubscribe(ch)

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
		}
	}
}

func acceptsGzip(header string) bool {
	wildcardQuality := -1.0
	for _, value := range strings.Split(header, ",") {
		parts := strings.Split(strings.TrimSpace(value), ";")
		name := strings.TrimSpace(parts[0])
		quality := 1.0
		for _, parameter := range parts[1:] {
			parameterName, parameterValue, ok := strings.Cut(strings.TrimSpace(parameter), "=")
			if ok && strings.EqualFold(parameterName, "q") {
				parsed, err := strconv.ParseFloat(parameterValue, 64)
				if err != nil || parsed < 0 || parsed > 1 {
					quality = 0
				} else {
					quality = parsed
				}
				break
			}
		}
		if strings.EqualFold(name, "gzip") {
			return quality > 0
		}
		if name == "*" {
			wildcardQuality = quality
		}
	}
	return wildcardQuality > 0
}
