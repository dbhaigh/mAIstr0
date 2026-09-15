// Package hub implements a tiny Server-Sent Events fan-out broadcaster so
// the orchestrator and every node can push cluster state to connected
// dashboards in real time instead of the browser having to poll for it.
package hub

import (
	"fmt"
	"net/http"
	"sync"
)

// Hub fans a stream of JSON snapshots out to any number of subscribed HTTP
// clients (typically browser EventSource connections).
type Hub struct {
	mu      sync.Mutex
	clients map[chan []byte]bool
}

func New() *Hub {
	return &Hub{clients: make(map[chan []byte]bool)}
}

// Broadcast sends data to every currently-subscribed client. Slow/stuck
// clients are dropped rather than allowed to block publishers.
func (h *Hub) Broadcast(data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- data:
		default:
			// client isn't keeping up; drop this update for it rather than block.
		}
	}
}

func (h *Hub) subscribe() chan []byte {
	ch := make(chan []byte, 8)
	h.mu.Lock()
	h.clients[ch] = true
	h.mu.Unlock()
	return ch
}

func (h *Hub) unsubscribe(ch chan []byte) {
	h.mu.Lock()
	delete(h.clients, ch)
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
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}
