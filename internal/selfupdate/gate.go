// Package selfupdate implements paired-peer executable distribution and idle
// replacement. Peer checksums detect corruption, not a malicious trusted peer.
package selfupdate

import (
	"net/http"
	"sync"
)

type Gate struct {
	mu     sync.Mutex
	active int
	frozen bool
}

func (g *Gate) Enter() (func(), bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.frozen {
		return nil, false
	}
	g.active++
	return func() { g.mu.Lock(); g.active--; g.mu.Unlock() }, true
}

func (g *Gate) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet || r.Method == http.MethodHead {
			next.ServeHTTP(w, r)
			return
		}
		g.mu.Lock()
		if g.frozen {
			g.mu.Unlock()
			http.Error(w, "node is restarting for a peer update; retry after reconnection", http.StatusServiceUnavailable)
			return
		}
		g.active++
		g.mu.Unlock()
		defer func() { g.mu.Lock(); g.active--; g.mu.Unlock() }()
		next.ServeHTTP(w, r)
	})
}

func (g *Gate) Freeze() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.frozen || g.active != 0 {
		return false
	}
	g.frozen = true
	return true
}

func (g *Gate) Thaw() {
	g.mu.Lock()
	g.frozen = false
	g.mu.Unlock()
}
