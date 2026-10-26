// Package discovery finds other mAIstr0 processes (orchestrators and nodes)
// on the local network via periodic UDP broadcast announcements, so nodes
// don't need to be told the orchestrator's address by hand.
package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/maistr0/maistr0/internal/logging"
	"net"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
)

// Port is the UDP port every instance broadcasts announcements on and
// listens for peers on.
const Port = 7999

const peerTTL = 15 * time.Second

// Announcement is broadcast periodically by every running instance.
type Announcement struct {
	Role           string  `json:"role"` // "orchestrator" or "node"
	ID             string  `json:"id"`
	HTTPAddr       string  `json:"http_addr"`       // plaintext pairing bootstrap, loopback clients only after pairing
	PeerAddr       string  `json:"peer_addr"`       // mutual-TLS address used for cluster traffic
	Score          float64 `json:"score,omitempty"` // capability score, used for orchestrator election
	Members        int     `json:"members,omitempty"`
	Elected        bool    `json:"elected,omitempty"`
	OrchestratorID string  `json:"orchestrator_id,omitempty"`
}

// Peer is a discovered instance, with the time we last heard from it.
type Peer struct {
	Announcement
	LastSeen time.Time `json:"last_seen"`
}

// Listener collects announcements from other instances on the LAN.
type Listener struct {
	selfIDs map[string]bool
	stop    chan struct{}
	conn    net.PacketConn
	ready   chan struct{}
	// bind opens the discovery socket; nil uses UDP Port. Tests inject fakes.
	bind func() (net.PacketConn, error)

	// mu protects peers and listener readiness/connection state. Copy values
	// under the lock, then release it before closing connections or waiting.
	mu       sync.RWMutex
	peers    map[string]Peer
	excluded map[string]bool
	readyErr error
}

// Listen starts listening for peer announcements in the background. Any
// announcement whose ID is in selfIDs is ignored, which matters when a
// single process runs multiple roles (e.g. --role=both) sharing one socket.
// It never returns an error for callers that want to keep running without
// discovery (e.g. port in use); it just logs and leaves the peer set empty.
func Listen(selfIDs ...string) *Listener {
	return listenWith(nil, selfIDs...)
}

func listenWith(bind func() (net.PacketConn, error), selfIDs ...string) *Listener {
	exclude := make(map[string]bool, len(selfIDs))
	for _, id := range selfIDs {
		exclude[id] = true
	}
	l := &Listener{
		selfIDs:  exclude,
		peers:    make(map[string]Peer),
		excluded: make(map[string]bool),
		stop:     make(chan struct{}),
		ready:    make(chan struct{}),
		bind:     bind,
	}
	go l.run()
	return l
}

const (
	listenerRetryMin = 250 * time.Millisecond
	listenerRetryMax = 5 * time.Second
)

func bindListenerPort() (net.PacketConn, error) {
	return listenUDP("udp4", ":"+itoa(Port), false)
}

func (l *Listener) bindPort() (net.PacketConn, error) {
	if l.bind != nil {
		return l.bind()
	}
	return bindListenerPort()
}

func (l *Listener) run() {
	conn, err := l.bindPort()
	if err != nil {
		logging.Errorf("discovery", "discovery: listener disabled, could not bind UDP port %d: %v", Port, err)
		l.mu.Lock()
		l.readyErr = err
		close(l.ready)
		l.mu.Unlock()
		return
	}
	if !l.setConn(conn) {
		close(l.ready)
		return
	}
	close(l.ready)

	for conn != nil {
		err := l.serve(conn)
		_ = conn.Close()
		l.setConn(nil)
		if err == nil {
			return
		}
		// A socket error would otherwise end discovery silently while the
		// process keeps running; reopen the port instead.
		logging.Errorf("discovery", "discovery: listener read on UDP port %d failed: %v; reopening", Port, err)
		conn = l.rebind()
	}
}

// setConn publishes the active socket. It returns false (and closes conn)
// when the listener was stopped concurrently, so Close never misses a socket.
func (l *Listener) setConn(conn net.PacketConn) bool {
	l.mu.Lock()
	l.conn = conn
	l.mu.Unlock()
	if conn != nil && l.stopped() {
		l.mu.Lock()
		l.conn = nil
		l.mu.Unlock()
		_ = conn.Close()
		return false
	}
	return true
}

func (l *Listener) stopped() bool {
	select {
	case <-l.stop:
		return true
	default:
		return false
	}
}

// rebind retries binding the discovery port with capped exponential backoff
// until it succeeds or the listener stops (returning nil).
func (l *Listener) rebind() net.PacketConn {
	delay := listenerRetryMin
	for {
		timer := time.NewTimer(delay)
		select {
		case <-l.stop:
			timer.Stop()
			return nil
		case <-timer.C:
		}
		conn, err := l.bindPort()
		if err == nil {
			if !l.setConn(conn) {
				return nil
			}
			logging.Infof("discovery", "discovery: listener reopened UDP port %d", Port)
			return conn
		}
		logging.Errorf("discovery", "discovery: could not reopen UDP port %d: %v", Port, err)
		delay = min(delay*2, listenerRetryMax)
	}
}

// serve reads announcements until the listener stops (nil) or the socket
// returns a non-timeout error.
func (l *Listener) serve(conn net.PacketConn) error {
	buf := make([]byte, 4096)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			if l.stopped() {
				return nil
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return err
		}
		var ann Announcement
		if err := json.Unmarshal(buf[:n], &ann); err != nil {
			continue
		}
		if ann.ID == "" || l.selfIDs[ann.ID] {
			continue
		}
		l.mu.Lock()
		if !l.excluded[ann.ID] {
			l.peers[ann.ID] = Peer{Announcement: ann, LastSeen: time.Now()}
		}
		l.mu.Unlock()
	}
}

func listenUDP(network, address string, broadcast bool) (net.PacketConn, error) {
	var config net.ListenConfig
	config.Control = func(_, _ string, raw syscall.RawConn) error {
		return configureSocket(raw, broadcast)
	}
	return config.ListenPacket(context.Background(), network, address)
}

// WaitReady waits until the UDP listener has bound successfully or failed.
func (l *Listener) WaitReady(timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-l.ready:
		l.mu.RLock()
		err := l.readyErr
		l.mu.RUnlock()
		if err != nil {
			return fmt.Errorf("discovery listener failed: %w", err)
		}
		return nil
	case <-timer.C:
		return fmt.Errorf("discovery listener did not start within %s", timeout)
	}
}

// Close stops the listener. It is used by startup auto-detection so the
// probe socket does not block the real node/orchestrator listener.
func (l *Listener) Close() {
	select {
	case <-l.stop:
		return
	default:
		close(l.stop)
	}
	l.mu.RLock()
	conn := l.conn
	l.mu.RUnlock()
	if conn != nil {
		_ = conn.Close()
	}
}

// Snapshot returns all peers seen within the discovery lease window. Expired
// peers are removed so callers cannot keep retrying a node that disappeared.
func (l *Listener) Snapshot() []Peer {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Peer, 0, len(l.peers))
	cutoff := time.Now().Add(-peerTTL)
	for id, p := range l.peers {
		if l.excluded[id] {
			delete(l.peers, id)
			continue
		}
		if p.LastSeen.After(cutoff) {
			out = append(out, p)
		} else {
			delete(l.peers, id)
		}
	}
	return out
}

// ExcludePeer suppresses future announcements from a peer until IncludePeer
// is called. It is used while a health check has declared the current leader
// unavailable, so that stale beacons cannot keep it in the failover election.
func (l *Listener) ExcludePeer(id string) {
	if id == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.excluded == nil {
		l.excluded = make(map[string]bool)
	}
	l.excluded[id] = true
	delete(l.peers, id)
}

// IncludePeer resumes accepting announcements from a previously excluded peer.
func (l *Listener) IncludePeer(id string) {
	if id == "" {
		return
	}
	l.mu.Lock()
	delete(l.excluded, id)
	l.mu.Unlock()
}

// FirstOrchestrator blocks (up to timeout) waiting for an orchestrator to be
// discovered, returning its mutual-TLS address. Returns "" if none is found.
func (l *Listener) FirstOrchestrator(timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, p := range l.Snapshot() {
			if p.Role == "orchestrator" {
				return p.PeerAddr
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return ""
}

// BestOrchestrator returns the announcing orchestrator with the highest
// capability score (ties broken by lowest ID): the current election winner.
func (l *Listener) BestOrchestrator() (Peer, bool) {
	return BestActiveOrchestrator(l.Snapshot())
}

// BestActiveOrchestrator selects from running orchestrators, ignoring
// candidates that have not completed startup.
func BestActiveOrchestrator(peers []Peer) (Peer, bool) {
	var best Peer
	found := false
	for _, p := range peers {
		if p.Role != "orchestrator" {
			continue
		}
		if !found || p.Score > best.Score || (p.Score == best.Score && p.ID < best.ID) {
			best = p
			found = true
		}
	}
	return best, found
}

// BestOrchestratorCandidate elects from active orchestrators and instances
// currently starting up as candidates. Higher capability wins; ties are
// broken by the lexicographically lowest stable ID.
func BestOrchestratorCandidate(self Announcement, peers []Peer) Announcement {
	best := self
	for _, peer := range peers {
		if peer.Role != "orchestrator" && peer.Role != "candidate" {
			continue
		}
		if outranks(peer.Announcement, best) {
			best = peer.Announcement
		}
	}
	return best
}

// IsBestOrchestratorCandidate reports whether self wins against the visible
// orchestrator candidates.
func IsBestOrchestratorCandidate(self Announcement, peers []Peer) bool {
	return BestOrchestratorCandidate(self, peers).ID == self.ID
}

// IsElectedOrchestrator compares against live leaders first so a candidate
// cannot preempt a running orchestrator during its startup election.
func IsElectedOrchestrator(self Announcement, peers []Peer) bool {
	active := make([]Peer, 0, len(peers))
	for _, peer := range peers {
		if peer.Role == "orchestrator" {
			active = append(active, peer)
		}
	}
	if len(active) > 0 {
		return IsBestOrchestratorCandidate(self, active)
	}
	if self.Role == "orchestrator" {
		return true
	}
	return IsBestOrchestratorCandidate(self, peers)
}

func outranks(candidate, incumbent Announcement) bool {
	return candidate.Score > incumbent.Score ||
		(candidate.Score == incumbent.Score && candidate.ID < incumbent.ID)
}

// OutboundIP makes a best-effort guess at this host's LAN IP by asking the
// OS which local address it would use to reach a public address (no packets
// are actually sent for a UDP "connect"). It never returns a loopback
// address: if the routing trick fails it falls back to the first
// non-loopback IPv4 interface so announcements stay reachable on the LAN.
func OutboundIP() string {
	conn, err := net.Dial("udp4", "8.8.8.8:80")
	if err == nil {
		defer closeutil.Discard(conn)
		ip := conn.LocalAddr().(*net.UDPAddr).IP
		if !ip.IsLoopback() {
			return ip.String()
		}
	}
	if ip := firstLANIP(); ip != "" {
		return ip
	}
	return "127.0.0.1"
}

var interfaceAddrs = net.InterfaceAddrs

// firstLANIP returns the first non-loopback, non-link-local IPv4 address of
// any local interface, or "" if none exists.
func firstLANIP() string {
	addrs, err := interfaceAddrs()
	if err != nil {
		return ""
	}
	for _, a := range addrs {
		ipNet, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		ip4 := ipNet.IP.To4()
		if ip4 == nil || ip4.IsLoopback() || ip4.IsLinkLocalUnicast() {
			continue
		}
		return ip4.String()
	}
	return ""
}

// IsSameSubnet reports whether the given IP is on the same subnet as any of
// this host's local network interfaces (including loopback).
func IsSameSubnet(target net.IP) bool {
	if target == nil {
		return false
	}
	if target.IsLoopback() {
		return true
	}
	if ip4 := target.To4(); ip4 != nil {
		target = ip4
	}
	addrs, err := interfaceAddrs()
	if err != nil {
		return false
	}
	for _, a := range addrs {
		ipNet, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		ones, _ := ipNet.Mask.Size()
		if ones == 0 {
			continue
		}
		if ipNet.Contains(target) {
			return true
		}
	}
	return false
}

// IsSameSubnetAddress parses an IP, host:port, or URL and reports whether
// the target IP is on the same subnet as any local interface.
func IsSameSubnetAddress(addr string) bool {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return false
	}
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		parsed, err := url.Parse(addr)
		if err == nil && parsed.Hostname() != "" {
			addr = parsed.Hostname()
		}
	}
	if host, _, err := net.SplitHostPort(addr); err == nil {
		addr = host
	}
	if addr == "localhost" {
		return true
	}
	ip := net.ParseIP(addr)
	if ip == nil {
		return false
	}
	return IsSameSubnet(ip)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [8]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
