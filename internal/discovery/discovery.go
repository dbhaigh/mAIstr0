// Package discovery finds other mAIstr0 processes (orchestrators and nodes)
// on the local network via periodic UDP broadcast announcements, so nodes
// don't need to be told the orchestrator's address by hand.
package discovery

import (
	"encoding/json"
	"log"
	"net"
	"sync"
	"syscall"
	"time"
)

// Port is the UDP port every instance broadcasts announcements on and
// listens for peers on.
const Port = 7999

const peerTTL = 15 * time.Second

// Announcement is broadcast periodically by every running instance.
type Announcement struct {
	Role     string  `json:"role"` // "orchestrator" or "node"
	ID       string  `json:"id"`
	HTTPAddr string  `json:"http_addr"`
	Score    float64 `json:"score,omitempty"` // capability score, used for orchestrator election
	Members  int     `json:"members,omitempty"`
	Elected  bool    `json:"elected,omitempty"`
}

// Peer is a discovered instance, with the time we last heard from it.
type Peer struct {
	Announcement
	LastSeen time.Time `json:"last_seen"`
}

// Beacon periodically broadcasts an announcement on the LAN until stop is closed.
func Beacon(get func() Announcement, interval time.Duration, stop <-chan struct{}) {
	conn, err := listenUDP("udp4", ":0", true)
	if err != nil {
		log.Printf("discovery: beacon disabled, could not open UDP socket: %v", err)
		return
	}
	defer conn.Close()

	send := func() {
		ann := get()
		if ann.ID == "" { // empty announcement = skip this tick (e.g. yielded orchestrator)
			return
		}
		buf, err := json.Marshal(ann)
		if err != nil {
			return
		}
		addrs := broadcastAddrs()
		if len(addrs) == 0 {
			log.Printf("discovery: no non-loopback IPv4 interface found; announcement not sent")
		}
		for _, addr := range addrs {
			if _, err := conn.WriteTo(buf, &net.UDPAddr{IP: addr, Port: Port}); err != nil {
				log.Printf("discovery: broadcast to %s failed: %v", addr, err)
			}
		}
	}

	send()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			send()
		}
	}
}

// Listener collects announcements from other instances on the LAN.
type Listener struct {
	selfIDs map[string]bool
	stop    chan struct{}
	conn    net.PacketConn

	mu    sync.RWMutex
	peers map[string]Peer
}

// Listen starts listening for peer announcements in the background. Any
// announcement whose ID is in selfIDs is ignored, which matters when a
// single process runs multiple roles (e.g. --role=both) sharing one socket.
// It never returns an error for callers that want to keep running without
// discovery (e.g. port in use); it just logs and leaves the peer set empty.
func Listen(selfIDs ...string) *Listener {
	exclude := make(map[string]bool, len(selfIDs))
	for _, id := range selfIDs {
		exclude[id] = true
	}
	l := &Listener{selfIDs: exclude, peers: make(map[string]Peer), stop: make(chan struct{})}
	go l.run()
	return l
}

func (l *Listener) run() {
	conn, err := listenUDP("udp4", ":"+itoa(Port), false)
	if err != nil {
		log.Printf("discovery: listener disabled, could not bind UDP port %d: %v", Port, err)
		return
	}
	l.mu.Lock()
	l.conn = conn
	l.mu.Unlock()
	defer conn.Close()
	defer func() {
		l.mu.Lock()
		l.conn = nil
		l.mu.Unlock()
	}()

	buf := make([]byte, 4096)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			select {
			case <-l.stop:
				return
			default:
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return
		}
		var ann Announcement
		if err := json.Unmarshal(buf[:n], &ann); err != nil {
			continue
		}
		if ann.ID == "" || l.selfIDs[ann.ID] {
			continue
		}
		l.mu.Lock()
		l.peers[ann.ID] = Peer{Announcement: ann, LastSeen: time.Now()}
		l.mu.Unlock()
	}
}

func listenUDP(network, address string, broadcast bool) (net.PacketConn, error) {
	var config net.ListenConfig
	config.Control = func(_, _ string, raw syscall.RawConn) error {
		return configureSocket(raw, broadcast)
	}
	return config.ListenPacket(nil, network, address)
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
		if p.LastSeen.After(cutoff) {
			out = append(out, p)
		} else {
			delete(l.peers, id)
		}
	}
	return out
}

// FirstOrchestrator blocks (up to timeout) waiting for an orchestrator to be
// discovered, returning its HTTP address. Returns "" if none is found in time.
func (l *Listener) FirstOrchestrator(timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, p := range l.Snapshot() {
			if p.Role == "orchestrator" {
				return p.HTTPAddr
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return ""
}

// BestOrchestrator returns the announcing orchestrator with the highest
// capability score (ties broken by lowest ID): the current election winner.
func (l *Listener) BestOrchestrator() (Peer, bool) {
	var best Peer
	found := false
	for _, p := range l.Snapshot() {
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

// OutboundIP makes a best-effort guess at this host's LAN IP by asking the
// OS which local address it would use to reach a public address (no packets
// are actually sent for a UDP "connect"). It never returns a loopback
// address: if the routing trick fails it falls back to the first
// non-loopback IPv4 interface so announcements stay reachable on the LAN.
func OutboundIP() string {
	conn, err := net.Dial("udp4", "8.8.8.8:80")
	if err == nil {
		defer conn.Close()
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

// firstLANIP returns the first non-loopback, non-link-local IPv4 address of
// any local interface, or "" if none exists.
func firstLANIP() string {
	addrs, err := net.InterfaceAddrs()
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

// broadcastAddrs returns the subnet broadcast address of every local IPv4
// LAN interface (loopback and link-local excluded), so announcements only
// go out on the local network, never to 127.0.0.1.
func broadcastAddrs() []net.IP {
	var out []net.IP
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return out
	}
	for _, a := range addrs {
		ipNet, ok := a.(*net.IPNet)
		if !ok || ipNet.IP.IsLoopback() || ipNet.IP.IsLinkLocalUnicast() {
			continue
		}
		ip4 := ipNet.IP.To4()
		if ip4 == nil {
			continue
		}
		mask := ipNet.Mask
		if len(mask) == 16 {
			mask = mask[12:]
		}
		if len(mask) != 4 {
			continue
		}
		bcast := make(net.IP, 4)
		for i := range ip4 {
			bcast[i] = ip4[i] | ^mask[i]
		}
		out = append(out, bcast)
	}
	return out
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
