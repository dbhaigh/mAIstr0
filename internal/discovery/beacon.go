package discovery

import (
	"encoding/json"
	"net"
	"time"

	"github.com/maistr0/maistr0/internal/logging"
)

// broadcastRoute pairs a local IPv4 interface address with its subnet
// broadcast address.
type broadcastRoute struct {
	Local     net.IP
	Broadcast net.IP
}

// Beacon periodically broadcasts an announcement on the LAN until stop is closed.
//
// Each announcement is sent from a socket bound to every local LAN address.
// On multi-homed hosts (for example a Hyper-V external switch plus a bridge
// on the same subnet) an unbound socket lets the OS pick one egress
// interface for every subnet broadcast, which may not be the one peers can
// hear. Binding the source address keeps each copy on its own interface.
func Beacon(get func() Announcement, interval time.Duration, stop <-chan struct{}) {
	BeaconWithSecret(get, interval, stop, nil)
}

func BeaconWithSecret(get func() Announcement, interval time.Duration, stop <-chan struct{}, secret []byte) {
	fallback, err := listenUDP("udp4", ":0", true)
	if err != nil {
		logging.Errorf("discovery", "discovery: beacon disabled, could not open UDP socket: %v", err)
		return
	}
	sender := newBeaconSender(fallback, func(local net.IP) (net.PacketConn, error) {
		return listenUDP("udp4", net.JoinHostPort(local.String(), "0"), true)
	})
	defer sender.close()

	send := func() {
		ann := get()
		if ann.ID == "" { // empty announcement = skip this tick (e.g. yielded orchestrator)
			return
		}
		if len(secret) != 0 {
			ann, err = signAnnouncement(ann, secret, time.Now())
			if err != nil {
				logging.Errorf("discovery", "discovery: could not authenticate LAN announcement: %v", err)
				return
			}
		}
		buf, err := json.Marshal(ann)
		if err != nil {
			return
		}
		routes := broadcastRoutes()
		if len(routes) == 0 {
			logging.Infof("discovery", "discovery: no non-loopback IPv4 interface found; announcement not sent")
		}
		sender.send(buf, routes)
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

// beaconSender owns the per-interface beacon sockets. It is used only by the
// Beacon goroutine and is not safe for concurrent use.
type beaconSender struct {
	fallback   net.PacketConn
	open       func(local net.IP) (net.PacketConn, error)
	port       int
	conns      map[string]net.PacketConn
	bindFailed map[string]bool
}

func newBeaconSender(fallback net.PacketConn, open func(net.IP) (net.PacketConn, error)) *beaconSender {
	return newBeaconSenderWithPort(fallback, open, Port)
}

func newBeaconSenderWithPort(fallback net.PacketConn, open func(net.IP) (net.PacketConn, error), port int) *beaconSender {
	return &beaconSender{
		fallback:   fallback,
		open:       open,
		port:       port,
		conns:      make(map[string]net.PacketConn),
		bindFailed: make(map[string]bool),
	}
}

// send writes buf to every route's broadcast address from that route's local
// address, falling back to the unbound socket when binding is impossible.
// Sockets for addresses that disappeared are closed.
func (s *beaconSender) send(buf []byte, routes []broadcastRoute) {
	current := make(map[string]bool, len(routes))
	for _, route := range routes {
		key := route.Local.String()
		current[key] = true
		conn := s.connFor(route.Local)
		target := &net.UDPAddr{IP: route.Broadcast, Port: s.port}
		if _, err := conn.WriteTo(buf, target); err != nil {
			logging.Errorf("discovery", "discovery: broadcast from %s to %s failed: %v", key, route.Broadcast, err)
			if conn != s.fallback {
				_ = conn.Close()
				delete(s.conns, key)
			}
		}
	}
	for key, conn := range s.conns {
		if !current[key] {
			_ = conn.Close()
			delete(s.conns, key)
		}
	}
	for key := range s.bindFailed {
		if !current[key] {
			delete(s.bindFailed, key)
		}
	}
}

func (s *beaconSender) connFor(local net.IP) net.PacketConn {
	key := local.String()
	if conn, ok := s.conns[key]; ok {
		return conn
	}
	if s.bindFailed[key] {
		return s.fallback
	}
	conn, err := s.open(local)
	if err != nil {
		logging.Errorf("discovery", "discovery: could not bind beacon to %s, using the default route: %v", key, err)
		s.bindFailed[key] = true
		return s.fallback
	}
	s.conns[key] = conn
	return conn
}

func (s *beaconSender) close() {
	for key, conn := range s.conns {
		_ = conn.Close()
		delete(s.conns, key)
	}
	_ = s.fallback.Close()
}

// broadcastRoutes returns every local IPv4 LAN address (loopback and
// link-local excluded) with its subnet broadcast address, so announcements
// only go out on the local network, never to 127.0.0.1.
func broadcastRoutes() []broadcastRoute {
	return broadcastRoutesFrom(net.InterfaceAddrs)
}

func broadcastRoutesFrom(getAddrs func() ([]net.Addr, error)) []broadcastRoute {
	var out []broadcastRoute
	addrs, err := getAddrs()
	if err != nil {
		return out
	}
	seen := make(map[string]bool, len(addrs))
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
		key := ip4.String()
		if seen[key] {
			continue
		}
		seen[key] = true
		bcast := make(net.IP, 4)
		for i := range ip4 {
			bcast[i] = ip4[i] | ^mask[i]
		}
		out = append(out, broadcastRoute{Local: append(net.IP(nil), ip4...), Broadcast: bcast})
	}
	return out
}
