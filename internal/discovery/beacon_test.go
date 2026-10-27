package discovery

import (
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

type sentPacket struct {
	payload string
	to      string
}

// fakePacketConn records writes and serves queued reads until it is closed.
type fakePacketConn struct {
	name     string
	mu       sync.Mutex
	readWake chan struct{}
	closedCh chan struct{}
	sent     *[]sentPacket
	reads    []fakeRead
	writeErr error
	closed   bool
}

type fakeRead struct {
	payload []byte
	err     error
}

func (c *fakePacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return 0, nil, net.ErrClosed
		}
		if len(c.reads) > 0 {
			next := c.reads[0]
			c.reads = c.reads[1:]
			c.mu.Unlock()
			if next.err != nil {
				return 0, nil, next.err
			}
			return copy(p, next.payload), &net.UDPAddr{}, nil
		}
		if c.readWake == nil {
			c.readWake = make(chan struct{})
		}
		wake := c.readWake
		c.mu.Unlock()
		<-wake
	}
}

func (c *fakePacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	*c.sent = append(*c.sent, sentPacket{payload: c.name, to: addr.String()})
	return len(p), nil
}

func (c *fakePacketConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		if c.readWake != nil {
			close(c.readWake)
			c.readWake = nil
		}
		if c.closedCh == nil {
			c.closedCh = make(chan struct{})
		}
		close(c.closedCh)
	}
	return nil
}

func (c *fakePacketConn) closedSignal() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closedCh == nil {
		c.closedCh = make(chan struct{})
		if c.closed {
			close(c.closedCh)
		}
	}
	return c.closedCh
}

func (c *fakePacketConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *fakePacketConn) LocalAddr() net.Addr              { return &net.UDPAddr{} }
func (c *fakePacketConn) SetDeadline(time.Time) error      { return nil }
func (c *fakePacketConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fakePacketConn) SetWriteDeadline(time.Time) error { return nil }

func ipNet(cidrIP string, prefix int) *net.IPNet {
	return &net.IPNet{IP: net.ParseIP(cidrIP), Mask: net.CIDRMask(prefix, 32)}
}

func TestBroadcastRoutes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		addrs []net.Addr
		want  []string
	}{
		{
			name: "multi-homed same subnet keeps each local address",
			addrs: []net.Addr{
				ipNet("192.168.1.200", 24),
				ipNet("192.168.1.189", 24),
				ipNet("172.21.128.1", 20),
			},
			want: []string{
				"192.168.1.200->192.168.1.255",
				"192.168.1.189->192.168.1.255",
				"172.21.128.1->172.21.143.255",
			},
		},
		{
			name: "skips loopback link-local IPv6 and duplicates",
			addrs: []net.Addr{
				ipNet("127.0.0.1", 8),
				ipNet("169.254.17.246", 16),
				&net.IPNet{IP: net.ParseIP("fe80::1"), Mask: net.CIDRMask(64, 128)},
				ipNet("10.0.0.5", 8),
				ipNet("10.0.0.5", 8),
				&net.IPAddr{IP: net.ParseIP("10.0.0.6")},
			},
			want: []string{"10.0.0.5->10.255.255.255"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got []string
			for _, r := range broadcastRoutesFrom(func() ([]net.Addr, error) { return tt.addrs, nil }) {
				got = append(got, r.Local.String()+"->"+r.Broadcast.String())
			}
			if len(got) != len(tt.want) {
				t.Fatalf("routes = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("routes = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestBeaconSenderBindsEachInterface(t *testing.T) {
	t.Parallel()
	port := ephemeralUDPPort(t)
	routeA := broadcastRoute{Local: net.ParseIP("192.168.1.200").To4(), Broadcast: net.ParseIP("192.168.1.255").To4()}
	routeB := broadcastRoute{Local: net.ParseIP("192.168.1.189").To4(), Broadcast: net.ParseIP("192.168.1.255").To4()}
	destination := func(ip string) string { return net.JoinHostPort(ip, strconv.Itoa(port)) }

	tests := []struct {
		name        string
		bindFail    map[string]bool
		routes      [][]broadcastRoute
		wantSent    []sentPacket
		wantOpens   int
		wantClosedA bool
	}{
		{
			name:   "each route uses a socket bound to its local address",
			routes: [][]broadcastRoute{{routeA, routeB}, {routeA, routeB}},
			wantSent: []sentPacket{
				{"192.168.1.200", destination("192.168.1.255")}, {"192.168.1.189", destination("192.168.1.255")},
				{"192.168.1.200", destination("192.168.1.255")}, {"192.168.1.189", destination("192.168.1.255")},
			},
			wantOpens: 2,
		},
		{
			name:     "bind failure falls back to the default route once",
			bindFail: map[string]bool{"192.168.1.189": true},
			routes:   [][]broadcastRoute{{routeA, routeB}, {routeA, routeB}},
			wantSent: []sentPacket{
				{"192.168.1.200", destination("192.168.1.255")}, {"fallback", destination("192.168.1.255")},
				{"192.168.1.200", destination("192.168.1.255")}, {"fallback", destination("192.168.1.255")},
			},
			wantOpens: 2,
		},
		{
			name:        "sockets for vanished interfaces are closed",
			routes:      [][]broadcastRoute{{routeA, routeB}, {routeB}},
			wantSent:    []sentPacket{{"192.168.1.200", destination("192.168.1.255")}, {"192.168.1.189", destination("192.168.1.255")}, {"192.168.1.189", destination("192.168.1.255")}},
			wantOpens:   2,
			wantClosedA: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sent []sentPacket
			opened := map[string]*fakePacketConn{}
			opens := 0
			fallback := &fakePacketConn{name: "fallback", sent: &sent}
			sender := newBeaconSenderWithPort(fallback, func(local net.IP) (net.PacketConn, error) {
				opens++
				if tt.bindFail[local.String()] {
					return nil, errors.New("bind refused")
				}
				conn := &fakePacketConn{name: local.String(), sent: &sent}
				opened[local.String()] = conn
				return conn, nil
			}, port)
			for _, routes := range tt.routes {
				sender.send([]byte("{}"), routes)
			}
			if opens != tt.wantOpens {
				t.Fatalf("opens = %d, want %d", opens, tt.wantOpens)
			}
			if len(sent) != len(tt.wantSent) {
				t.Fatalf("sent = %v, want %v", sent, tt.wantSent)
			}
			for i := range sent {
				if sent[i] != tt.wantSent[i] {
					t.Fatalf("sent = %v, want %v", sent, tt.wantSent)
				}
			}
			if a := opened["192.168.1.200"]; a != nil && a.isClosed() != tt.wantClosedA {
				t.Fatalf("socket for vanished interface closed = %v, want %v", a.isClosed(), tt.wantClosedA)
			}
			sender.close()
			for name, conn := range opened {
				if !conn.isClosed() {
					t.Fatalf("socket %s left open after close", name)
				}
			}
			if !fallback.isClosed() {
				t.Fatal("fallback socket left open after close")
			}
		})
	}
}

func TestBeaconSenderReopensAfterWriteError(t *testing.T) {
	t.Parallel()
	var sent []sentPacket
	route := broadcastRoute{Local: net.ParseIP("10.0.0.5").To4(), Broadcast: net.ParseIP("10.255.255.255").To4()}
	var conns []*fakePacketConn
	sender := newBeaconSender(&fakePacketConn{name: "fallback", sent: &sent}, func(local net.IP) (net.PacketConn, error) {
		conn := &fakePacketConn{name: local.String(), sent: &sent}
		if len(conns) == 0 {
			conn.writeErr = errors.New("network unreachable")
		}
		conns = append(conns, conn)
		return conn, nil
	})
	defer sender.close()

	sender.send([]byte("{}"), []broadcastRoute{route})
	sender.send([]byte("{}"), []broadcastRoute{route})
	if len(conns) != 2 || !conns[0].isClosed() {
		t.Fatalf("failed socket was not replaced: %d sockets, first closed=%v", len(conns), len(conns) > 0 && conns[0].isClosed())
	}
	if len(sent) != 1 || sent[0].payload != "10.0.0.5" {
		t.Fatalf("sent = %v, want one packet from the reopened socket", sent)
	}
}

func TestListenerRebindsAfterReadError(t *testing.T) {
	t.Parallel()
	ann, err := json.Marshal(Announcement{Role: "node", ID: "peer-after-rebind"})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		bindErrs int
	}{
		{name: "rebind succeeds immediately"},
		{name: "rebind retries after a bind failure", bindErrs: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mu sync.Mutex
			binds := 0
			first := &fakePacketConn{reads: []fakeRead{{err: errors.New("wsarecvfrom: connection reset")}}}
			second := &fakePacketConn{reads: []fakeRead{{payload: ann}}}
			listener := listenWithOptions(0, time.Millisecond, time.Millisecond, func() (net.PacketConn, error) {
				mu.Lock()
				defer mu.Unlock()
				binds++
				switch {
				case binds == 1:
					return first, nil
				case binds <= 1+tt.bindErrs:
					return nil, errors.New("address in use")
				default:
					return second, nil
				}
			})
			if err := listener.WaitReady(time.Second); err != nil {
				t.Fatal(err)
			}

			waitForPeer(t, listener, "peer-after-rebind")
			if !first.isClosed() {
				t.Fatal("failed socket was not closed")
			}
			listener.Close()
			select {
			case <-second.closedSignal():
			case <-time.After(time.Second):
				t.Fatal("rebound socket was not closed by Close")
			}
		})
	}
}

func ephemeralUDPPort(t *testing.T) int {
	t.Helper()
	conn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close ephemeral UDP listener: %v", err)
		}
	}()
	return conn.LocalAddr().(*net.UDPAddr).Port
}
