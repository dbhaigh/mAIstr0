package nodeagent

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/maistr0/maistr0/internal/closeutil"
	"github.com/maistr0/maistr0/internal/nodeagent/supervisor"
	"github.com/maistr0/maistr0/internal/peeridentity"
)

type rpcRelayManager struct {
	identity     *peeridentity.Identity
	nodeID       string
	listenAddr   string
	advertiseURL string

	mu      sync.Mutex
	workers map[string]*rpcRelayInstance
	heads   map[string]*rpcHeadRelay
	closed  bool
}

type rpcHeadRelay struct {
	instance  *rpcRelayInstance
	targets   []supervisor.RPCRelayTarget
	addresses []string
}

type rpcRelayInstance struct {
	ctx       context.Context
	cancel    context.CancelFunc
	listeners []net.Listener
	conns     sync.Map
	wg        sync.WaitGroup
	closeOnce sync.Once
}

func newRPCRelayManager(identity *peeridentity.Identity, nodeID, listenAddr, advertiseURL string) (*rpcRelayManager, error) {
	if identity == nil || strings.TrimSpace(nodeID) == "" {
		return nil, errors.New("identity and node ID are required")
	}
	if listenAddr == "" {
		listenAddr = ":7452"
	}
	_, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid RPC relay listen address: %w", err)
	}
	if number, err := strconv.Atoi(port); err != nil || number < 0 || number > 65535 {
		return nil, errors.New("RPC relay listen address must use a port between 0 and 65535")
	}
	return &rpcRelayManager{
		identity: identity, nodeID: nodeID, listenAddr: listenAddr, advertiseURL: advertiseURL,
		workers: make(map[string]*rpcRelayInstance), heads: make(map[string]*rpcHeadRelay),
	}, nil
}

func (m *rpcRelayManager) StartWorker(ctx context.Context, id, backend string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	host, port, err := net.SplitHostPort(backend)
	if err != nil || !relayHostIsLoopback(host) || !validRelayPort(port) {
		return "", errors.New("RPC relay backend must be a valid loopback TCP address")
	}
	m.mu.Lock()
	existing := m.workers[id]
	closed := m.closed
	m.mu.Unlock()
	if closed {
		return "", errors.New("RPC relay manager is closed")
	}
	if existing != nil {
		return m.workerEndpoint(existing), nil
	}
	raw, err := net.Listen("tcp", m.listenAddr)
	if err != nil {
		return "", fmt.Errorf("listen for authenticated RPC relay: %w", err)
	}
	instance := newRPCRelayInstance()
	listener := tls.NewListener(raw, m.identity.PinnedPeerServerTLSConfig())
	instance.listeners = append(instance.listeners, listener)
	endpoint := m.endpointFor(raw.Addr())
	if endpoint == "" {
		_ = instance.close()
		return "", errors.New("could not determine advertised RPC relay endpoint")
	}
	instance.start(listener, func(context.Context) (net.Conn, error) {
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(instance.ctx, "tcp", backend)
	}, true, m.identity)
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		_ = instance.close()
		return "", errors.New("RPC relay manager is closed")
	}
	if existing := m.workers[id]; existing != nil {
		m.mu.Unlock()
		_ = instance.close()
		return m.workerEndpoint(existing), nil
	}
	m.workers[id] = instance
	m.mu.Unlock()
	return endpoint, nil
}

func (m *rpcRelayManager) StartHead(ctx context.Context, id string, targets []supervisor.RPCRelayTarget) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, errors.New("head RPC relay requires at least one worker target")
	}
	m.mu.Lock()
	existing := m.heads[id]
	closed := m.closed
	m.mu.Unlock()
	if closed {
		return nil, errors.New("RPC relay manager is closed")
	}
	if existing != nil {
		if equalRelayTargets(existing.targets, targets) {
			return append([]string(nil), existing.addresses...), nil
		}
		return nil, errors.New("head RPC relay is already configured with different targets")
	}

	instance := newRPCRelayInstance()
	addresses := make([]string, len(targets))
	listeners := make([]net.Listener, len(targets))
	seen := make(map[string]struct{}, len(targets))
	for index, target := range targets {
		if target.PeerID == "" {
			_ = instance.close()
			return nil, errors.New("RPC relay target peer ID is required")
		}
		if _, exists := seen[target.PeerID]; exists {
			_ = instance.close()
			return nil, fmt.Errorf("RPC relay target peer %q is duplicated", target.PeerID)
		}
		seen[target.PeerID] = struct{}{}
		host, port, err := net.SplitHostPort(target.Address)
		if err != nil || !validRelayPort(port) {
			_ = instance.close()
			return nil, fmt.Errorf("RPC relay target %q has an invalid address", target.PeerID)
		}
		if target.PeerID == m.nodeID {
			if !relayHostIsLoopback(host) {
				_ = instance.close()
				return nil, errors.New("local RPC backend must use a loopback address")
			}
			addresses[index] = target.Address
			continue
		}
		if !relayHostIsDialable(host) {
			_ = instance.close()
			return nil, fmt.Errorf("remote RPC relay target %q must have a routable address", target.PeerID)
		}
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			_ = instance.close()
			return nil, fmt.Errorf("listen for local RPC relay endpoint: %w", err)
		}
		listeners[index] = listener
		instance.listeners = append(instance.listeners, listener)
		addresses[index] = listener.Addr().String()
	}
	head := &rpcHeadRelay{
		instance:  instance,
		targets:   append([]supervisor.RPCRelayTarget(nil), targets...),
		addresses: append([]string(nil), addresses...),
	}
	for index, listener := range listeners {
		if listener == nil {
			continue
		}
		target := targets[index]
		instance.start(listener, func(ctx context.Context) (net.Conn, error) {
			return m.identity.DialPinned(ctx, "tcp", target.Address, target.PeerID)
		}, false, nil)
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		_ = instance.close()
		return nil, errors.New("RPC relay manager is closed")
	}
	if existing := m.heads[id]; existing != nil {
		m.mu.Unlock()
		_ = instance.close()
		if equalRelayTargets(existing.targets, targets) {
			return append([]string(nil), existing.addresses...), nil
		}
		return nil, errors.New("head RPC relay is already configured with different targets")
	}
	m.heads[id] = head
	m.mu.Unlock()

	return addresses, nil
}

func (m *rpcRelayManager) Stop(_ context.Context, id string) error {
	m.mu.Lock()
	worker := m.workers[id]
	head := m.heads[id]
	delete(m.workers, id)
	delete(m.heads, id)
	m.mu.Unlock()
	var result error
	if worker != nil {
		result = errors.Join(result, worker.close())
	}
	if head != nil {
		result = errors.Join(result, head.instance.close())
	}
	return result
}

func (m *rpcRelayManager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	workers := make([]*rpcRelayInstance, 0, len(m.workers))
	for _, instance := range m.workers {
		workers = append(workers, instance)
	}
	heads := make([]*rpcHeadRelay, 0, len(m.heads))
	for _, head := range m.heads {
		heads = append(heads, head)
	}
	clear(m.workers)
	clear(m.heads)
	m.mu.Unlock()
	var result error
	for _, instance := range workers {
		result = errors.Join(result, instance.close())
	}
	for _, head := range heads {
		result = errors.Join(result, head.instance.close())
	}
	return result
}

func (m *rpcRelayManager) workerEndpoint(instance *rpcRelayInstance) string {
	if len(instance.listeners) == 0 {
		return ""
	}
	return m.endpointFor(instance.listeners[0].Addr())
}

func (m *rpcRelayManager) endpointFor(address net.Addr) string {
	tcpAddress, ok := address.(*net.TCPAddr)
	if !ok || tcpAddress.Port < 1 || tcpAddress.Port > 65535 {
		return ""
	}
	bindHost, _, err := net.SplitHostPort(m.listenAddr)
	if err != nil {
		return ""
	}
	host := strings.Trim(bindHost, "[]")
	if host == "" || host == "0.0.0.0" || host == "::" {
		advertised, err := url.Parse(m.advertiseURL)
		if err != nil || advertised.Hostname() == "" {
			return ""
		}
		host = advertised.Hostname()
	}
	return net.JoinHostPort(host, strconv.Itoa(tcpAddress.Port))
}

func newRPCRelayInstance() *rpcRelayInstance {
	ctx, cancel := context.WithCancel(context.Background())
	return &rpcRelayInstance{ctx: ctx, cancel: cancel}
}

func (instance *rpcRelayInstance) start(
	listener net.Listener,
	dial func(context.Context) (net.Conn, error),
	tlsServer bool,
	serverIdentity *peeridentity.Identity,
) {
	instance.wg.Add(1)
	go instance.serve(listener, dial, tlsServer, serverIdentity)
}

func (instance *rpcRelayInstance) serve(
	listener net.Listener,
	dial func(context.Context) (net.Conn, error),
	tlsServer bool,
	serverIdentity *peeridentity.Identity,
) {
	defer instance.wg.Done()
	for {
		connection, err := listener.Accept()
		if err != nil {
			if instance.ctx.Err() != nil {
				return
			}
			continue
		}
		instance.wg.Add(1)
		go func(connection net.Conn) {
			defer instance.wg.Done()
			instance.relay(connection, dial, tlsServer, serverIdentity)
		}(connection)
	}
}

func (instance *rpcRelayInstance) relay(
	inbound net.Conn,
	dial func(context.Context) (net.Conn, error),
	tlsServer bool,
	serverIdentity *peeridentity.Identity,
) {
	instance.track(inbound, true)
	defer func() {
		instance.track(inbound, false)
		_ = inbound.Close()
	}()
	if tlsServer {
		connection, ok := inbound.(*tls.Conn)
		if !ok || serverIdentity == nil {
			return
		}
		_ = connection.SetDeadline(time.Now().Add(10 * time.Second))
		if err := connection.HandshakeContext(instance.ctx); err != nil {
			return
		}
		state := connection.ConnectionState()
		if len(state.PeerCertificates) == 0 {
			return
		}
		tracked, err := serverIdentity.TrackPinnedPeerConnection(state.PeerCertificates[0], inbound)
		if err != nil {
			return
		}
		instance.track(inbound, false)
		inbound = tracked
		instance.track(inbound, true)
		_ = connection.SetDeadline(time.Time{})
	}
	outbound, err := dial(instance.ctx)
	if err != nil {
		return
	}
	instance.track(outbound, true)
	defer instance.track(outbound, false)
	defer closeutil.Discard(outbound)

	copyDone := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(outbound, inbound)
		closeWrite(outbound)
		copyDone <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(inbound, outbound)
		closeWrite(inbound)
		copyDone <- struct{}{}
	}()
	select {
	case <-copyDone:
	case <-instance.ctx.Done():
	}
	_ = inbound.Close()
	_ = outbound.Close()
	<-copyDone
}

func (instance *rpcRelayInstance) track(connection net.Conn, add bool) {
	if add {
		instance.conns.Store(connection, struct{}{})
		return
	}
	instance.conns.Delete(connection)
}

func (instance *rpcRelayInstance) close() error {
	var result error
	instance.closeOnce.Do(func() {
		instance.cancel()
		for _, listener := range instance.listeners {
			result = errors.Join(result, listener.Close())
		}
		instance.conns.Range(func(key, _ any) bool {
			if connection, ok := key.(net.Conn); ok {
				result = errors.Join(result, connection.Close())
			}
			return true
		})
		instance.wg.Wait()
	})
	return result
}

func closeWrite(connection net.Conn) {
	if closeWriter, ok := connection.(interface{ CloseWrite() error }); ok {
		_ = closeWriter.CloseWrite()
	}
}

func relayHostIsLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

func relayHostIsDialable(host string) bool {
	if host == "" || strings.EqualFold(host, "localhost") {
		return strings.EqualFold(host, "localhost")
	}
	if ip := net.ParseIP(strings.Trim(host, "[]")); ip != nil {
		return !ip.IsUnspecified() && !ip.IsLinkLocalUnicast()
	}
	return !strings.ContainsAny(host, "\x00\r\n\t /")
}

func validRelayPort(port string) bool {
	number, err := strconv.Atoi(port)
	return err == nil && number > 0 && number <= 65535
}

func equalRelayTargets(left, right []supervisor.RPCRelayTarget) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

var _ supervisor.RPCRelayController = (*rpcRelayManager)(nil)
