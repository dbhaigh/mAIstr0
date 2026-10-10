package peeridentity

import (
	"bufio"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"
)

// ServeDual dispatches TLS connections to secureHandler and plaintext
// connections to localHandler on the same port.
func ServeDual(addr string, localHandler, secureHandler http.Handler, tlsConfig *tls.Config) error {
	raw, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return ServeDualListener(raw, localHandler, secureHandler, tlsConfig)
}

// ServeDualListener serves plaintext loopback and mutual-TLS requests on an
// already-bound listener.
func ServeDualListener(raw net.Listener, localHandler, secureHandler http.Handler, tlsConfig *tls.Config) error {
	split := newSplitListener(raw)
	go split.run()

	plainServer := &http.Server{Handler: localHandler}
	secureServer := &http.Server{Handler: secureHandler, TLSConfig: tlsConfig}
	results := make(chan error, 2)
	go func() { results <- plainServer.Serve(split.listener(false)) }()
	go func() { results <- secureServer.Serve(tls.NewListener(split.listener(true), tlsConfig)) }()

	err := <-results
	_ = split.Close()
	_ = plainServer.Close()
	_ = secureServer.Close()
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

type splitListener struct {
	raw    net.Listener
	plain  chan net.Conn
	secure chan net.Conn
	done   chan struct{}
	once   sync.Once
}

func newSplitListener(raw net.Listener) *splitListener {
	return &splitListener{
		raw: raw, plain: make(chan net.Conn), secure: make(chan net.Conn),
		done: make(chan struct{}),
	}
}

func (s *splitListener) run() {
	for {
		conn, err := s.raw.Accept()
		if err != nil {
			_ = s.Close()
			return
		}
		go s.route(conn)
	}
}

func (s *splitListener) route(conn net.Conn) {
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(conn)
	first, err := reader.Peek(1)
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		_ = conn.Close()
		return
	}
	target := s.plain
	if first[0] == 0x16 {
		target = s.secure
	}
	select {
	case target <- bufferedConn{Conn: conn, Reader: reader}:
	case <-s.done:
		_ = conn.Close()
	}
}

func (s *splitListener) listener(secure bool) net.Listener {
	target := s.plain
	if secure {
		target = s.secure
	}
	return &channelListener{connections: target, done: s.done, addr: s.raw.Addr()}
}

func (s *splitListener) Close() error {
	var err error
	s.once.Do(func() {
		close(s.done)
		err = s.raw.Close()
	})
	return err
}

type bufferedConn struct {
	net.Conn
	Reader *bufio.Reader
}

func (c bufferedConn) Read(p []byte) (int, error) { return c.Reader.Read(p) }

type channelListener struct {
	connections <-chan net.Conn
	done        <-chan struct{}
	addr        net.Addr
}

func (l *channelListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.connections:
		return conn, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *channelListener) Close() error   { return nil }
func (l *channelListener) Addr() net.Addr { return l.addr }
