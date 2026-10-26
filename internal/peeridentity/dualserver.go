package peeridentity

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

// ServeDualContext closes both listeners and cancels requests when ctx ends.
func ServeDualContext(ctx context.Context, addr string, localHandler, secureHandler http.Handler, tlsConfig *tls.Config) error {
	raw, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	return ServeDualListenerContext(ctx, raw, localHandler, secureHandler, tlsConfig)
}

// ServeDualListener serves plaintext loopback and mutual-TLS requests on an
// already-bound listener.
func ServeDualListener(raw net.Listener, localHandler, secureHandler http.Handler, tlsConfig *tls.Config) error {
	return ServeDualListenerContext(context.Background(), raw, localHandler, secureHandler, tlsConfig)
}

func ServeDualListenerContext(ctx context.Context, raw net.Listener, localHandler, secureHandler http.Handler, tlsConfig *tls.Config) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	split := newSplitListener(raw)
	splitDone := make(chan struct{})
	go func() {
		defer close(splitDone)
		split.run()
	}()

	plainServer := newHTTPServer(localHandler, nil)
	secureServer := newHTTPServer(secureHandler, tlsConfig)
	plainServer.BaseContext = func(net.Listener) context.Context { return ctx }
	secureServer.BaseContext = plainServer.BaseContext
	results := make(chan error, 2)
	go func() { results <- plainServer.Serve(split.listener(false)) }()
	go func() { results <- secureServer.Serve(tls.NewListener(split.listener(true), tlsConfig)) }()

	var err error
	remaining := 2
	select {
	case <-ctx.Done():
	case err = <-results:
		remaining--
	}
	cancel()
	_ = split.Close()
	err = ignoreServerShutdown(err)
	shutdownCtx, stopShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopShutdown()
	for _, server := range []*http.Server{plainServer, secureServer} {
		if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil && err == nil {
			err = fmt.Errorf("shut down active HTTP requests: %w", shutdownErr)
		}
	}
	_ = plainServer.Close()
	_ = secureServer.Close()
	// Closed servers report shutdown errors; keep the first real failure.
	for range remaining {
		if serveErr := ignoreServerShutdown(<-results); err == nil {
			err = serveErr
		}
	}
	<-splitDone
	split.routes.Wait()
	return err
}

func ignoreServerShutdown(err error) error {
	if errors.Is(err, http.ErrServerClosed) || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func newHTTPServer(handler http.Handler, tlsConfig *tls.Config) *http.Server {
	return &http.Server{
		Handler:           handler,
		TLSConfig:         tlsConfig,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       90 * time.Second,
	}
}

type splitListener struct {
	raw    net.Listener
	plain  chan net.Conn
	secure chan net.Conn
	done   chan struct{}
	// once closes done exactly once; it is independent of listener channels.
	once sync.Once
	// pending owns sockets until protocol classification hands them to HTTP.
	routeMu sync.Mutex
	pending map[net.Conn]struct{}
	routes  sync.WaitGroup
}

func newSplitListener(raw net.Listener) *splitListener {
	return &splitListener{
		raw: raw, plain: make(chan net.Conn), secure: make(chan net.Conn),
		done:    make(chan struct{}),
		pending: make(map[net.Conn]struct{}),
	}
}

func (s *splitListener) run() {
	for {
		conn, err := s.raw.Accept()
		if err != nil {
			_ = s.Close()
			return
		}
		if !s.startRoute(conn) {
			return
		}
	}
}

func (s *splitListener) startRoute(conn net.Conn) bool {
	s.routeMu.Lock()
	select {
	case <-s.done:
		s.routeMu.Unlock()
		_ = conn.Close()
		return false
	default:
	}
	s.pending[conn] = struct{}{}
	s.routes.Add(1)
	s.routeMu.Unlock()
	go s.route(conn)
	return true
}

func (s *splitListener) route(conn net.Conn) {
	defer func() {
		s.routeMu.Lock()
		delete(s.pending, conn)
		s.routeMu.Unlock()
		s.routes.Done()
	}()
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
		s.routeMu.Lock()
		pending := make([]net.Conn, 0, len(s.pending))
		for conn := range s.pending {
			pending = append(pending, conn)
		}
		s.routeMu.Unlock()
		for _, conn := range pending {
			_ = conn.Close()
		}
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
