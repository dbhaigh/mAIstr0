package discovery

import (
	"net"
	"sync"
	"testing"
	"time"
)

type joinedPacketConn struct {
	fakePacketConn
	reading chan struct{}
	closed  chan struct{}
	finish  chan struct{}
	once    sync.Once
}

func (c *joinedPacketConn) ReadFrom([]byte) (int, net.Addr, error) {
	close(c.reading)
	<-c.closed
	<-c.finish
	return 0, nil, net.ErrClosed
}

func (c *joinedPacketConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func TestConcurrentListenerCloseJoinsReader(t *testing.T) {
	conn := &joinedPacketConn{
		reading: make(chan struct{}), closed: make(chan struct{}), finish: make(chan struct{}),
	}
	listener := listenWith(func() (net.PacketConn, error) { return conn, nil })
	if err := listener.WaitReady(time.Second); err != nil {
		t.Fatal(err)
	}
	<-conn.reading
	done := make(chan struct{}, 16)
	for range 16 {
		go func() { listener.Close(); done <- struct{}{} }()
	}
	<-conn.closed
	select {
	case <-done:
		t.Error("Close returned before its reader exited")
	case <-time.After(30 * time.Millisecond):
	}
	close(conn.finish)
	for range 16 {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("concurrent Close did not join the reader")
		}
	}
}
