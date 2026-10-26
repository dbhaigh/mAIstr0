package nodeagent

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type gatedRelayConn struct {
	net.Conn
	reading chan struct{}
	closed  chan struct{}
	finish  chan struct{}
	once    sync.Once
}

func (c *gatedRelayConn) Read([]byte) (int, error) {
	close(c.reading)
	<-c.finish
	return 0, io.EOF
}

func (c *gatedRelayConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func TestCanceledRelayJoinsBothCopyDirections(t *testing.T) {
	inbound := &gatedRelayConn{reading: make(chan struct{}), closed: make(chan struct{}), finish: make(chan struct{})}
	outbound := &gatedRelayConn{reading: make(chan struct{}), closed: make(chan struct{}), finish: make(chan struct{})}
	instance := newRPCRelayInstance()
	done := make(chan struct{})
	go func() {
		defer close(done)
		instance.relay(inbound, func(context.Context) (net.Conn, error) { return outbound, nil }, false, nil)
	}()
	<-inbound.reading
	<-outbound.reading
	instance.cancel()
	<-inbound.closed
	<-outbound.closed
	close(inbound.finish)
	select {
	case <-done:
		t.Error("relay returned before both copy directions exited")
	case <-time.After(30 * time.Millisecond):
	}
	close(outbound.finish)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("relay did not finish after both copies exited")
	}
	instance.conns.Range(func(_, _ any) bool {
		t.Error("closed relay retained a tracked connection")
		return true
	})
}
