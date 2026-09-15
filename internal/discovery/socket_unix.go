//go:build !windows

package discovery

import (
	"syscall"
)

func configureSocket(raw syscall.RawConn, broadcast bool) error {
	var controlErr error
	err := raw.Control(func(fd uintptr) {
		if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
			controlErr = err
			return
		}
		if broadcast {
			controlErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_BROADCAST, 1)
		}
	})
	if err != nil {
		return err
	}
	return controlErr
}