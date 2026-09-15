//go:build windows

package discovery

import "syscall"

func configureSocket(raw syscall.RawConn, broadcast bool) error {
	// Windows permits UDP broadcast with the standard socket defaults.
	return nil
}