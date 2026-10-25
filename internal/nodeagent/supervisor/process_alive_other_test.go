//go:build !windows && !linux

package supervisor

import "syscall"

func processAlive(pid int) bool {
	return syscall.Kill(pid, 0) == nil
}
