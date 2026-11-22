//go:build !windows && !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package execution

import "os/exec"

func processTreeSupported() bool { return false }

func startProcessTree(*exec.Cmd) (processTree, error) {
	return nil, errProcessTreeUnsupported
}
