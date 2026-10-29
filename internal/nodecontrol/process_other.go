//go:build !windows

package nodecontrol

import "os/exec"

func configureChildProcess(*exec.Cmd) {}
