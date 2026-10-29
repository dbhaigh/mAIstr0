//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package supervisor

import (
	"errors"
	"os/exec"
	"syscall"
)

type unixProcessTree struct {
	pid int
}

func nativeWindows() bool { return false }

func configureProbeCommand(*exec.Cmd) {}

func startProcessTree(command *exec.Cmd) (processTree, error) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		return nil, err
	}
	return &unixProcessTree{pid: command.Process.Pid}, nil
}

func (tree *unixProcessTree) Kill() error {
	err := syscall.Kill(-tree.pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}

func (tree *unixProcessTree) Close() error { return nil }
