//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package execution

import (
	"errors"
	"os/exec"
	"syscall"
	"time"
)

type unixProcessTree struct {
	pid int
}

func processTreeSupported() bool { return true }

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

func (tree *unixProcessTree) Close() error {
	if err := tree.Kill(); err != nil {
		return err
	}
	deadline := time.Now().Add(processTreeTerminationWait)
	for {
		err := syscall.Kill(-tree.pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		if err != nil {
			return err
		}
		if time.Now().After(deadline) {
			return errors.New("process group did not terminate")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
