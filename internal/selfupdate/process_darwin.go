//go:build darwin

package selfupdate

import (
	"errors"
	"fmt"
	"os/exec"
	"syscall"
	"time"
)

func detach(command *exec.Cmd) { command.SysProcAttr = &syscall.SysProcAttr{Setsid: true} }
func hideConsole(*exec.Cmd)    {}

func waitForParent(pid int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		if err != nil && !errors.Is(err, syscall.EPERM) {
			return fmt.Errorf("inspect update parent: %w", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("update parent did not exit before replacement deadline")
}
