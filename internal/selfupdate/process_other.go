//go:build !windows && !linux && !darwin

package selfupdate

import (
	"errors"
	"os/exec"
	"time"
)

func detach(*exec.Cmd)      {}
func hideConsole(*exec.Cmd) {}
func waitForParent(int, time.Duration) error {
	return errors.New("peer updates support Windows and Linux only")
}
