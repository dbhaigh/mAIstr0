//go:build windows

package nodecontrol

import (
	"os/exec"
	"testing"

	"golang.org/x/sys/windows"
)

func TestConfigureChildProcessDoesNotCreateConsoleWindow(t *testing.T) {
	command := exec.Command("maistr0")
	configureChildProcess(command)
	attributes := command.SysProcAttr
	if attributes == nil || attributes.CreationFlags&windows.CREATE_NO_WINDOW == 0 {
		t.Fatalf("child process attributes = %#v, want CREATE_NO_WINDOW", command.SysProcAttr)
	}
}
