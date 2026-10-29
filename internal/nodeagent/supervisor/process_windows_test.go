//go:build windows

package supervisor

import (
	"os/exec"
	"testing"

	"golang.org/x/sys/windows"
)

func TestProbeCommandDoesNotCreateConsoleWindow(t *testing.T) {
	command := exec.Command("probe.exe")
	configureProbeCommand(command)
	attributes := command.SysProcAttr
	if attributes == nil || attributes.CreationFlags&windows.CREATE_NO_WINDOW == 0 || !attributes.HideWindow {
		t.Fatalf("probe command attributes = %#v, want CREATE_NO_WINDOW and HideWindow", attributes)
	}
}
