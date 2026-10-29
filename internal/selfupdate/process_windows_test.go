//go:build windows

package selfupdate

import (
	"os/exec"
	"testing"

	"golang.org/x/sys/windows"
)

func TestVersionProbeDoesNotCreateConsoleWindow(t *testing.T) {
	command := exec.Command("maistr0", "-version")
	hideConsole(command)
	attributes := command.SysProcAttr
	if attributes == nil || attributes.CreationFlags&windows.CREATE_NO_WINDOW == 0 || !attributes.HideWindow {
		t.Fatalf("probe process attributes = %#v, want CREATE_NO_WINDOW and HideWindow", command.SysProcAttr)
	}
}
