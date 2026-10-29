//go:build windows

package hardware

import (
	"context"
	"testing"

	"golang.org/x/sys/windows"
)

func TestHardwareCommandDoesNotCreateConsoleWindow(t *testing.T) {
	command := newHardwareCommand(context.Background(), "probe.exe")
	attributes := command.SysProcAttr
	if attributes == nil || attributes.CreationFlags&windows.CREATE_NO_WINDOW == 0 || !attributes.HideWindow {
		t.Fatalf("hardware command attributes = %#v, want CREATE_NO_WINDOW and HideWindow", attributes)
	}
}
