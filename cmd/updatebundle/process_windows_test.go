//go:build windows

package main

import (
	"os/exec"
	"testing"

	"golang.org/x/sys/windows"
)

func TestVersionProbeDoesNotCreateConsoleWindow(t *testing.T) {
	command := exec.Command("maistr0.exe", "-version")
	hideProbeWindow(command)
	attributes := command.SysProcAttr
	if attributes == nil || attributes.CreationFlags&windows.CREATE_NO_WINDOW == 0 || !attributes.HideWindow {
		t.Fatalf("version probe attributes = %#v, want CREATE_NO_WINDOW and HideWindow", attributes)
	}
}
